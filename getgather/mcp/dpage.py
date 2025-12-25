import asyncio
import ipaddress
import os
import urllib.parse
from typing import Any

import zendriver as zd
from bs4 import BeautifulSoup, Tag
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastmcp.server.dependencies import get_http_headers
from nanoid import generate
from patchright.async_api import Page

from getgather.browser.profile import BrowserProfile
from getgather.browser.session import BrowserSession
from getgather.config import settings
from getgather.distill import (
    Match,
    autoclick,
    capture_page_artifacts,
    convert,
    distill,
    get_incognito_browser_profile,
    get_selector,
    load_distillation_patterns,
    report_distill_error,
    run_distillation_loop,
    terminate,
)
from getgather.logs import logger
from getgather.mcp.html_renderer import DEFAULT_TITLE, render_form
from getgather.zen_distill import (
    autoclick as zen_autoclick,
    capture_page_artifacts as zen_capture_page_artifacts,
    distill as zen_distill,
    get_new_page,
    init_zendriver_browser,
    page_query_selector,
    run_distillation_loop as zen_run_distillation_loop,
    terminate_zendriver_browser,
    zen_navigate_with_retry,
    zen_report_distill_error,
)

router = APIRouter(prefix="/dpage", tags=["dpage"])


active_pages: dict[str, Page | zd.Tab] = {}
distillation_results: dict[str, str | list[dict[str, str | list[str]]] | dict[str, Any]] = {}
pending_actions: dict[str, dict[str, Any]] = {}  # Store actions to resume after signin

# Patchright
incognito_browser_profiles: dict[str, BrowserProfile] = {}
global_browser_profile: BrowserProfile | None = None

# Zendriver
incognito_browsers: dict[str, zd.Browser] = {}
zen_global_browser: zd.Browser | None = None

FRIENDLY_CHARS: str = "23456789abcdefghijkmnpqrstuvwxyz"


async def dpage_add(page: Page | zd.Tab, location: str, profile_id: str | None = None):
    if isinstance(page, zd.Tab):
        return await zen_dpage_add(page, location, profile_id)

    id = generate(FRIENDLY_CHARS, 8)
    if settings.HOSTNAME:
        id = f"{settings.HOSTNAME}-{id}"

    try:
        if not location.startswith("http"):
            location = f"https://{location}"
        await page.goto(location, timeout=settings.BROWSER_TIMEOUT, wait_until="domcontentloaded")
    except Exception as error:
        hostname = urllib.parse.urlparse(location).hostname or "unknown"
        await report_distill_error(
            error=error,
            page=page,
            profile_id=profile_id or "unknown",
            location=location,
            hostname=hostname,
            iteration=0,
        )
    active_pages[id] = page
    return id


async def zen_dpage_add(page: zd.Tab, location: str, profile_id: str | None = None):
    id = generate(FRIENDLY_CHARS, 8)
    if settings.HOSTNAME:
        id = f"{settings.HOSTNAME}-{id}"

    try:
        if not location.startswith("http"):
            location = f"https://{location}"
        await zen_navigate_with_retry(page, location)
    except Exception as error:
        hostname = urllib.parse.urlparse(location).hostname or "unknown"
        await zen_report_distill_error(
            error=error,
            page=page,
            profile_id=profile_id or "unknown",
            location=location,
            hostname=hostname,
            iteration=0,
        )
    active_pages[id] = page
    return id


async def dpage_close(id: str) -> None:
    if id in active_pages:
        await active_pages[id].close()
        del active_pages[id]


async def dpage_check(id: str):
    TICK = 1  # seconds
    TIMEOUT = 120  # seconds
    max = TIMEOUT // TICK

    for iteration in range(max):
        logger.debug(f"Checking dpage {id}: {iteration + 1} of {max}")
        await asyncio.sleep(TICK)

        # Check if signin completed
        if id in distillation_results:
            return distillation_results[id]

    return None


async def dpage_finalize(id: str):
    if id in incognito_browser_profiles:
        await BrowserSession.get(incognito_browser_profiles[id]).stop()
        del incognito_browser_profiles[id]
        return True
    elif id in incognito_browsers:
        browser = incognito_browsers[id]
        await terminate_zendriver_browser(browser)
        return True
    raise ValueError(f"Browser profile for signin {id} not found in incognito browser profiles")


async def zen_dpage_finalize(id: str):
    if id in incognito_browsers:
        browser = incognito_browsers[id]
        await terminate_zendriver_browser(browser)
        return True
    raise ValueError(f"Browser profile for signin {id} not found in incognito browser profiles")


def render(content: str, options: dict[str, str] | None = None) -> str:
    """Render HTML template with content and options."""
    if options is None:
        options = {}

    title = options.get("title", DEFAULT_TITLE)
    action = options.get("action", "")

    return render_form(content, title, action)


# Since the browser can't redirect from GET to POST,
# we'll use an auto-submit form to do that.
def redirect(id: str) -> HTMLResponse:
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html>
    <body>
      <form id="redirect" action="/dpage/{id}" method="post">
      </form>
      <script>document.getElementById('redirect').submit();</script>
    </body>
    </html>
    """)


@router.get("", response_class=HTMLResponse)
@router.get("/{id}", response_class=HTMLResponse)
async def get_dpage(id: str | None = None) -> HTMLResponse:
    if id:
        if id in active_pages:
            return redirect(id)
        raise HTTPException(status_code=404, detail="Invalid page id")

    raise HTTPException(status_code=400, detail="Missing page id")


FINISHED_MSG = "Finished! You can close this window now."


@router.post("/{id}", response_class=HTMLResponse)
async def post_dpage(id: str, request: Request) -> HTMLResponse:
    if id not in active_pages:
        raise HTTPException(status_code=404, detail="Page not found")

    page = active_pages[id]
    if isinstance(page, zd.Tab):
        return await zen_post_dpage(page, id, request)

    form_data = await request.form()
    fields: dict[str, str] = {k: str(v) for k, v in form_data.items()}

    path = os.path.join(os.path.dirname(__file__), "patterns", "**/*.html")
    patterns = load_distillation_patterns(path)

    logger.info(f"Continuing distillation for page {id}...")
    logger.debug(f"Available distillation patterns: {len(patterns)}")

    TICK = 1  # seconds
    TIMEOUT = 15  # seconds
    max = TIMEOUT // TICK

    current = Match(name="", priority=-1, distilled="")

    if settings.LOG_LEVEL == "DEBUG":
        await capture_page_artifacts(page, identifier=id, prefix="dpage_debug")

    for iteration in range(max):
        logger.debug(f"Iteration {iteration + 1} of {max}")
        await asyncio.sleep(TICK)

        location = page.url
        hostname = urllib.parse.urlparse(location).hostname

        match = await distill(hostname, page, patterns, profile_id=id)
        if not match:
            logger.info("No matched pattern found")
            continue

        distilled = match.distilled
        document = BeautifulSoup(distilled, "html.parser")
        title_element = BeautifulSoup(distilled, "html.parser").find("title")
        title = title_element.get_text() if title_element is not None else DEFAULT_TITLE
        action = f"/dpage/{id}"
        options = {"title": title, "action": action}
        inputs = document.find_all("input")

        if match.distilled == current.distilled:
            logger.info(f"Still the same: {match.name}")
            has_inputs = len(inputs) > 0
            max_reached = iteration == max - 1
            if max_reached and has_inputs:
                logger.info("Still the same after timeout and need inputs, render the page...")
                return HTMLResponse(render(str(document.find("body")), options))
            continue

        current = match

        if await terminate(distilled):
            logger.info("Finished!")
            converted = await convert(distilled)

            if id in pending_actions:
                action_info = pending_actions[id]
                logger.info(f"Signin completed for {id}, resuming action...")

                action_result = await dpage_with_action(
                    initial_url=action_info["initial_url"],
                    action=action_info["action"],
                    timeout=action_info["timeout"],
                    _signin_completed=True,
                    _page_id=id,
                )

                distillation_results[id] = action_result

                del pending_actions[id]
                await dpage_close(id)
                return HTMLResponse(render(FINISHED_MSG, options))

            await dpage_close(id)
            if converted is not None:
                print(converted)
                distillation_results[id] = converted
            else:
                logger.info("No conversion found")
                distillation_results[id] = distilled
            return HTMLResponse(render(FINISHED_MSG, options))

        names: list[str] = []

        if fields.get("button"):
            button = document.find("button", value=str(fields.get("button")))
            if button:
                logger.info(f"Clicking button button[value={fields.get('button')}]")
                await autoclick(page, distilled, f"button[value={fields.get('button')}]")
                continue

        for input in inputs:
            if isinstance(input, Tag):
                gg_match = input.get("gg-match")
                selector, frame_selector = get_selector(
                    str(gg_match) if gg_match is not None else ""
                )
                name = input.get("name")
                input_type = input.get("type")

                if selector:
                    if input_type == "checkbox":
                        if not name:
                            logger.warning(f"No name for the checkbox {selector}")
                            continue
                        value = fields.get(str(name))
                        checked = value and len(str(value)) > 0
                        names.append(str(name))
                        logger.info(f"Status of checkbox {name}={checked}")
                        if checked:
                            if frame_selector:
                                await (
                                    page.frame_locator(str(frame_selector))
                                    .locator(str(selector))
                                    .check()
                                )
                            else:
                                await page.check(str(selector))
                    elif input_type == "radio":
                        if name is not None:
                            name_str = str(name)
                            value = fields.get(name_str)
                            if not value or len(value) == 0:
                                logger.warning(f"No form data found for radio button group {name}")
                                continue
                            radio = document.find("input", {"type": "radio", "id": str(value)})
                            if not radio or not isinstance(radio, Tag):
                                logger.warning(f"No radio button found with id {value}")
                                continue
                            logger.info(f"Handling radio button group {name}")
                            logger.info(f"Using form data {name}={value}")
                            radio_selector, radio_frame_selector = get_selector(
                                str(radio.get("gg-match"))
                            )
                            if radio_frame_selector:
                                await (
                                    page.frame_locator(str(radio_frame_selector))
                                    .locator(str(radio_selector))
                                    .check()
                                )
                            else:
                                await page.check(str(radio_selector))
                            radio["checked"] = "checked"
                            current.distilled = str(document)
                            names.append(str(input.get("id")) if input.get("id") else "radio")
                            await asyncio.sleep(0.25)
                    elif name is not None:
                        name_str = str(name)
                        value = fields.get(name_str)
                        if value and len(value) > 0:
                            logger.info(f"Using form data {name}")
                            names.append(name_str)
                            input["value"] = value
                            current.distilled = str(document)
                            if frame_selector:
                                await (
                                    page.frame_locator(str(frame_selector))
                                    .locator(str(selector))
                                    .fill(value)
                                )
                            else:
                                await page.fill(str(selector), value)
                            del fields[name_str]
                            await asyncio.sleep(0.25)
                        else:
                            logger.info(f"No form data found for {name}")

        await autoclick(page, distilled, "[gg-autoclick]:not(button)")
        SUBMIT_BUTTON = "button[gg-autoclick], button[type=submit]"
        if document.select(SUBMIT_BUTTON):
            if len(names) > 0 and len(inputs) == len(names):
                logger.info("Submitting form, all fields are filled...")
                await autoclick(page, distilled, SUBMIT_BUTTON)
                continue
            logger.warning("Not all form fields are filled")
            return HTMLResponse(render(str(document.find("body")), options))

    location = page.url
    hostname = urllib.parse.urlparse(location).hostname or "unknown"
    timeout_error = TimeoutError("Timeout reached in post_dpage")
    await report_distill_error(
        error=timeout_error,
        page=page,
        profile_id=id,
        location=location,
        hostname=hostname,
        iteration=max,
    )
    raise HTTPException(status_code=503, detail="Timeout reached")


def is_local_address(host: str) -> bool:
    hostname = host.split(":")[0].lower().strip()
    try:
        ip = ipaddress.ip_address(hostname)
        return ip.is_loopback
    except ValueError:
        return hostname in ("localhost", "127.0.0.1")


async def dpage_mcp_tool(initial_url: str, result_key: str, timeout: int = 2) -> dict[str, Any]:
    """Generic MCP tool based on distillation"""
    path = os.path.join(os.path.dirname(__file__), "patterns", "**/*.html")
    patterns = load_distillation_patterns(path)

    headers = get_http_headers(include_all=True)
    incognito = headers.get("x-incognito", "0") == "1"
    signin_id = headers.get("x-signin-id") or None

    if incognito:
        browser_profile = await get_incognito_browser_profile(signin_id)
    else:
        global global_browser_profile
        if global_browser_profile is None:
            logger.info(f"Creating global browser profile...")
            global_browser_profile = BrowserProfile()
            session = BrowserSession.get(global_browser_profile)
            session = await session.start()
            init_page = await session.new_page()  # never use old pages in global session due to really difficult race conditions with concurrent requests
            try:
                await init_page.goto(initial_url)
                await init_page.close()
            except Exception as e:
                await report_distill_error(
                    error=e,
                    page=init_page,
                    profile_id=global_browser_profile.id,
                    location=initial_url,
                    hostname=urllib.parse.urlparse(initial_url).hostname or "",
                    iteration=0,
                )
            await asyncio.sleep(1)

        browser_profile = global_browser_profile

    if not incognito or signin_id is not None:
        # First, try without any interaction as this will work if the user signed in previously (using global browser profile or incognito with signin_id)
        terminated, distilled, converted = await run_distillation_loop(
            initial_url,
            patterns,
            browser_profile=browser_profile,
            interactive=False,
            timeout=timeout,
            stop_ok=False,  # Keep global session alive
        )
        if terminated:
            distillation_result = converted if converted is not None else distilled
            return {result_key: distillation_result}

    # If that didn't work, try signing in via distillation
    session = BrowserSession.get(browser_profile)
    await session.start()
    page = await session.context.new_page()
    id = await dpage_add(page, initial_url, browser_profile.id)

    if incognito:
        incognito_browser_profiles[id] = browser_profile

    host = headers.get("x-forwarded-host") or headers.get("host")
    if host is None:
        logger.warning("Missing Host header; defaulting to localhost")
        base_url = "http://localhost:23456"
    else:
        default_scheme = "http" if is_local_address(host) else "https"
        scheme = headers.get("x-forwarded-proto", default_scheme)
        base_url = f"{scheme}://{host}"

    url = f"{base_url}/dpage/{id}"
    logger.info(f"Continue with the sign in at {url}", extra={"url": url, "id": id})
    return {
        "url": url,
        "message": f"Continue to sign in in your browser at {url}.",
        "signin_id": id,
        "system_message": (
            f"Try open the url {url} in a browser with a tool if available."
            "Give the url to the user so the user can open it manually in their browser."
            "Then call check_signin tool with the signin_id to check if the sign in process is completed. "
            "Once it is completed successfully, then call this tool again to proceed with the action."
        ),
    }


async def zen_post_dpage(page: zd.Tab, id: str, request: Request) -> HTMLResponse:
    form_data = await request.form()
    fields: dict[str, str] = {k: str(v) for k, v in form_data.items()}

    path = os.path.join(os.path.dirname(__file__), "patterns", "**/*.html")
    patterns = load_distillation_patterns(path)

    logger.info(f"Continuing distillation for page {id}...")
    logger.debug(f"Available distillation patterns: {len(patterns)}")

    TICK = 1  # seconds
    TIMEOUT = 15  # seconds
    max = TIMEOUT // TICK

    current = Match(name="", priority=-1, distilled="")

    if settings.LOG_LEVEL == "DEBUG":
        await zen_capture_page_artifacts(page, identifier=id, prefix="dpage_debug")

    for iteration in range(max):
        logger.debug(f"Iteration {iteration + 1} of {max}")
        await asyncio.sleep(TICK)

        hostname = str(urllib.parse.urlparse(page.url).hostname) if page.url else None

        match = await zen_distill(hostname, page, patterns)
        if not match:
            logger.info("No matched pattern found")
            continue

        if match.distilled == current.distilled:
            logger.info(f"Still the same: {match.name}")
            continue

        current = match
        distilled = match.distilled

        title_element = BeautifulSoup(distilled, "html.parser").find("title")
        title = title_element.get_text() if title_element is not None else DEFAULT_TITLE
        action = f"/dpage/{id}"
        options = {"title": title, "action": action}

        if await terminate(distilled):
            logger.info("Finished!")
            converted = await convert(distilled)

            if id in pending_actions:
                action_info = pending_actions[id]
                logger.info(f"Signin completed for {id}, resuming action...")

                action_result = await zen_dpage_with_action(
                    initial_url=action_info["initial_url"],
                    action=action_info["action"],
                    timeout=action_info["timeout"],
                    _signin_completed=True,
                    _page_id=id,
                )

                distillation_results[id] = action_result

                del pending_actions[id]
                await dpage_close(id)
                return HTMLResponse(render(FINISHED_MSG, options))

            await dpage_close(id)
            if converted is not None:
                print(converted)
                distillation_results[id] = converted
            else:
                logger.info("No conversion found")
                distillation_results[id] = distilled
            return HTMLResponse(render(FINISHED_MSG, options))

        names: list[str] = []
        document = BeautifulSoup(distilled, "html.parser")
        inputs = document.find_all("input")

        if fields.get("button"):
            button = document.find("button", value=str(fields.get("button")))
            if button:
                logger.info(f"Clicking button button[value={fields.get('button')}]")
                await zen_autoclick(page, distilled, f"button[value={fields.get('button')}]")
                continue

        for input in inputs:
            if isinstance(input, Tag):
                gg_match = input.get("gg-match")
                element = await page_query_selector(
                    page, selector=str(gg_match) if gg_match is not None else ""
                )
                name = input.get("name")
                input_type = input.get("type")

                if element:
                    if input_type == "checkbox":
                        if not name:
                            logger.warning(f"No name for the checkbox {gg_match}")
                            continue
                        value = fields.get(str(name))
                        checked = value and len(str(value)) > 0
                        names.append(str(name))
                        logger.info(f"Status of checkbox {name}={checked}")
                        current_checked_value = (
                            element.element.get("checked") or element.element.get("value") == "true"
                        )
                        if current_checked_value != checked:
                            logger.info(f"Clicking checkbox {name} to set it to {checked}")
                            await element.click()
                    elif input_type == "radio":
                        if name is not None:
                            name_str = str(name)
                            value = fields.get(name_str)
                            if not value or len(value) == 0:
                                logger.warning(f"No form data found for radio button group {name}")
                                continue
                            radio = document.find("input", {"type": "radio", "value": str(value)})
                            if not radio or not isinstance(radio, Tag):
                                logger.warning(f"No radio button found with value {value}")
                                continue
                            logger.info(f"Handling radio button group {name}")
                            logger.info(f"Using form data {name}={value}")
                            radio_element = await page_query_selector(
                                page, selector=str(radio.get("gg-match"))
                            )
                            if radio_element:
                                await radio_element.click()
                                radio["checked"] = "checked"
                                current.distilled = str(document)
                                names.append(str(input.get("id")) if input.get("id") else "radio")
                    elif name is not None:
                        name_str = str(name)
                        value = fields.get(name_str)
                        if value and len(value) > 0:
                            logger.info(f"Using form data {name}")
                            names.append(name_str)
                            input["value"] = value
                            current.distilled = str(document)
                            await element.type_text(value)
                            del fields[name_str]
                        else:
                            logger.info(f"No form data found for {name}")

        await zen_autoclick(page, distilled, "[gg-autoclick]:not(button)")
        SUBMIT_BUTTON = "button[gg-autoclick], button[type=submit]"
        if document.select(SUBMIT_BUTTON):
            if len(names) > 0 and len(inputs) == len(names):
                logger.info("Submitting form, all fields are filled...")
                await zen_autoclick(page, distilled, SUBMIT_BUTTON)
                continue
            logger.warning("Not all form fields are filled")
            return HTMLResponse(render(str(document.find("body")), options))

    hostname_attr: str | None = getattr(page, "hostname", None)  # type: ignore[assignment]
    location = getattr(page, "url", "unknown")  # type: ignore[assignment]
    timeout_error = TimeoutError("Timeout reached in zen_post_dpage")
    await zen_report_distill_error(
        error=timeout_error,
        page=page,
        profile_id=id,
        location=location,
        hostname=hostname_attr or "unknown",
        iteration=max,
    )
    raise HTTPException(status_code=503, detail="Timeout reached")


async def zen_dpage_mcp_tool(initial_url: str, result_key: str, timeout: int = 2) -> dict[str, Any]:
    """Generic MCP tool based on distillation with Zendriver"""
    path = os.path.join(os.path.dirname(__file__), "patterns", "**/*.html")
    patterns = load_distillation_patterns(path)

    headers = get_http_headers(include_all=True)
    incognito = headers.get("x-incognito", "0") == "1"
    signin_id = headers.get("x-signin-id") or None

    browser = None
    if incognito:
        browser = await init_zendriver_browser(signin_id)
    else:
        global zen_global_browser
        if zen_global_browser is None:
            logger.info("Creating global browser for Zendriver...")
            zen_global_browser = await init_zendriver_browser()
            await get_new_page(zen_global_browser)
            logger.info(f"Global browser created with id {zen_global_browser.id}")  # type: ignore[attr-defined]
        browser = zen_global_browser

    if not incognito or signin_id is not None:
        # First, try without any interaction as this will work if the user signed in previously
        terminated, distilled, converted = await zen_run_distillation_loop(
            initial_url, patterns, browser, timeout, interactive=False
        )
        if terminated:
            distillation_result = converted if converted is not None else distilled
            return {result_key: distillation_result}

    page = await get_new_page(browser)
    page.hostname = urllib.parse.urlparse(initial_url).hostname  # type: ignore[attr-defined]

    id = await dpage_add(page, initial_url, browser.id)  # type: ignore[attr-defined]

    if incognito:
        incognito_browsers[id] = browser

    host = headers.get("x-forwarded-host") or headers.get("host")
    if host is None:
        logger.warning("Missing Host header; defaulting to localhost")
        base_url = "http://localhost:23456"
    else:
        default_scheme = "http" if is_local_address(host) else "https"
        scheme = headers.get("x-forwarded-proto", default_scheme)
        base_url = f"{scheme}://{host}"

    url = f"{base_url}/dpage/{id}"
    logger.info(f"Continue with the sign in at {url}", extra={"url": url, "id": id})
    return {
        "url": url,
        "message": f"Continue to sign in in your browser at {url}.",
        "signin_id": id,
        "system_message": (
            f"Try open the url {url} in a browser with a tool if available."
            "Give the url to the user so the user can open it manually in their browser."
            "Then call check_signin tool with the signin_id to check if the sign in process is completed. "
            "Once it is completed successfully, then call this tool again to proceed with the action."
        ),
    }


async def dpage_with_action(
    initial_url: str,
    action: Any,
    timeout: int = 2,
    _signin_completed: bool = False,
    _page_id: str | None = None,
) -> dict[str, Any]:
    """Execute an action after signin completion.

    Args:
        initial_url: URL to navigate to
        action: Async function that receives a Page and returns a dict
        timeout: Timeout in seconds
        _signin_completed: Whether the signin process is completed
        _page_id: ID of the page to resume from
    Returns:
        Dict with result or signin flow info
    """
    headers = get_http_headers(include_all=True)
    incognito = headers.get("x-incognito", "0") == "1"
    signin_id = headers.get("x-signin-id") or None
    global global_browser_profile
    global incognito_browser_profiles

    # Step 1: If resuming after signin completion, use the active page directly
    if _signin_completed and _page_id is not None and _page_id in active_pages:
        logger.info(f"Resuming action after signin with page_id={_page_id}")
        page = active_pages[_page_id]
        action_info = pending_actions[_page_id]
        if isinstance(page, Page):
            await page.goto(initial_url, wait_until="commit")
        else:
            await zen_navigate_with_retry(page, initial_url)
        result = await action(page, action_info["browser_profile"])
        return result

    # Step 2: If global_browser_profile exists, try executing action directly
    # This will work if user signed in previously and session is still valid
    if (global_browser_profile is not None and not incognito) or signin_id is not None:
        if global_browser_profile is not None and not incognito:
            browser_profile = global_browser_profile
        else:
            browser_profile = await get_incognito_browser_profile(signin_id=signin_id)
        try:
            logger.info("Trying action with existing global browser session...")
            session = BrowserSession.get(browser_profile)
            await session.start()
            page = await session.new_page()
            await page.goto(initial_url, wait_until="commit")
            result = await action(page, browser_profile)
            await page.close()
            logger.info("Action succeeded with existing session!")
            return result
        except Exception as e:
            logger.info(
                f"dpage_with_action failed with existing session (likely not signed in): {e}"
            )

    # Step 3: User not signed in - create interactive signin flow with action
    # Create or get browser profile for signin flow
    browser_profile: BrowserProfile
    if incognito:
        browser_profile = await get_incognito_browser_profile(signin_id=signin_id)
    else:
        if global_browser_profile is None:
            logger.info("Creating global browser profile for signin flow...")
            global_browser_profile = BrowserProfile()
        browser_profile = global_browser_profile

    session = BrowserSession.get(browser_profile)
    await session.start()
    page = await session.context.new_page()
    id = await dpage_add(page, initial_url, browser_profile.id)

    # Store action for auto-resumption after signin
    pending_actions[id] = {
        "action": action,
        "initial_url": initial_url,
        "timeout": timeout,
        "page_id": id,
        "browser_profile": browser_profile,
    }

    if incognito:
        incognito_browser_profiles[id] = browser_profile

    host = headers.get("x-forwarded-host") or headers.get("host")
    if host is None:
        logger.warning("Missing Host header; defaulting to localhost")
        base_url = "http://localhost:23456"
    else:
        default_scheme = "http" if is_local_address(host) else "https"
        scheme = headers.get("x-forwarded-proto", default_scheme)
        base_url = f"{scheme}://{host}"

    url = f"{base_url}/dpage/{id}"
    logger.info(f"dpage_with_action: Continue with sign in at {url}", extra={"url": url, "id": id})

    message = "Continue to sign in in your browser"

    return {
        "url": url,
        "message": f"{message} at {url}.",
        "signin_id": id,
        "system_message": (
            f"Try open the url {url} in a browser with a tool if available."
            "Give the url to the user so the user can open it manually in their browser."
            f"Then call check_signin tool with the signin_id to check if the sign in process is completed. "
        ),
    }


async def zen_dpage_with_action(
    initial_url: str,
    action: Any,
    timeout: int = 2,
    _signin_completed: bool = False,
    _page_id: str | None = None,
) -> dict[str, Any]:
    """Execute an action after signin completion with Zendriver.

    Args:
        initial_url: URL to navigate to
        action: Async function that receives a Page and returns a dict
        timeout: Timeout in seconds
        _signin_completed: Whether the signin process is completed
        _page_id: ID of the page to resume from
    Returns:
        Dict with result or signin flow info
    """
    headers = get_http_headers(include_all=True)
    incognito = headers.get("x-incognito", "0") == "1"
    signin_id = headers.get("x-signin-id") or None
    global zen_global_browser
    global incognito_browsers

    # Step 1: If resuming after signin completion, use the active page directly
    if _signin_completed and _page_id is not None and _page_id in active_pages:
        logger.info(f"Resuming action after signin with page_id={_page_id}")
        page = active_pages[_page_id]

        if not isinstance(page, zd.Tab):
            raise ValueError(f"Expected Zendriver Tab for page {_page_id}, got {type(page)}")

        action_info = pending_actions[_page_id]

        try:
            await zen_navigate_with_retry(page, initial_url)
        except Exception as e:
            logger.warning(f"Failed to navigate to {initial_url}: {e}")

        result = await action(page, action_info["browser"])
        return result

    # Step 2: If global_browser_profile exists, try executing action directly
    # This will work if user signed in previously and session is still valid
    if (zen_global_browser is not None and not incognito) or signin_id is not None:
        browser = None
        if zen_global_browser is not None and not incognito:
            browser = zen_global_browser
        else:
            browser = await init_zendriver_browser(signin_id)

        try:
            logger.info("Trying action with existing global browser session...")
            page = await get_new_page(browser)
            await zen_navigate_with_retry(page, initial_url)
            result = await action(page, browser)
            await page.close()
            logger.info("Action succeeded with existing session!")
            return result
        except Exception as e:
            logger.info(
                f"zen_dpage_with_action failed with existing session (likely not signed in): {e}"
            )

    # Step 3: User not signed in - create interactive signin flow with action
    browser_instance: zd.Browser
    if incognito:
        browser_instance = await init_zendriver_browser(signin_id)
    else:
        if zen_global_browser is None:
            logger.info("Creating global browser for Zendriver signin flow...")
            zen_global_browser = await init_zendriver_browser()
            await get_new_page(zen_global_browser)
        browser_instance = zen_global_browser

    page = await get_new_page(browser_instance)
    page.hostname = urllib.parse.urlparse(initial_url).hostname  # type: ignore

    id = await dpage_add(page, initial_url, browser_instance.id)  # type: ignore

    # Store action for auto-resumption after signin
    pending_actions[id] = {
        "action": action,
        "initial_url": initial_url,
        "timeout": timeout,
        "page_id": id,
        "browser": browser_instance,
    }

    if incognito:
        incognito_browsers[id] = browser_instance

    host = headers.get("x-forwarded-host") or headers.get("host")
    if host is None:
        logger.warning("Missing Host header; defaulting to localhost")
        base_url = "http://localhost:23456"
    else:
        default_scheme = "http" if is_local_address(host) else "https"
        scheme = headers.get("x-forwarded-proto", default_scheme)
        base_url = f"{scheme}://{host}"

    url = f"{base_url}/dpage/{id}"
    logger.info(
        f"zen_dpage_with_action: Continue with sign in at {url}", extra={"url": url, "id": id}
    )

    message = "Continue to sign in in your browser"

    return {
        "url": url,
        "message": f"{message} at {url}.",
        "signin_id": id,
        "system_message": (
            f"Try open the url {url} in a browser with a tool if available."
            "Give the url to the user so the user can open it manually in their browser."
            f"Then call check_signin tool with the signin_id to check if the sign in process is completed. "
        ),
    }
