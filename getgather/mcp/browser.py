from datetime import datetime, timedelta
from typing import TypedDict, cast

import zendriver as zd
from zendriver.core.browser import shutil

from getgather.config import settings
from getgather.logs import logger


async def terminate_zendriver_browser(browser: zd.Browser):
    await browser.stop()
    browser_id = cast(str, browser.id)  # type: ignore[attr-defined]
    user_data_dir = settings.profiles_dir / browser_id
    logger.info(
        f"Terminating Zendriver browser with user_data_dir: {user_data_dir}",
        extra={"profile_id": browser_id},
    )
    for directory in [
        "Default/DawnGraphiteCache",
        "Default/DawnWebGPUCache",
        "Default/GPUCache",
        "Default/Code Cache",
        "Default/Cache",
        "GraphiteDawnCache",
        "GrShaderCache",
        "ShaderCache",
        "Subresource Filter",
        "segmentation_platform",
    ]:
        path = user_data_dir / directory

        if path.exists():
            try:
                shutil.rmtree(path)
            except Exception as e:
                logger.warning(f"Failed to remove {directory}: {e}")


class BrowserInformation(TypedDict):
    last_active_timestamp: datetime


class BrowserManager:
    """Manages browser instances."""

    def __init__(self):
        self._incognito_browsers: dict[str, zd.Browser] = {}
        self._zen_global_browser: zd.Browser | None = None
        self._browser_information: dict[str, BrowserInformation] = {}

    def get_incognito_browser(self, id: str) -> zd.Browser | None:
        """Get an incognito browser by ID."""
        self.update_last_active(id)
        return self._incognito_browsers.get(id)

    def set_incognito_browser(self, id: str, browser: zd.Browser) -> None:
        """Set an incognito browser by ID."""
        self.update_last_active(id)
        self._incognito_browsers[id] = browser

    def has_incognito_browser(self, id: str) -> bool:
        """Check if an incognito browser exists by ID."""
        return id in self._incognito_browsers

    def get_global_browser(self) -> zd.Browser | None:
        """Get the global browser instance."""
        return self._zen_global_browser

    def set_global_browser(self, browser: zd.Browser) -> None:
        """Set the global browser instance."""
        self._zen_global_browser = browser

    def update_last_active(self, id: str):
        """Update the last active timestamp for this session."""
        if id not in self._browser_information:
            self._browser_information[id] = {"last_active_timestamp": datetime.now()}
        self._browser_information[id]["last_active_timestamp"] = datetime.now()

    async def cleanup_incognito_browsers(self):
        """Cleanup incognito browsers that have not been used in the last 1 hour."""
        current_time = datetime.now()
        max_session_age = timedelta(minutes=settings.BROWSER_SESSION_AGE)
        signin_ids = self._incognito_browsers.keys()

        logger.info(f"Checking for old browsers to stop. Found {len(signin_ids)} browsers")

        # Find sessions that are older than max_session_age
        for signin_id in signin_ids:
            browser_information = self._browser_information.get(signin_id)
            if browser_information is None:
                logger.warning(
                    f"Signin ID {signin_id} has no browser information, skipping cleanup check"
                )
                continue

            last_active_timestamp = browser_information.get("last_active_timestamp")

            session_age = current_time - last_active_timestamp
            if session_age > max_session_age:
                try:
                    logger.info(
                        f"Signin ID {signin_id} has been inactive for more than {settings.BROWSER_SESSION_AGE} minutes, stopping it"
                    )
                    browser = self._incognito_browsers.get(signin_id)
                    if browser is None:
                        logger.warning(f"Signin ID {signin_id} not found, skipping termination")
                        continue
                    await terminate_zendriver_browser(browser)
                    logger.info(f"Successfully stopped browser with signin ID {signin_id}")
                except Exception as e:
                    logger.error(f"Failed to stop browser with signin ID {signin_id}: {e}")


browser_manager = BrowserManager()
