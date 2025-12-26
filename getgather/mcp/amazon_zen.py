import asyncio
import json
from datetime import datetime
from typing import Any

import zendriver as zd

from getgather.distill import convert
from getgather.logs import logger
from getgather.mcp.dpage import zen_dpage_mcp_tool, zen_dpage_with_action
from getgather.mcp.registry import GatherMCP
from getgather.zen_distill import page_query_selector

amazon_zen_mcp = GatherMCP(brand_id="amazon_zen", name="Amazon Zen MCP")


@amazon_zen_mcp.tool
async def search_purchase_history(keyword: str, page_number: int = 1) -> dict[str, Any]:
    """Search purchase history from amazon."""
    return await zen_dpage_mcp_tool(
        f"https://www.amazon.com/your-orders/search?page={page_number}&search={keyword}",
        "order_history",
    )


@amazon_zen_mcp.tool
async def get_purchase_history(
    year: str | int | None = None, start_index: int = 0
) -> dict[str, Any]:
    """Get purchase/order history of a amazon with dpage."""

    if year is None:
        target_year = datetime.now().year
    elif isinstance(year, str):
        try:
            target_year = int(year)
        except ValueError:
            target_year = datetime.now().year
    else:
        target_year = int(year)

    current_year = datetime.now().year
    if not (1900 <= target_year <= current_year + 1):
        raise ValueError(f"Year {target_year} is out of valid range (1900-{current_year + 1})")

    return await zen_dpage_mcp_tool(
        f"https://www.amazon.com/your-orders/orders?timeFilter=year-{target_year}&startIndex={start_index}",
        "amazon_purchase_history",
    )


@amazon_zen_mcp.tool
async def search_product(keyword: str) -> dict[str, Any]:
    """Search product on amazon."""
    return await zen_dpage_mcp_tool(
        f"https://www.amazon.com/s?k={keyword}",
        "product_list",
    )


@amazon_zen_mcp.tool
async def get_browsing_history() -> dict[str, Any]:
    """Get browsing history from amazon."""

    async def get_browsing_history_action(page: zd.Tab, _) -> dict[str, Any]:
        logger.info(f"Getting browsing history from {page.url}")
        current_url = page.url
        if current_url is None or "signin" in current_url:
            raise Exception("User is not signed in")

        is_empty = await page_query_selector(
            page, "//span[contains(., 'You have no recently viewed items.')"
        )
        logger.info(f"is_empty: {is_empty}")
        if is_empty:
            logger.info(f"No browsing history")
            return {"browsing_history_data": []}

        logger.info(f"Navigating to {page.url}")

        await page.send(zd.cdp.page.reload())
        logger.info("Page reloaded, waiting for browsing-history API response")

        browsing_history_api_url = None
        request_headers = None
        async with page.expect_response(".*browsing-history/.*") as response:
            logger.info("Waiting for browsing-history API response")
            response_value = await response.value
            browsing_history_api_url = response_value.response.url
            logger.info(f"Found browsing history API URL: {browsing_history_api_url}")
            request_value = await response.request
            request_headers = request_value.headers
            logger.debug(
                f"Request headers captured: {len(request_headers) if request_headers else 0} headers"
            )

        # Extract output from data-client-recs-list attribute
        logger.info("Extracting browsing history IDs from data-client-recs-list attribute")

        raw_attribute = await page.evaluate("""
            (() => {{
                const element = document.querySelector('div[data-client-recs-list]');
                return element ? element.getAttribute('data-client-recs-list') : null;
            }})()
        """)
        logger.info(
            f"Raw attribute value: {raw_attribute[:200] if raw_attribute and len(str(raw_attribute)) > 200 else raw_attribute}"
        )
        raw_attribute_str = str(raw_attribute) if raw_attribute is not None else "[]"
        output = [json.dumps(item) for item in json.loads(raw_attribute_str)]
        logger.info(f"Extracted {len(output)} browsing history IDs")

        async def get_browsing_history(start_index: int, end_index: int):
            logger.info(
                f"Getting browsing history batch: indices {start_index} to {end_index} (batch size: {end_index - start_index})"
            )

            # Convert headers dict to JavaScript object format
            headers_js = json.dumps(request_headers or {})
            ids_js = json.dumps(output[start_index:end_index])
            logger.info(
                f"Requesting {len(output[start_index:end_index])} items from API: {browsing_history_api_url}"
            )

            try:
                html = await page.evaluate(f"""
                    (async () => {{
                        const headers = {headers_js};
                        const ids = {ids_js};
                        const res = await fetch('{browsing_history_api_url}', {{
                            method: 'POST',
                            headers: headers,
                            credentials: 'include',
                            body: JSON.stringify({{"ids": ids}})
                        }});
                        if (!res.ok) {{
                            throw new Error(`HTTP error! status: ${{res.status}}`);
                        }}
                        return await res.text();
                    }})()
                """)
                logger.info(
                    f"Received HTML response for batch {start_index}-{end_index}, length: {len(html) if html else 0} characters"
                )
                logger.info(f"HTML: {html[:200] if html and len(html) > 200 else html}")
            except Exception as e:
                logger.info(f"Error fetching browsing history batch {start_index}-{end_index}: {e}")
                raise
            distilled = f"""
                <html gg-domain="amazon">
                    <body>
                        {html}
                    </body>
                    <script type="application/json" id="browsing_history">
                        {{
                            "rows": "div#gridItemRoot",
                            "columns": [
                                {{
                                    "name": "name",
                                    "selector": "a.a-link-normal > span > div"
                                }},
                                {{
                                    "name": "url",
                                    "selector": "div[class*='uncoverable-faceout'] > a[class='a-link-normal aok-block']",
                                    "attribute": "href"
                                }},
                                {{
                                    "name": "image_url",
                                    "selector": "a > div > img.a-dynamic-image",
                                    "attribute": "src"
                                }},
                                {{
                                    "name": "rating",
                                    "selector": "div.a-icon-row > a > i > span"
                                }},
                                {{
                                    "name": "rating_count",
                                    "selector": "div.a-icon-row > a > span"
                                }},
                                {{
                                    "name": "price",
                                    "selector": "span.a-color-price > span"
                                }},
                                {{
                                    "name": "price_unit",
                                    "selector": "span[class='a-size-mini a-color-price aok-nowrap']"
                                }},
                                {{
                                    "name": "delivery_message",
                                    "selector": "div.udm-primary-delivery-message"
                                }}
                            ]
                        }}
                    </script>
                </html>
            """
            logger.debug(f"Converting distilled HTML for batch {start_index}-{end_index}")
            converted = await convert(distilled)
            if converted is not None:
                logger.info(
                    f"Converted batch {start_index}-{end_index}: found {len(converted)} items"
                )
                for item in converted:
                    item["url"] = f"https://www.amazon.com{item['url']}"
            else:
                logger.warning(f"Conversion returned None for batch {start_index}-{end_index}")
            return converted

        num_batches = (len(output) + 99) // 100
        logger.info(f"Fetching browsing history in {num_batches} batch(es) of up to 100 items each")
        browsing_history_list = await asyncio.gather(*[
            get_browsing_history(i, i + 100) for i in range(0, len(output), 100)
        ])
        logger.info(f"Completed fetching all {num_batches} batch(es)")

        # Flatten the list of lists
        logger.info("Flattening browsing history results")
        flattened_history: list[Any] = []
        for idx, batch in enumerate(browsing_history_list):
            if batch is not None:
                logger.debug(f"Adding batch {idx} with {len(batch)} items to flattened history")
                flattened_history.extend(batch)
            else:
                logger.warning(f"Batch {idx} was None, skipping")

        logger.info(f"Total browsing history items collected: {len(flattened_history)}")
        return {"browsing_history_data": flattened_history}

    return await zen_dpage_with_action(
        "https://www.amazon.com/gp/history?ref_=nav_AccountFlyout_browsinghistory",
        action=get_browsing_history_action,
    )
