from typing import Any

import zendriver as zd

from getgather.mcp.dpage import zen_dpage_with_action
from getgather.mcp.registry import GatherMCP
from getgather.zen_actions import parse_response_json

alfagift_mcp = GatherMCP(brand_id="alfagift", name="Alfagift MCP")


@alfagift_mcp.tool
async def get_cart() -> dict[str, Any]:
    """Get cart alfagift."""

    async def action(tab: zd.Tab, _) -> dict[str, Any]:
        results: dict[str, Any] = {}
        async with tab.expect_response(".*/active-cart-by-memberId.*") as resp:
            await tab.get("https://alfagift.id/cart")
            results = await parse_response_json(resp, {}, "cart")

        return {"alfagift_cart": results["data"]["listCartDetail"]}

    return await zen_dpage_with_action(
        "https://alfagift.id",
        action,
    )


@alfagift_mcp.tool
async def get_order_done() -> dict[str, Any]:
    """Get order done alfagift."""

    async def action(tab: zd.Tab, _) -> dict[str, Any]:
        results: dict[str, Any] = {}
        async with tab.expect_response(".*/order-ereceipt-service/list/complete.*") as resp:
            await tab.get("https://alfagift.id/order-done")
            results = await parse_response_json(resp, {}, "order-done")

        return {"alfagift_order_done": results["data"]}

    return await zen_dpage_with_action(
        "https://alfagift.id",
        action,
    )
