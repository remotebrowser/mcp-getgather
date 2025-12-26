from typing import Any

import zendriver as zd

from getgather.mcp.dpage import zen_dpage_with_action
from getgather.mcp.registry import GatherMCP
from getgather.zen_actions import parse_response_json

americanairlines_mcp = GatherMCP(brand_id="americanairlines", name="American Airlines MCP")


@americanairlines_mcp.tool
async def get_upcoming_flights() -> dict[str, Any]:
    """Get upcoming flights of americanairlines."""

    async def action(tab: zd.Tab, _) -> dict[str, Any]:
        async with tab.expect_response(".*loyalty/api/upcoming-trips.*") as resp:
            data = await parse_response_json(resp, {}, "upcoming flights")
        return {"americanairlines_upcoming_flights": data}

    return await zen_dpage_with_action(
        "https://www.aa.com/aadvantage-program/profile/account-summary",
        action,
    )


@americanairlines_mcp.tool
async def get_recent_activity() -> dict[str, Any]:
    """Get recent activity (purchase history) of americanairlines."""

    async def action(tab: zd.Tab, _) -> dict[str, Any]:
        async with tab.expect_response(
            ".*api/loyalty/miles/transaction/orchestrator/memberActivity.*"
        ) as resp:
            data = await parse_response_json(resp, {}, "recent activity")

        return {"americanairlines_recent_activity": data}

    return await zen_dpage_with_action(
        "https://www.aa.com/aadvantage-program/profile/account-summary",
        action,
    )
