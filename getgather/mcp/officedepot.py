from typing import Any

from getgather.mcp.dpage import zen_dpage_mcp_tool
from getgather.mcp.registry import GatherMCP

officedepot_mcp = GatherMCP(brand_id="officedepot", name="Office Depot MCP")


@officedepot_mcp.tool
async def get_purchase_history() -> dict[str, Any]:
    """Get the purchase history from a user's account."""
    return await zen_dpage_mcp_tool(
        "https://www.officedepot.com/orderhistory/orderHistoryListSet.do?ordersInMonths=0&orderType=ALL&orderStatus=A&searchValue=",
        "officedepot_purchase_history",
    )


@officedepot_mcp.tool
async def get_purchase_history_details(order_number: str) -> dict[str, Any]:
    """Get the purchase history details for a specific order."""
    return await zen_dpage_mcp_tool(
        f"https://www.officedepot.com/orderhistory/orderHistoryDetail.do?id={order_number}",
        "officedepot_purchase_history_details",
    )
