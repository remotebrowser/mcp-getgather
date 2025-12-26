from typing import Any

from getgather.mcp.dpage import zen_dpage_mcp_tool
from getgather.mcp.registry import GatherMCP

aliexpress_mcp = GatherMCP(brand_id="aliexpress", name="AliExpress MCP")


@aliexpress_mcp.tool
async def get_orders() -> dict[str, Any]:
    """Get orders of aliexpress."""
    return await zen_dpage_mcp_tool(
        "https://www.aliexpress.com/p/order/index.html",
        "aliexpress_orders",
    )
