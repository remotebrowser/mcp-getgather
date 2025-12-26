from typing import Any

from getgather.mcp.dpage import zen_dpage_mcp_tool
from getgather.mcp.registry import GatherMCP

jetblue_mcp = GatherMCP(brand_id="jetblue", name="JetBlue MCP")


@jetblue_mcp.tool
async def get_profile() -> dict[str, Any]:
    """Get profile of jetblue."""
    return await zen_dpage_mcp_tool("https://www.jetblue.com/", "jetblue_profile")
