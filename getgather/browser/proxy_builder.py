"""Proxy configuration builder with template-based dynamic parameter replacement."""

import re
from typing import Any

from getgather.api.types import RequestInfo
from getgather.browser.proxy_types import ProxyConfig
from getgather.logs import logger


def build_proxy_config(
    proxy_config: ProxyConfig,
    profile_id: str,
    request_info: RequestInfo | None = None,
) -> dict[str, str] | None:
    """Build proxy configuration dict with dynamic parameter replacement.

    Args:
        proxy_config: ProxyConfig instance to build from
        profile_id: Profile ID to use as session identifier
        request_info: Optional request information with location data

    Returns:
        dict: Proxy configuration with server, username, password
        None: If no server configured or proxy type is 'none'
    """
    # Handle 'none' proxy type - no proxy
    if proxy_config.proxy_type == "none":
        logger.info("Proxy type is 'none', skipping proxy")
        return None

    # Extract values for template replacement
    values = _extract_values(profile_id, request_info)

    # Format 1: url_template (full URL with credentials and dynamic params)
    if proxy_config.url_template:
        full_url = _build_params(proxy_config.url_template, values)
        if not full_url:
            logger.warning("url_template resulted in empty string, skipping proxy")
            return None

        # Parse the built URL to extract components
        temp_config = ProxyConfig(url=full_url)
        if not temp_config.server:
            logger.warning(f"Failed to parse url_template result: {temp_config.masked_url}")
            return None

        result = {
            "server": temp_config.server,
        }
        if temp_config.base_username:
            result["username"] = temp_config.base_username
        if temp_config.password:
            result["password"] = temp_config.password

        logger.info(
            f"Built proxy config from url_template - server: {temp_config.server}, "
            f"username: {temp_config.base_username}, has_password: {bool(temp_config.password)}"
        )
        return result

    # Format 2: Separate components (url + username_template + password)
    if not proxy_config.server:
        logger.info("No proxy server configured, skipping proxy")
        return None

    # Build username from base + template
    username = None

    # Priority: username_template > base_username
    if proxy_config.username_template:
        # Build from template (may not need base_username)
        params = _build_params(proxy_config.username_template, values)
        if params:
            prefix = proxy_config.username_template.split("-", 1)[0]
            username = f"{prefix}-{params}"
    elif proxy_config.base_username:
        # Use base username if no template
        username = proxy_config.base_username

    if username:
        logger.info(f"Built proxy username: {username}")

    result = {
        "server": proxy_config.server,
    }
    if username:
        result["username"] = username
    if proxy_config.password:
        result["password"] = proxy_config.password

    logger.info(
        f"Built proxy config - server: {proxy_config.server}, username: {username}, "
        f"has_password: {bool(proxy_config.password)}"
    )
    return result


def _extract_values(profile_id: str, request_info: RequestInfo | None) -> dict[str, Any]:
    """Extract replacement values from request info.

    Args:
        profile_id: Profile ID to use as session identifier
        request_info: Optional request information

    Returns:
        dict: Mapping of placeholder names to values
    """
    values = {
        "session_id": profile_id,  # Use profile_id as session identifier
    }

    if not request_info:
        return values

    country = None
    if request_info.country:
        country = request_info.country.lower()
        values["country"] = country

    # Only include state for US requests (state-us_{state} format)
    if request_info.state and country == "us":
        values["state"] = request_info.state.lower().replace(" ", "_")

    if request_info.city:
        values["city"] = request_info.city.lower().replace(" ", "_")
        # city_compacted: removes dashes, underscores, and spaces
        values["city_compacted"] = (
            request_info.city.lower().replace("-", "").replace("_", "").replace(" ", "")
        )
    if request_info.postal_code:
        values["postal_code"] = request_info.postal_code

    return values


def _build_params(template: str, values: dict[str, Any]) -> str:
    """Build params string by only including segments with actual values.

    Splits template by placeholders and only joins segments that have values.

    Examples:
    - Template: 'cc-{country}-city-{city}', values: {'country': 'us'} -> 'cc-us'
    - Template: 'cc-{country}-city-{city}', values: {} -> ''
    - Template: 'state-us_{state}', values: {'state': 'ca'} -> 'state-us_ca'
    - Template: 'state-us_{state}', values: {} -> ''

    Args:
        template: Template string with {placeholders}
        values: Mapping of placeholder names to values

    Returns:
        str: Params with only segments that have values, or empty string
    """
    # Split by placeholders to get segments
    # We'll rebuild by only including segments where we have values
    parts: list[str] = []
    current = template

    # Find all placeholders in order
    placeholders: list[str] = re.findall(r"\{([^}]+)\}", template)

    for placeholder in placeholders:
        # Split on this placeholder
        before, _, after = current.partition(f"{{{placeholder}}}")

        # If we have a value for this placeholder, include the segment
        if placeholder in values and values[placeholder] is not None:
            parts.append(before + str(values[placeholder]))

        current = after

    # Add any remaining text
    if current:
        parts.append(current)

    result = "".join(parts)

    # Clean up separators at start/end
    result = result.strip("-_")

    return result
