"""Read-only station mode selector for a Home Assistant Pipecat integration.

The caller registers the station_mode() function as an ASGI GET endpoint.
No public auth: bind this route to the local trusted HA network only.
Do not expose Supervisor tokens or accept mode changes over this endpoint.
"""
from __future__ import annotations

import httpx

SELECTOR = "input_select.komnata_assistent"


async def station_mode(supervisor: str, token: str) -> dict[str, str]:
    """Answer Codex only when the HA selector is explicitly GPT Live.

    On errors fail closed to stock Alice; no cloud model is silently selected.
    """
    if not token:
        return {"status": "ok", "mode": "alice"}
    endpoint = supervisor.rstrip("/") + "/core/api/states/" + SELECTOR
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=1.5) as client:
            response = await client.get(
                endpoint, headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            data = response.json()
            value = str(data.get("state", "")).strip().casefold()
            if value == "gpt live":
                return {"status": "ok", "mode": "codex"}
            if value == "quasar assistant":
                return {"status": "ok", "mode": "quasar"}
    except (httpx.HTTPError, ValueError, AttributeError, TypeError):
        pass
    return {"status": "ok", "mode": "alice"}
