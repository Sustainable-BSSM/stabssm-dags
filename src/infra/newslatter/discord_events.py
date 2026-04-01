"""Discord Guild Scheduled Events를 가져와 뉴스레터용 교내 이벤트 목록을 반환한다."""

import logging
from datetime import date, timedelta
from typing import List

import requests

from src.common.config.discord import DiscordConfig

logger = logging.getLogger(__name__)

_API_BASE = "https://discord.com/api/v10"


def fetch_upcoming_events(week: str) -> List[dict]:
    """week(예: '2026-03-04') 기준 2주 뒤까지의 Discord 이벤트를 반환한다.

    Returns:
        [{"name": str, "description": str, "location": str,
          "start": str(ISO), "end": str(ISO)}, ...]
    """
    year, month, week_num = (int(x) for x in week.split("-"))
    week_start = date(year, month, (week_num - 1) * 7 + 1)
    cutoff = week_start + timedelta(weeks=2)

    raw_events = _call_api()
    if not raw_events:
        return []

    results = []
    for ev in raw_events:
        start_str = ev.get("scheduled_start_time", "")
        if not start_str:
            continue

        event_date = date.fromisoformat(start_str[:10])
        if event_date > cutoff:
            continue

        results.append(
            {
                "name": ev.get("name", ""),
                "description": ev.get("description", ""),
                "location": (ev.get("entity_metadata") or {}).get("location", ""),
                "start": start_str,
                "end": ev.get("scheduled_end_time", ""),
            }
        )

    logger.info(f"[discord_events] {len(results)}건 (cutoff={cutoff})")
    return results


def _call_api() -> list:
    guild_id = DiscordConfig.GUILD_ID
    token = DiscordConfig.BOT_API_KEY
    if not guild_id or not token:
        logger.warning(
            "[discord_events] DISCORD_GUILD_ID 또는 DISCORD_BOT_API_KEY 미설정"
        )
        return []

    url = f"{_API_BASE}/guilds/{guild_id}/scheduled-events"
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()
