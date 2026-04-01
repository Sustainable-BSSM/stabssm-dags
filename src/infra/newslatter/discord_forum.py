"""Discord 포럼 채널에서 '프로젝트' 태그가 달린 최근 2주 게시글을 수집하고,
리액션(50%) + 최신순(20%) + 댓글 수(30%) 가중 점수로 상위 3개를 선별한다.
"""

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

import requests

from src.common.config.discord import DiscordConfig

logger = logging.getLogger(__name__)

_API_BASE = "https://discord.com/api/v10"
_PROJECT_TAG_NAME = "프로젝트"
_TOP_N = 3

# 가중치
_W_REACTION = 0.50
_W_RECENCY = 0.20
_W_COMMENTS = 0.30


def fetch_recent_projects(week: str) -> List[dict]:
    """week 기준 최근 2주간 '프로젝트' 태그 게시글 중 상위 3개를 반환한다.

    점수 = 리액션 수(50%) + 최신순(20%) + 댓글 수(30%)

    Returns:
        [{"name": str, "created_at": str(ISO), "score": float,
          "reactions": int, "comments": int}, ...]
    """
    channel_id = DiscordConfig.FORUM_CHANNEL_ID
    token = DiscordConfig.BOT_API_KEY
    if not channel_id or not token:
        logger.warning(
            "[discord_forum] DISCORD_FORUM_CHANNEL_ID 또는 DISCORD_BOT_API_KEY 미설정"
        )
        return []

    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }

    # [1] 채널 조회 → 태그 목록 확보
    tag_id = _get_project_tag_id(channel_id, headers)
    if not tag_id:
        logger.warning(f"[discord_forum] '{_PROJECT_TAG_NAME}' 태그를 찾을 수 없음")
        return []

    # [2] + [3] active (guild 레벨) + archived threads 수집
    guild_id = DiscordConfig.GUILD_ID
    all_threads = _fetch_active_threads(guild_id, channel_id, headers)
    all_threads.extend(_fetch_archived_threads(channel_id, headers))

    # [4] + [5] + [6] 태그 필터링 + 날짜 필터링
    year, month, week_num = (int(x) for x in week.split("-"))
    week_start = date(year, month, (week_num - 1) * 7 + 1)
    cutoff = week_start - timedelta(weeks=2)

    candidates = []
    for thread in all_threads:
        if tag_id not in thread.get("applied_tags", []):
            continue

        meta = thread.get("thread_metadata", {})
        ts = meta.get("create_timestamp") or meta.get("archive_timestamp")
        if not ts:
            continue

        thread_date = _parse_date(ts)
        if not thread_date or thread_date < cutoff:
            continue

        # 댓글 수: thread 객체의 message_count
        comments = thread.get("message_count", 0)

        # 첫 번째 메시지에서 리액션 수 + 본문 가져오기
        thread_id = thread["id"]
        first_msg = _fetch_first_message(thread_id, headers)

        candidates.append(
            {
                "name": thread.get("name", ""),
                "description": first_msg["content"],
                "created_at": ts,
                "thread_date": thread_date,
                "reactions": first_msg["reactions"],
                "comments": comments,
            }
        )

    if not candidates:
        logger.info("[discord_forum] 프로젝트 0건")
        return []

    # 점수 계산 (min-max 정규화)
    ranked = _rank(candidates, week_start)
    top = ranked[:_TOP_N]
    logger.info(
        f"[discord_forum] 프로젝트 {len(candidates)}건 중 상위 {len(top)}건 선별"
    )
    return top


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _rank(candidates: list, week_start: date) -> list:
    """리액션(50%) + 최신순(20%) + 댓글(30%) 가중 점수로 정렬."""
    max_reactions = max((c["reactions"] for c in candidates), default=1) or 1
    max_comments = max((c["comments"] for c in candidates), default=1) or 1

    # 최신순: week_start 대비 며칠 전인지 (작을수록 최신)
    max_age = 14  # 2주

    for c in candidates:
        norm_reactions = c["reactions"] / max_reactions
        norm_comments = c["comments"] / max_comments

        age_days = (week_start - c["thread_date"]).days
        norm_recency = max(0, 1 - age_days / max_age)

        c["score"] = (
            _W_REACTION * norm_reactions
            + _W_RECENCY * norm_recency
            + _W_COMMENTS * norm_comments
        )

    candidates.sort(key=lambda c: c["score"], reverse=True)

    # thread_date는 직렬화 불가하므로 제거
    for c in candidates:
        del c["thread_date"]

    return candidates


# ---------------------------------------------------------------------------
# Discord API helpers
# ---------------------------------------------------------------------------


def _get_project_tag_id(channel_id: str, headers: dict) -> Optional[str]:
    resp = requests.get(
        f"{_API_BASE}/channels/{channel_id}", headers=headers, timeout=10
    )
    resp.raise_for_status()
    data = resp.json()
    for tag in data.get("available_tags", []):
        if tag.get("name") == _PROJECT_TAG_NAME:
            return tag["id"]
    return None


def _fetch_active_threads(guild_id: str, channel_id: str, headers: dict) -> list:
    """Guild 레벨 active threads에서 해당 채널의 스레드만 필터링한다."""
    resp = requests.get(
        f"{_API_BASE}/guilds/{guild_id}/threads/active",
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    threads = resp.json().get("threads", [])
    return [t for t in threads if t.get("parent_id") == channel_id]


def _fetch_archived_threads(channel_id: str, headers: dict) -> list:
    """before 기반 페이징으로 archived threads를 전부 가져온다."""
    all_threads = []
    before = None

    while True:
        params = {"limit": 100}
        if before:
            params["before"] = before

        resp = requests.get(
            f"{_API_BASE}/channels/{channel_id}/threads/archived/public",
            headers=headers,
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        threads = data.get("threads", [])
        if not threads:
            break

        all_threads.extend(threads)

        if not data.get("has_more", False):
            break

        last_meta = threads[-1].get("thread_metadata", {})
        before = last_meta.get("archive_timestamp")
        if not before:
            break

    return all_threads


def _fetch_first_message(thread_id: str, headers: dict) -> dict:
    """스레드의 첫 번째 메시지에서 리액션 수와 본문을 반환한다.

    Returns:
        {"reactions": int, "content": str}
    """
    try:
        resp = requests.get(
            f"{_API_BASE}/channels/{thread_id}/messages",
            headers=headers,
            params={"limit": 1, "after": "0"},
            timeout=10,
        )
        resp.raise_for_status()
        messages = resp.json()
        if not messages:
            return {"reactions": 0, "content": ""}

        first_msg = messages[0]
        reactions = sum(r.get("count", 0) for r in first_msg.get("reactions", []))
        content = first_msg.get("content", "")
        return {"reactions": reactions, "content": content}
    except Exception as e:
        logger.warning(f"[discord_forum] 첫 메시지 조회 실패 thread={thread_id}: {e}")
        return {"reactions": 0, "content": ""}


def _parse_date(ts: str) -> Optional[date]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None
