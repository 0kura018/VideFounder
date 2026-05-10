from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import math
import re
from typing import Any

from googleapiclient.discovery import build


@dataclass
class VideoCandidate:
    title: str
    url: str
    tags: list[str]
    author: str
    description: str
    language: str = None
    view_count: int = 0
    published_at: str = ""
    source: str = "youtube"


def normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def split_query(query: str) -> list[str]:
    phrases = [normalize(part) for part in query.split(",")]
    return [p for p in phrases if p]


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def phrase_words(phrase: str) -> list[str]:
    return [w for w in normalize(phrase).split() if w]

def fetch_youtube_candidates(api_key: str, query: str, max_results: int = 10, relevance_language: str = None) -> list[VideoCandidate]:
    youtube = build("youtube", "v3", developerKey=api_key)

    search_params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results
    }
    if relevance_language:
        search_params["relevanceLanguage"] = relevance_language

    search_response = youtube.search().list(**search_params).execute()

    video_ids: list[str] = []
    for item in search_response.get("items", []):
        vid = item.get("id", {}).get("videoId")
        if vid:
            video_ids.append(vid)

    if not video_ids:
        return []

    videos_response = youtube.videos().list(
        part="snippet,statistics",
        id=",".join(video_ids)
    ).execute()

    by_id = {item["id"]: item for item in videos_response.get("items", [])}

    candidates: list[VideoCandidate] = []
    for vid in video_ids:
        item = by_id.get(vid)
        if not item:
            continue

        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        candidates.append(
            VideoCandidate(
                title=snippet.get("title", ""),
                url=f"https://www.youtube.com/watch?v={vid}",
                tags=snippet.get("tags", []) or [],
                author=snippet.get("channelTitle", ""),
                description=snippet.get("description", ""),
                language=snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage"),
                view_count=int(statistics.get("viewCount", 0)),
                published_at=snippet.get("publishedAt", ""),
            )
        )

    return candidates


def score_video(query_phrases: list[str], profile: dict[str, Any], video: VideoCandidate, channel_filters: set[str] = None) -> float:
    title_n = normalize(video.title)
    desc_n = normalize(video.description)
    tags_n = [normalize(tag) for tag in video.tags]

    combined = " ".join([title_n, desc_n, " ".join(tags_n)])
    title_words = set(title_n.split())
    tag_words = set(" ".join(tags_n).split())
    combined_words = set(combined.split())

    interests = profile.get("interests", [])
    interest_weights = profile.get("interest_weights", {})
    query_counts = profile.get("query_counts", {})
    tag_weights = profile.get("tag_weights", {})

    author_n = normalize(video.author)
    if channel_filters is None:
        channel_filters = set()

    score = 0.0
    matched_phrases = 0

    for idx, phrase in enumerate(query_phrases):
        phrase_n = normalize(phrase)
        if not phrase_n:
            continue

        is_secondary = idx > 0
        weight_mult = 0.5 if is_secondary else 1.0

        if phrase_n in channel_filters:
            continue

        words = phrase_words(phrase_n)
        phrase_score = 0.0

        exact_in_title = phrase_n in title_n
        exact_in_desc = phrase_n in desc_n
        exact_in_tags = any(phrase_n in tag for tag in tags_n)

        if exact_in_title:
            phrase_score += 8.0
        if exact_in_desc:
            phrase_score += 5.0
        if exact_in_tags:
            phrase_score += 7.0

        word_overlap_title = len(set(words) & title_words)
        word_overlap_tags = len(set(words) & tag_words)

        phrase_score += word_overlap_title * 2.0
        phrase_score += word_overlap_tags * 2.5

        fuzzy_best = max(
            similarity(phrase_n, title_n),
            similarity(phrase_n, desc_n),
            max((similarity(phrase_n, tag) for tag in tags_n), default=0.0)
        )
        if fuzzy_best > 0.72:
            phrase_score += fuzzy_best * 3.0

        if phrase_n in query_counts:
            phrase_score += min(query_counts[phrase_n] * 0.15, 0.6)

        phrase_score *= weight_mult

        if phrase_score > 0:
            matched_phrases += 1

        score += phrase_score

    if query_phrases and matched_phrases == len(query_phrases):
        score += 20.0

    query_title_sim = max(
        (similarity(normalize(phrase), title_n) for phrase in query_phrases),
        default=0.0
    )
    interest_factor = min(0.75, max(0.0, (0.45 - query_title_sim) / 0.45)) if query_title_sim < 0.45 else 0.0

    for interest in interests:
        i = normalize(interest)
        if not i:
            continue

        weight = float(interest_weights.get(interest, 1.0))
        if i in combined:
            score += 2.5 * weight * interest_factor
        else:
            score += max(
                similarity(i, title_n),
                similarity(i, desc_n),
                max((similarity(i, tag) for tag in tags_n), default=0.0)
            ) * 2.0 * weight * interest_factor

    tag_bonus = 0.0
    for tag in tags_n:
        tag_bonus += float(tag_weights.get(tag, 0.0))

    tag_bonus = max(-10.0, min(10.0, tag_bonus))
    score += tag_bonus * interest_factor

    pref_lang = profile.get("language")
    if pref_lang and video.language:
        if video.language.lower().startswith(pref_lang.lower()):
            score += 5.0

    if video.view_count > 0:
        score += min(math.log10(video.view_count), 7.0) * 0.4

    if video.published_at:
        try:
            pub = datetime.fromisoformat(video.published_at.replace("Z", "+00:00"))
            days_ago = (datetime.now(timezone.utc) - pub).days
            score += max(0.0, 3.0 - days_ago / 365.0)
        except (ValueError, TypeError):
            pass

    if channel_filters:
        if similarity(author_n, max(channel_filters, key=lambda cf: similarity(author_n, cf))) >= 0.8:
            score += 15.0
        else:
            score -= 1000.0

    return score


def _video_similarity(a: VideoCandidate, b: VideoCandidate) -> float:
    tags_a = {normalize(t) for t in a.tags}
    tags_b = {normalize(t) for t in b.tags}
    union = tags_a | tags_b
    jaccard = len(tags_a & tags_b) / len(union) if union else 0.0
    title_sim = similarity(normalize(a.title), normalize(b.title))
    author_sim = 1.0 if normalize(a.author) == normalize(b.author) else 0.0
    return 0.5 * jaccard + 0.3 * title_sim + 0.2 * author_sim


def _mmr_rerank(
    scored: list[tuple[float, VideoCandidate]],
    lambda_mmr: float = 0.7,
) -> list[tuple[float, VideoCandidate]]:
    if not scored:
        return []

    raw = [s for s, _ in scored]
    min_s, max_s = min(raw), max(raw)
    score_range = max_s - min_s if max_s != min_s else 1.0

    candidates = list(scored)
    selected: list[tuple[float, VideoCandidate]] = []

    while candidates:
        best_mmr = -float("inf")
        best_idx = 0
        for i, (s, video) in enumerate(candidates):
            norm_score = (s - min_s) / score_range
            if not selected:
                mmr_score = norm_score
            else:
                max_sim = max(_video_similarity(video, sel_v) for _, sel_v in selected)
                mmr_score = lambda_mmr * norm_score - (1.0 - lambda_mmr) * max_sim
            if mmr_score > best_mmr:
                best_mmr = mmr_score
                best_idx = i
        selected.append(candidates.pop(best_idx))

    return selected


def _detect_channel_filters(query_phrases: list[str], candidates: list[VideoCandidate]) -> set[str]:
    channel_filters: set[str] = set()
    all_authors = {normalize(c.author) for c in candidates}
    for idx, phrase in enumerate(query_phrases):
        if idx == 0:
            continue
        phrase_n = normalize(phrase)
        if not phrase_n:
            continue
        for author in all_authors:
            if similarity(phrase_n, author) >= 0.8:
                channel_filters.add(phrase_n)
                break
    return channel_filters


def pick_best_video(query: str, profile: dict[str, Any], candidates: list[VideoCandidate], recent_tags: list[list[str]] = None) -> dict[str, Any] | None:
    query_phrases = split_query(query)
    if not query_phrases:
        return None

    channel_filters = _detect_channel_filters(query_phrases, candidates)

    scored: list[tuple[float, VideoCandidate]] = []
    for video in candidates:
        s = score_video(query_phrases, profile, video, channel_filters=channel_filters)

        if recent_tags:
            video_tags_n = {normalize(t) for t in video.tags}
            penalty = 0.0
            for prev_tags in recent_tags:
                prev_tags_n = {normalize(t) for t in prev_tags}
                overlap = len(video_tags_n & prev_tags_n)
                if overlap > 0:
                    penalty += overlap * 1.5

            s -= min(penalty, 15.0)

        scored.append((s, video))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_video = scored[0]

    return {
        "title": best_video.title,
        "url": best_video.url,
        "author": best_video.author,
        "tags": best_video.tags,
        "score": best_score,
        "source": best_video.source,
        "description": best_video.description,
    }


def pick_all_videos(query: str, profile: dict[str, Any], candidates: list[VideoCandidate], recent_tags: list[list[str]] = None) -> list[dict[str, Any]]:
    query_phrases = split_query(query)
    if not query_phrases:
        return []

    channel_filters = _detect_channel_filters(query_phrases, candidates)

    scored: list[tuple[float, VideoCandidate]] = []
    for video in candidates:
        s = score_video(query_phrases, profile, video, channel_filters=channel_filters)

        if recent_tags:
            video_tags_n = {normalize(t) for t in video.tags}
            penalty = 0.0
            for prev_tags in recent_tags:
                prev_tags_n = {normalize(t) for t in prev_tags}
                overlap = len(video_tags_n & prev_tags_n)
                if overlap > 0:
                    penalty += overlap * 1.5
            s -= min(penalty, 15.0)

        scored.append((s, video))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    scored = _mmr_rerank(scored)

    return [
        {
            "title": v.title,
            "url": v.url,
            "author": v.author,
            "tags": v.tags,
            "score": s,
            "source": v.source,
            "description": v.description,
        }
        for s, v in scored
    ]
