from dataclasses import dataclass
from difflib import SequenceMatcher
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
        part="snippet",
        id=",".join(video_ids)
    ).execute()

    by_id = {item["id"]: item for item in videos_response.get("items", [])}

    candidates: list[VideoCandidate] = []
    for vid in video_ids:
        item = by_id.get(vid)
        if not item:
            continue

        snippet = item.get("snippet", {})
        candidates.append(
            VideoCandidate(
                title=snippet.get("title", ""),
                url=f"https://www.youtube.com/watch?v={vid}",
                tags=snippet.get("tags", []) or [],
                author=snippet.get("channelTitle", ""),
                description=snippet.get("description", ""),
                language=snippet.get("defaultAudioLanguage") or snippet.get("defaultLanguage"),
            )
        )

    return candidates


def score_video(query_phrases: list[str], profile: dict[str, Any], video: VideoCandidate) -> float:
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

    score = 0.0
    matched_phrases = 0

    for phrase in query_phrases:
        phrase_n = normalize(phrase)
        if not phrase_n:
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

        if phrase_score > 0:
            matched_phrases += 1

        score += phrase_score

    if query_phrases and matched_phrases == len(query_phrases):
        score += 20.0

    for interest in interests:
        i = normalize(interest)
        if not i:
            continue

        weight = float(interest_weights.get(interest, 1.0))
        if i in combined:
            score += 2.5 * weight
        else:
            score += max(
                similarity(i, title_n),
                similarity(i, desc_n),
                max((similarity(i, tag) for tag in tags_n), default=0.0)
            ) * 2.0 * weight

    tag_bonus = 0.0
    for tag in tags_n:
        tag_bonus += float(tag_weights.get(tag, 0.0))

    tag_bonus = max(-10.0, min(10.0, tag_bonus))
    score += tag_bonus

    pref_lang = profile.get("language")
    if pref_lang and video.language:
        if video.language.lower().startswith(pref_lang.lower()):
            score += 5.0

    return score


def pick_best_video(query: str, profile: dict[str, Any], candidates: list[VideoCandidate], recent_tags: list[list[str]] = None) -> dict[str, Any] | None:
    query_phrases = split_query(query)
    if not query_phrases:
        return None

    scored: list[tuple[float, VideoCandidate]] = []
    for video in candidates:
        s = score_video(query_phrases, profile, video)

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