import asyncio
import random
import json
import os
import uuid
from typing import Any

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import TOKEN, ADMIN_ID, YOUTUBE_API_KEY
from youtube_api import fetch_youtube_candidates, pick_best_video, pick_all_videos, split_query, normalize

bot = Bot(token=TOKEN)
dp = Dispatcher()

USERS_FILE = "users.json"
PROFILES_FILE = "profiles.json"

DEFAULT_INTERESTS = ["teto", "vocaloid", "music"]
QUERY_TO_INTEREST_THRESHOLD = 3

users: set[int] = set()
profiles: dict[str, dict[str, Any]] = {}
result_cache: dict[str, dict[str, Any]] = {}
search_sessions: dict[str, dict[str, Any]] = {}
waiting_for_search: set[int] = set()
user_history: dict[int, set[str]] = {}
user_tag_history: dict[int, list[list[str]]] = {}

def load_json(path: str, default: Any):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state():
    global users, profiles
    users = set(load_json(USERS_FILE, []))
    profiles = load_json(PROFILES_FILE, {})


def save_users():
    save_json(USERS_FILE, list(users))


def save_profiles():
    save_json(PROFILES_FILE, profiles)


def ensure_profile(user_id: int):
    uid = str(user_id)
    if uid not in profiles:
        profiles[uid] = {
            "interests": list(DEFAULT_INTERESTS),
            "interest_weights": {i: 1.0 for i in DEFAULT_INTERESTS},
            "query_counts": {},
            "tag_weights": {},
            "language": None,
        }
        save_profiles()
    else:
        profiles[uid].setdefault("interests", list(DEFAULT_INTERESTS))
        profiles[uid].setdefault("interest_weights", {i: 1.0 for i in DEFAULT_INTERESTS})
        profiles[uid].setdefault("query_counts", {})
        profiles[uid].setdefault("tag_weights", {})
        profiles[uid].setdefault("language", None)


def register_user(user_id: int):
    ensure_profile(user_id)
    users.add(user_id)
    save_users()


def get_profile(user_id: int) -> dict[str, Any]:
    ensure_profile(user_id)
    return profiles[str(user_id)]


def get_user_interests(user_id: int) -> list[str]:
    return list(get_profile(user_id).get("interests", []))


def add_interests(user_id: int, new_terms: list[str], weight: float = 1.0, remove: int = 0):
    profile = get_profile(user_id)
    interests = profile["interests"]
    interest_weights = profile["interest_weights"]

    if remove:
        interests.clear()
        interest_weights.clear()

    for term in new_terms:
        term = normalize(term)
        if not term:
            continue
        if term not in interests:
            interests.append(term)
        interest_weights[term] = max(float(interest_weights.get(term, 0.0)), weight)

    save_profiles()


def remove_interests(user_id: int, new_terms: list[str], weight: float = 1.0):
    profile = get_profile(user_id)
    interests = profile["interests"]
    interest_weights = profile["interest_weights"]
    removed = ""
    for term in new_terms:
        term = normalize(term)
        if not term:
            continue
        if term in interests:
            removed += f"{term} "
            interests.remove(term)
            del interest_weights[term]

    save_profiles()
    return removed


def add_query_terms(user_id: int, terms: list[str]):
    """
    Each comma-separated search term is counted.
    If it repeats enough times, it is promoted into interests with a smaller weight.
    """
    profile = get_profile(user_id)
    query_counts = profile["query_counts"]

    newly_promoted = []

    for term in terms:
        term = normalize(term)
        if not term:
            continue

        query_counts[term] = int(query_counts.get(term, 0)) + 1

        if query_counts[term] >= QUERY_TO_INTEREST_THRESHOLD:
            if term not in profile["interests"]:
                newly_promoted.append(term)

            profile["interest_weights"][term] = max(float(profile["interest_weights"].get(term, 0.0)), 0.6)

    if newly_promoted:
        for term in newly_promoted:
            profile["interests"].append(term)

    save_profiles()


def clear_query_terms(user_id: int):
    """
    Clears all query counts for the given user.
    """
    profile = get_profile(user_id)
    profile["query_counts"] = {}
    save_profiles()


def apply_feedback(user_id: int, tags: list[str], liked: bool):
    profile = get_profile(user_id)
    tag_weights = profile.get("tag_weights", {})
    interest_weights = profile.get("interest_weights", {})

    delta = 0.4 if liked else -0.4

    for tag in tags:
        t = normalize(tag)
        if not t:
            continue

        current_tag_w = float(tag_weights.get(t, 0.0))
        tag_weights[t] = round(max(-3.0, min(3.0, current_tag_w + delta)), 2)

        if t in interest_weights:
            current_int_w = float(interest_weights.get(t, 1.0))
            interest_weights[t] = round(max(0.1, min(4.0, current_int_w + delta)), 2)

    profile["tag_weights"] = tag_weights
    profile["interest_weights"] = interest_weights
    save_profiles()


def build_feedback_keyboard(result_id: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="👍 Like", callback_data=f"rate:{result_id}:like")
    kb.button(text="👎 Dislike", callback_data=f"rate:{result_id}:dislike")
    kb.adjust(2)
    return kb.as_markup()


def build_search_keyboard(result_id: str, session_id: str, index: int, total: int):
    kb = InlineKeyboardBuilder()
    prev_index = (index - 1) % total
    next_index = (index + 1) % total
    kb.button(text="⬅️ Back", callback_data=f"nav:{session_id}:{prev_index}")
    kb.button(text=f"{index + 1}/{total}", callback_data="noop")
    kb.button(text="➡️ Next", callback_data=f"nav:{session_id}:{next_index}")
    kb.adjust(3)
    kb.row()
    kb.button(text="👍 Like", callback_data=f"rate:{result_id}:like")
    kb.button(text="👎 Dislike", callback_data=f"rate:{result_id}:dislike")
    kb.adjust(3, 2)
    return kb.as_markup()


def store_result(owner_id: int, result: dict[str, Any], source_query: str):
    result_id = uuid.uuid4().hex[:12]
    result_cache[result_id] = {
        "owner_id": owner_id,
        "title": result.get("title", ""),
        "url": result.get("url", ""),
        "tags": result.get("tags", []),
        "author": result.get("author", ""),
        "source": result.get("source", ""),
        "query": source_query,
    }
    return result_id


def format_one_result(result: dict[str, Any], source_query: str) -> str:
    tags = result.get("tags") or []
    tags_text = ", ".join(tags) if tags else "none"

    return (
        f"<i>{source_query}</i>\n\n"
        f"<b>Title:</b> {result.get('title', 'Untitled')}\n\n"
        f"<b>Author:</b> {result.get('author', 'unknown')}\n\n"
        f"<b>Tags:</b> {tags_text}\n\n"
        f"<b>Source:</b> {result.get('source', 'youtube')}\n\n"
        #f"<b>Link:</b> {result.get('url', '')}"
    )


async def send_best_video(message: types.Message, query: str, mode: str = "search"):
    user_id = message.from_user.id
    profile = get_profile(user_id)

    if user_id not in user_history:
        user_history[user_id] = set()

    seen_urls = user_history[user_id]
    recent_tags = user_tag_history.get(user_id, [])
    lang = profile.get("language")

    candidates = await asyncio.to_thread(fetch_youtube_candidates, YOUTUBE_API_KEY, query, 10, lang)

    if not candidates and "," in query:
        shorter_query = ",".join(query.split(",")[:-1]).strip()
        if shorter_query:
            candidates = await asyncio.to_thread(fetch_youtube_candidates, YOUTUBE_API_KEY, shorter_query, 10, lang)

    if not candidates:
        if message.chat.type == "private":
            await message.answer("Nothing found.")
        else:
            await message.reply("Nothing found.")
        return

    filtered = [c for c in candidates if c.url not in seen_urls]
    if not filtered:
        seen_urls.clear()
        filtered = candidates

    all_results = pick_all_videos(query, profile, filtered, recent_tags=recent_tags)
    if not all_results:
        if message.chat.type == "private":
            await message.answer("Nothing found.")
        else:
            await message.reply("Nothing found.")
        return

    best = all_results[0]

    seen_urls.add(best["url"])

    if user_id not in user_tag_history:
        user_tag_history[user_id] = []
    user_tag_history[user_id].append(best.get("tags", []))
    if len(user_tag_history[user_id]) > 10:
        user_tag_history[user_id].pop(0)

    result_id = store_result(user_id, best, query)
    text = format_one_result(best, query)

    session_id = uuid.uuid4().hex[:12]
    search_sessions[session_id] = {
        "owner_id": user_id,
        "query": query,
        "results": all_results,
        "index": 0,
        "result_ids": {0: result_id},
    }

    use_nav = len(all_results) > 1
    if use_nav:
        keyboard = build_search_keyboard(result_id, session_id, 0, len(all_results))
    else:
        keyboard = build_feedback_keyboard(result_id)

    full_text = text + f"\n{best.get('url', '')}"

    if message.chat.type == "private":
        await message.answer(full_text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await message.reply(full_text, parse_mode="HTML", reply_markup=keyboard)

def build_recommendation_query(profile: dict[str, Any]) -> str:
    interests = profile.get("interests", [])
    interest_weights = profile.get("interest_weights", {})
    query_counts = profile.get("query_counts", {})
    tag_weights = profile.get("tag_weights", {})

    weighted_terms: list[tuple[float, str]] = []

    for term in interests:
        w = float(interest_weights.get(term, 1.0))
        weighted_terms.append((w, term))

    for term, count in query_counts.items():
        if count >= QUERY_TO_INTEREST_THRESHOLD:
            weighted_terms.append((0.6 + min(count * 0.05, 0.3), term))

    for tag, weight in tag_weights.items():
        if weight > 0.4:
            weighted_terms.append((weight * 0.7, tag))

    unique_map: dict[str, float] = {}
    for w, t in weighted_terms:
        t_n = normalize(t)
        if t_n:
            unique_map[t_n] = max(unique_map.get(t_n, 0.0), w)


    all_terms = sorted(unique_map.items(), key=lambda x: x[1], reverse=True)
    
    if not all_terms:
        return ""


    filtered_terms = [t for t in all_terms if t[1] >= 0.3]
    if not filtered_terms:
        filtered_terms = all_terms[:3]

    anchors = filtered_terms[:3]
    anchor = random.choice(anchors)[0]
    

    others = [t[0] for t in filtered_terms if t[0] != anchor]
    

    sample_size = min(len(others), random.randint(1, 3))
    
    selected = [anchor]
    if others:
        weights = [next(w for term, w in filtered_terms if term == t) for t in others]

        while len(selected) < sample_size + 1 and others:
            pick = random.choices(others, weights=weights, k=1)[0]
            if pick not in selected:
                selected.append(pick)
            else:
                if len(set(others)) == len(set(selected) - {anchor}):
                    break

    return ", ".join(selected)


@dp.message(Command("start"))
async def start(message: types.Message):
    register_user(message.from_user.id)
    await message.answer(
        "Bot is ready.\n"
        "Commands:\n"
        "/search <keywords>\n"
        "/recommend\n"
        "/set_interests <tags>\n"
        "/profile\n"
        "/broadcast <text> (ADMIN ONLY)\n"
        "/my_interests\n"
        "/remove_interests <tags>\n"
        "/add_interests <tags>\n"
        "/language\n"
    )


@dp.message(Command("set_interests"))
async def set_interests(message: types.Message):
    register_user(message.from_user.id)

    raw = message.text.partition(" ")[2].strip()
    if not raw:
        if message.chat.type == "private":
            await message.answer("Use: /set_interests -----, ---- ---- ----, --- ----")
        else:
            await message.reply("Use: /set_interests -----, ---- ---- ----, --- ----")
        return

    interests = [normalize(x) for x in raw.split(",") if normalize(x)]
    add_interests(message.from_user.id, interests, weight=1.0, remove=1)

    if message.chat.type == "private":
        await message.reply("Saved interests: " + ", ".join(interests))
    else:
        await message.answer("Saved interests: " + ", ".join(interests))


@dp.message(Command("remove_interests"))
async def set_interests(message: types.Message):
    register_user(message.from_user.id)

    raw = message.text.partition(" ")[2].strip()
    if not raw:
        if message.chat.type == "private":
            await message.answer("Use: /remove_interests -----, ---- ---- ----, --- ----")
        else:
            await message.reply("Use: /remove_interests -----, ---- ---- ----, --- ----")
        return

    interests = [normalize(x) for x in raw.split(",") if normalize(x)]

    removed = remove_interests(message.from_user.id, interests, weight=1.0)
    if removed == "":
        if message.chat.type == "private":
            await message.reply("No interests found.")
        else:
            await message.answer("No interests found.")
        return
    if message.chat.type == "private":
        await message.answer("Removed interests: " + removed)
    else:
        await message.reply("Removed interests: " + removed)


@dp.message(Command("add_interests"))
async def set_interests(message: types.Message):
    register_user(message.from_user.id)

    raw = message.text.partition(" ")[2].strip()
    if not raw:
        if message.chat.type == "private":
            await message.answer("Use: /add_interests -----, ---- ---- ----, --- ----")
        else:
            await message.reply("Use: /add_interests -----, ---- ---- ----, --- ----")
        return

    interests = [normalize(x) for x in raw.split(",") if normalize(x)]
    add_interests(message.from_user.id, interests, weight=1.0)

    if message.chat.type == "private":
        await message.answer("Saved interests: " + ", ".join(interests))
    else:
        await message.reply("Saved interests: " + ", ".join(interests))


@dp.message(Command("my_interests"))
async def set_interests(message: types.Message):
    register_user(message.from_user.id)

    profile = get_profile(message.from_user.id)
    interests = profile["interests"]
    interest_weights = profile["interest_weights"]

    for term, weight in interest_weights.items():
        if weight > 0.0:
            if message.chat.type == "private":
                await message.answer(f"{term} ({weight:.2f})")
            else:
                await message.reply(f"{term} ({weight:.2f})")


@dp.message(Command("profile"))
async def profile_cmd(message: types.Message):
    register_user(message.from_user.id)
    profile = get_profile(message.from_user.id)

    text = ("<b>Your profile:</b>\n"
        f"<b>Interests:</b> {', '.join(profile.get('interests', []))}\n\n"
        f"<b>Query history:</b> {', '.join([f'{k}({v})' for k, v in profile.get('query_counts', {}).items()]) or 'none'}")
    if message.chat.type == "private":
        await message.answer(text, parse_mode="HTML")
    else:
        await message.reply(text, parse_mode="HTML")


@dp.message(Command("search"))
async def search_cmd(message: types.Message):
    register_user(message.from_user.id)

    query = message.text.partition(" ")[2].strip()
    if not query:
        waiting_for_search.add(message.from_user.id)
        if message.chat.type == "private":
            await message.answer("Send me keywords or a title to search for.")
        else:
            await message.reply("Send me keywords or a title to search for.")
        return

    query_terms = split_query(query)
    add_query_terms(message.from_user.id, query_terms)


    await send_best_video(message, query, mode="search")

@dp.message(Command("clear_history"))
async def clear_history_cmd(message: types.Message):
    register_user(message.from_user.id)
    clear_query_terms(message.from_user.id)
    if message.chat.type == "private":
        await message.answer("Query history cleared.")
    else:
        await message.reply("Query history cleared.")


@dp.message(Command("recommend"))
async def recommend_cmd(message: types.Message):
    register_user(message.from_user.id)
    profile = get_profile(message.from_user.id)

    if not profile.get("interests"):
        if message.chat.type == "private":
            await message.answer("No interests saved yet. Use /set_interests first.")
        else:
            await message.reply("No interests saved yet. Use /set_interests first.")
        return

    recommend_query = build_recommendation_query(profile)
    if not recommend_query:
        if message.chat.type == "private":
            await message.answer("No recommendation seeds available yet.")
        else:
            await message.reply("No recommendation seeds available yet.")
        return

    await send_best_video(message, recommend_query, mode="recommend")


@dp.message(Command("language"))
async def language_cmd(message: types.Message):
    register_user(message.from_user.id)
    builder = InlineKeyboardBuilder()
    builder.button(text="Yes", callback_data="lang_pref:yes")
    builder.button(text="No", callback_data="lang_pref:no")

    await message.answer(
        "Do you want to use language preferences?",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(F.data.startswith("lang_pref:"))
async def lang_pref_callback(callback: types.CallbackQuery):
    choice = callback.data.split(":")[1]

    if choice == "no":
        profile = get_profile(callback.from_user.id)
        profile["language"] = None
        save_profiles()
        await callback.message.edit_text("Language preferences disabled.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    builder.button(text="Ru", callback_data="lang_set:ru")
    builder.button(text="En", callback_data="lang_set:en")
    builder.button(text="Auto", callback_data="lang_set:auto")
    builder.adjust(3)

    await callback.message.edit_text(
        "Choose language:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("lang_set:"))
async def lang_set_callback(callback: types.CallbackQuery):
    lang_code = callback.data.split(":")[1]
    profile = get_profile(callback.from_user.id)

    if lang_code == "auto":
        profile["language"] = None
    else:
        profile["language"] = lang_code

    save_profiles()

    await callback.message.edit_text(f"Language preference set to: {lang_code.capitalize()}")
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def noop_callback(callback: types.CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data.startswith("nav:"))
async def nav_callback(callback: types.CallbackQuery):
    try:
        _, session_id, idx_str = callback.data.split(":")
        new_index = int(idx_str)
    except ValueError:
        await callback.answer("Invalid", show_alert=False)
        return

    session = search_sessions.get(session_id)
    if not session:
        await callback.answer("Session expired", show_alert=False)
        return

    is_owner = callback.from_user.id == session["owner_id"]
    is_group = callback.message.chat.type in ("group", "supergroup")
    is_chat_admin = False
    if is_group and not is_owner:
        member = await callback.message.chat.get_member(callback.from_user.id)
        is_chat_admin = member.status in ("creator", "administrator")
    if not is_owner and not is_chat_admin:
        await callback.answer("Not yours", show_alert=False)
        return

    results = session["results"]
    if new_index < 0 or new_index >= len(results):
        await callback.answer("No more results", show_alert=False)
        return

    session["index"] = new_index
    result = results[new_index]

    if new_index not in session["result_ids"]:
        result_id = store_result(session["owner_id"], result, session["query"])
        session["result_ids"][new_index] = result_id
    else:
        result_id = session["result_ids"][new_index]

    desc = format_one_result(result, session["query"])
    full_text = desc + f"\n{result.get('url', '')}"
    keyboard = build_search_keyboard(result_id, session_id, new_index, len(results))

    await callback.message.edit_text(full_text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("rate:"))
async def rate_callback(callback: types.CallbackQuery):
    try:
        _, result_id, vote = callback.data.split(":")
    except ValueError:
        await callback.answer("Invalid", show_alert=False)
        return

    item = result_cache.get(result_id)
    if not item:
        await callback.answer("Expired", show_alert=False)
        return

    if callback.from_user.id != item["owner_id"]:
        await callback.answer("Not yours", show_alert=False)
        return

    liked = vote == "like"

    apply_feedback(callback.from_user.id, item.get("tags", []), liked)

    try:
        new_text = callback.message.text

        if "Feedback:" not in new_text:
            new_text += f"\n\nFeedback: {'👍 Like' if liked else '👎 Dislike'}"

        await callback.message.edit_text(new_text)

    except Exception:
        pass

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.answer("Saved", show_alert=False)


@dp.message(Command("broadcast"))
async def broadcast_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        if message.chat.type == "private":
            await message.answer("No access.")
        else:
            await message.reply("No access.")
        return

    text = message.text.partition(" ")[2].strip()
    if not text:
        if message.chat.type == "private":
            await message.answer("Use: /broadcast <message>")
        else:
            await message.reply("Use: /broadcast <message>")
        return

    sent = 0
    failed = 0

    for user_id in users:
        try:
            await bot.send_message(user_id, text)
            sent += 1
        except Exception:
            failed += 1

    if message.chat.type == "private":
        await message.answer(f"Done. Sent: {sent}, Failed: {failed}")
    else:
        await message.reply(f"Done. Sent: {sent}, Failed: {failed}")


@dp.message(F.text)
async def free_text_search(message: types.Message):
    if message.from_user.id not in waiting_for_search:
        return

    if message.text.startswith("/"):
        return

    waiting_for_search.discard(message.from_user.id)

    query = message.text.strip()
    query_terms = split_query(query)
    add_query_terms(message.from_user.id, query_terms)

    await send_best_video(message, query, mode="search")


async def main():
    load_state()
    print("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
