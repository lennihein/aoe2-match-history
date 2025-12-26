from __future__ import annotations

import datetime as dt
import json
import re
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Configuration (adjust here, no CLI args needed)
import os
USER_IDS = ["12649589"]
BASE_URL = "https://www.aoe2insights.com/user/{user_id}/matches/?page={page}"
DATA_DIR = Path(os.getenv("AOE2_DATA_DIR", "data"))
GAME_SPEED_FACTOR = 1.7  # AoE2 game time runs faster than real time
SESSION_IDLE_MINUTES = 20  # minimum idle time (after previous game's end) to start a new session
# Set to a list like ["RM 1v1"] to restrict session analytics to specific modes, or None for all
SESSION_MODE_FILTER = None
MAX_FETCH_PAGES = 2000
FETCH_TIMEOUT_SECONDS = 12

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
)

# --- Shared parsing helpers ---


def parse_int(text):
    if text is None:
        return None
    cleaned = text.replace(" ", "").replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return cleaned


def parse_datetime_value(value):
    if not value:
        return None
    formats = [
        "%b. %d, %Y, %I:%M %p",
        "%b %d, %Y, %I:%M %p",
        "%b. %d, %Y, %I %p",
        "%b %d, %Y, %I %p",
        "%B %d, %Y, %I:%M %p",
        "%B %d, %Y, %I %p",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    cleaned = str(value).replace("a.m.", "AM").replace("p.m.", "PM")
    cleaned = cleaned.replace(" ", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    try:
        return dt.datetime.fromisoformat(cleaned)
    except Exception:
        pass
    for fmt in formats:
        try:
            return dt.datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def format_dt(dt_obj):
    if not dt_obj:
        return None
    return dt_obj.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def duration_to_seconds(duration_str: str):
    if not duration_str:
        return None
    m = re.match(r"(?:(?P<h>\d+)h\s*)?(?P<m>\d+)m\s*(?P<s>\d+)s", duration_str.strip())
    if not m:
        return None
    hours = int(m.group("h") or 0)
    minutes = int(m.group("m") or 0)
    seconds = int(m.group("s") or 0)
    return hours * 3600 + minutes * 60 + seconds


def duration_to_real_seconds(duration_str: str, speed_factor: float = GAME_SPEED_FACTOR):
    game_seconds = duration_to_seconds(duration_str)
    if game_seconds is None:
        return None
    return game_seconds / speed_factor


# --- Fetching and caching ---


def cache_path_for(user_id: str):
    if not user_id.isdigit():
        raise ValueError(f"Invalid user_id: {user_id}. Must be numeric.")
    return DATA_DIR / f"matches_{user_id}.json"


def status_path_for(user_id: str):
    return DATA_DIR / f"status_{user_id}.json"


def load_status(user_id: str):
    path = status_path_for(user_id)
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass
    return {"is_complete": True}


def save_status(user_id: str, status: dict):
    path = status_path_for(user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)


def parse_match_tile(tile):
    match_id_node = tile.select_one(".text-muted small")
    game_id = match_id_node.get_text(strip=True).lstrip("#") if match_id_node else None
    duration_icon = tile.select_one(".mt-2 .fa-clock")
    duration = duration_icon.parent.get_text(" ", strip=True) if duration_icon else None
    date_span = tile.select_one(".mt-2 span[title]")
    dt_value = date_span["title"].strip() if date_span and date_span.has_attr("title") else None
    start_dt = parse_datetime_value(dt_value)
    real_duration = duration_to_real_seconds(duration)
    end_dt = start_dt + dt.timedelta(seconds=real_duration) if start_dt and real_duration is not None else None
    map_node = tile.select_one(".col-md-3 .d-flex.flex-column > div:nth-of-type(2)")
    map_name = map_node.get_text(strip=True) if map_node else None
    mode_node = tile.select_one(".col-md-3 a.stretched-link strong")
    mode = mode_node.get_text(" ", strip=True) if mode_node else None
    if mode:
        mode = re.sub(r"\s+", " ", mode).strip()
    teams = []
    for team in tile.select("ul.team"):
        team_info = {"won": "won" in team.get("class", []), "players": []}
        for li in team.select("li"):
            anchor = li.select_one("a[href^='/user/']")
            player_name = anchor.get_text(strip=True) if anchor else None
            player_href = anchor.get("href") if anchor else None
            player_id = None
            if player_href:
                parts = [p for p in player_href.split("/") if p]
                if len(parts) >= 2:
                    player_id = parts[1]
            civ_icon = li.select_one("i.image-icon")
            civ = civ_icon.get("title") if civ_icon else None
            rating_span = li.select_one(".rating span")
            rating = parse_int(rating_span.get_text()) if rating_span else None
            change_span = li.select_one(".rating-change")
            rating_change = parse_int(change_span.get_text(strip=True)) if change_span else None
            strat_em = li.select_one(".strategy em")
            strategy = strat_em.get_text(strip=True) if strat_em else None
            if not strategy:
                strat_node = li.select_one(".strategy")
                strategy = strat_node.get_text(" ", strip=True) if strat_node else None
            team_info["players"].append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "civ": civ,
                    "elo": rating,
                    "elo_change": rating_change,
                    "strategy": strategy,
                }
            )
        teams.append(team_info)
    start_iso = format_dt(start_dt)
    end_iso = format_dt(end_dt)
    return {
        "game_id": game_id,
        "mode": mode,
        "map": map_name,
        "duration": duration,
        "start_datetime": start_iso,
        "end_datetime": end_iso,
        "teams": teams,
    }


def normalize_match(match):
    if not isinstance(match, dict):
        return None
    game_id = match.get("game_id") or match.get("match_id")
    teams = match.get("teams") or []
    normalized_teams = []
    for team in teams:
        players = []
        for p in team.get("players", []):
            players.append(
                {
                    "player_id": p.get("player_id") or p.get("id"),
                    "player_name": p.get("player_name") or p.get("name"),
                    "civ": p.get("civ"),
                    "elo": p.get("elo"),
                    "elo_change": p.get("elo_change"),
                    "strategy": p.get("strategy"),
                }
            )
        normalized_teams.append({"won": bool(team.get("won")), "players": players})
    raw_dt = match.get("start_datetime") or match.get("datetime") or match.get("datetime_parsed") or match.get("date")
    dt_iso = None
    start_dt = None
    if raw_dt:
        parsed = parse_datetime_value(raw_dt)
        if parsed:
            start_dt = parsed
            dt_iso = format_dt(parsed)
    end_raw = match.get("end_datetime")
    end_dt = None
    end_iso = None
    if end_raw:
        parsed_end = parse_datetime_value(end_raw)
        if parsed_end:
            end_dt = parsed_end
            end_iso = format_dt(parsed_end)
    if start_dt is None and end_dt is not None:
        real_dur = duration_to_real_seconds(match.get("duration"))
        if real_dur is not None:
            start_dt = end_dt - dt.timedelta(seconds=real_dur)
            dt_iso = format_dt(start_dt)
    return {
        "game_id": game_id,
        "mode": match.get("mode"),
        "map": match.get("map"),
        "duration": match.get("duration"),
        "start_datetime": dt_iso,
        "end_datetime": end_iso,
        "teams": normalized_teams,
    }


def load_cached_matches(path: Path):
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            try:
                raw = json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: could not decode {path}, starting fresh.")
                return []
        normalized = [m for m in (normalize_match(item) for item in raw) if m and m.get("game_id")]
        return normalized
    return []


def match_sort_key(match):
    dt_raw = match.get("start_datetime") or match.get("datetime") or match.get("date")
    parsed = parse_datetime_value(dt_raw) if dt_raw else None
    if parsed:
        return parsed
    end_raw = match.get("end_datetime")
    parsed_end = parse_datetime_value(end_raw) if end_raw else None
    return parsed_end or dt.datetime.min


def save_matches(matches, path: Path):
    sorted_matches = sorted(matches, key=match_sort_key, reverse=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted_matches, f, ensure_ascii=False, indent=2)


def fetch_new_matches(user_id: str, known_ids=None, start_page: int = 1, max_pages: int = None, timeout_seconds: int = None, stop_at_known: bool = True):
    if max_pages is None:
        max_pages = MAX_FETCH_PAGES
    if timeout_seconds is None:
        timeout_seconds = FETCH_TIMEOUT_SECONDS
        
    known_ids = set(known_ids or [])
    new_matches = []
    reached_known = False
    reached_end = False
    is_complete = True
    timed_out = False
    
    import time
    start_time = time.time()
    
    last_page = start_page - 1
    for page in range(start_page, start_page + max_pages):
        # Check timeout
        if time.time() - start_time > timeout_seconds:
            print(f"Fetch timed out after {timeout_seconds} seconds. Returning partial results.")
            is_complete = False
            timed_out = True
            break
            
        url = BASE_URL.format(user_id=user_id, page=page)
        print(f"Fetching page {page} for user {user_id}...")
        try:
            resp = SESSION.get(url, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}. Stopping fetch.")
            is_complete = False
            break
            
        text_lower = resp.text.lower()
        if "#not found" in text_lower or resp.status_code == 404:
            print("Reached end of history (#not found or 404).")
            reached_end = True
            last_page = page
            break
        if resp.status_code >= 400:
            print(f"Hit an HTTP error {resp.status_code}; stopping fetch.")
            is_complete = False
            break
        soup = BeautifulSoup(resp.text, "lxml")
        tiles = soup.select("div.match-tile")
        if not tiles:
            print("No match tiles found on this page; stopping fetch.")
            reached_end = True
            last_page = page
            break
            
        page_has_new = False
        for tile in tiles:
            match = parse_match_tile(tile)
            game_id = match.get("game_id")
            if not game_id:
                continue
            if game_id in known_ids:
                reached_known = True
                if stop_at_known:
                    print(f"Encountered cached match {game_id}; stopping at previously stored data.")
                    break
                else:
                    continue # Skip and keep looking for older matches
            new_matches.append(match)
            page_has_new = True
            
        last_page = page
        if reached_known and stop_at_known:
            break
    
    # If we didn't timeout, didn't reach end, and didn't reach known, 
    # but the loop finished, it means we hit max_pages.
    if is_complete and not reached_end and (not reached_known or not stop_at_known):
        # Check if we actually processed all pages we intended
        # range(start_page, start_page + max_pages) has max_pages items.
        if last_page == start_page + max_pages - 1:
            is_complete = False # We hit the page limit, so it's partial.
        
    return new_matches, is_complete, reached_known, reached_end, last_page, timed_out


# --- Ranked RM 1v1 stats ---

DURATION_BUCKETS = [
    ("< 5m", 0, 5 * 60),
    ("5-15m", 5 * 60, 15 * 60),
    ("15-25m", 15 * 60, 25 * 60),
    ("25-40m", 25 * 60, 40 * 60),
    (">= 40m", 40 * 60, None),
]


def bucket_label(seconds: int):
    if seconds is None:
        return None
    for label, lower, upper in DURATION_BUCKETS:
        if upper is None and seconds >= lower:
            return label
        if upper is not None and lower <= seconds < upper:
            return label
    return None


def compute_ranked_stats(matches, user_id: str):
    total = 0
    wins = 0
    opponents = defaultdict(lambda: {"name": None, "matches": 0, "wins": 0})
    duration_stats = defaultdict(lambda: {"matches": 0, "wins": 0})
    civ_stats = defaultdict(lambda: {"matches": 0, "wins": 0})
    opp_civ_stats = defaultdict(lambda: {"matches": 0, "wins": 0})
    map_stats = defaultdict(lambda: {"matches": 0, "wins": 0})

    for match in matches:
        if match.get("mode") != "RM 1v1":
            continue
        teams = match.get("teams") or []
        user_team_idx = None
        user_player = None
        for idx, team in enumerate(teams):
            for p in team.get("players", []):
                if p.get("player_id") == user_id:
                    user_team_idx = idx
                    user_player = p
                    break
            if user_player:
                break
        if user_player is None:
            continue
        user_win = bool(teams[user_team_idx].get("won")) if user_team_idx is not None else False
        opp_players = [p for i, t in enumerate(teams) if i != user_team_idx for p in t.get("players", [])]
        if not opp_players:
            continue
        total += 1
        if user_win:
            wins += 1

        seconds = duration_to_seconds(match.get("duration"))
        label = bucket_label(seconds)
        if label:
            duration_stats[label]["matches"] += 1
            if user_win:
                duration_stats[label]["wins"] += 1

        user_civ = user_player.get("civ")
        if user_civ:
            civ_stats[user_civ]["matches"] += 1
            if user_win:
                civ_stats[user_civ]["wins"] += 1

        map_name = match.get("map")
        if map_name:
            map_stats[map_name]["matches"] += 1
            if user_win:
                map_stats[map_name]["wins"] += 1

        for op in opp_players:
            key = op.get("player_id") or f"name:{op.get('player_name')}"
            name = op.get("player_name") or key
            entry = opponents[key]
            entry["name"] = name
            entry["matches"] += 1
            if user_win:
                entry["wins"] += 1
            opp_civ = op.get("civ")
            if opp_civ:
                opp_civ_stats[opp_civ]["matches"] += 1
                if user_win:
                    opp_civ_stats[opp_civ]["wins"] += 1

    return {
        "total": total,
        "wins": wins,
        "opponents": opponents,
        "duration": duration_stats,
        "civs": civ_stats,
        "opp_civs": opp_civ_stats,
        "maps": map_stats,
    }


def win_rate(row):
    return (row["wins"] / row["matches"] * 100) if row["matches"] else 0.0


def sorted_items_list(d, key_fn=None):
    items = []
    for k, v in d.items():
        v = dict(v)
        v["key"] = k
        v["win_rate"] = win_rate(v)
        items.append(v)
    if key_fn:
        items.sort(key=key_fn)
    return items


def print_ranked_analytics(matches_by_user):
    for user_id, matches in matches_by_user.items():
        stats = compute_ranked_stats(matches, user_id)
        total = stats["total"]
        wins = stats["wins"]
        print(
            f"[{user_id}] RM 1v1 ranked (elo change) matches: {total}, "
            f"wins: {wins}, win rate: {(wins/total*100) if total else 0:.1f}%"
        )

        opp_rows = sorted_items_list(stats["opponents"], key_fn=lambda r: (-r["matches"], -r["wins"]))
        print("  Frequent opponents (top 5):")
        for row in opp_rows[:5]:
            print(f"    {row['name']}: {row['matches']} matches, {row['wins']} wins ({row['win_rate']:.1f}% win)")

        print("  Win rates by match duration:")
        duration_order = [b[0] for b in DURATION_BUCKETS]
        duration_rows = {row["key"]: row for row in sorted_items_list(stats["duration"])}
        for label in duration_order:
            row = duration_rows.get(label, {"matches": 0, "wins": 0, "win_rate": 0})
            print(f"    {label}: {row['win_rate']:.1f}% ({row.get('wins',0)} wins / {row.get('matches',0)} matches)")

        civ_rows = sorted_items_list(stats["civs"], key_fn=lambda r: (-r["matches"], -r["wins"]))
        print("  Win rates by your civilization (top 10):")
        for row in civ_rows[:10]:
            print(f"    {row['key']}: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")

        opp_civ_rows = sorted_items_list(stats["opp_civs"], key_fn=lambda r: (-r["matches"], -r["wins"]))
        print("  Win rates by opponent civilization (top 10):")
        for row in opp_civ_rows[:10]:
            print(f"    {row['key']}: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")
        print("-")


# --- Session analytics ---


def user_outcome(match, user_id):
    teams = match.get("teams") or []
    user_team_idx = None
    user_player = None
    for idx, t in enumerate(teams):
        for p in t.get("players", []):
            if p.get("player_id") == user_id:
                user_team_idx = idx
                user_player = p
                break
        if user_player:
            break
    if user_player is None:
        return None, None
    win = bool(teams[user_team_idx].get("won"))
    return win, user_player


def prepare_user_matches(matches, user_id, mode_filter=None):
    eligible = []
    parse_fail = 0
    filtered_out = 0
    for m in matches:
        if mode_filter and m.get("mode") not in mode_filter:
            filtered_out += 1
            continue
        ts = parse_datetime_value(m.get("start_datetime") or m.get("datetime") or m.get("date"))
        if ts is None:
            parse_fail += 1
            continue
        win, player = user_outcome(m, user_id)
        if win is None:
            filtered_out += 1
            continue
        end_ts = None
        end_raw = m.get("end_datetime")
        if end_raw:
            end_ts = parse_datetime_value(end_raw)
        if end_ts is None:
            dur_seconds = duration_to_seconds(m.get("duration"))
            end_ts = ts + dt.timedelta(seconds=dur_seconds) if dur_seconds is not None else ts
        eligible.append({"ts": ts, "end_ts": end_ts, "match": m, "win": win})
    return eligible, parse_fail, filtered_out


def _entry_label(entry):
    m = entry.get("match", {})
    gid = m.get("game_id") or m.get("match_id") or "unknown"
    dur = m.get("duration") or "unknown"
    return gid, dur


def group_sessions(prepared, idle_minutes=SESSION_IDLE_MINUTES):
    prepared = sorted(prepared, key=lambda x: x["ts"], reverse=False)
    sessions = []
    current = []
    last_end = None
    for entry in prepared:
        ts = entry["ts"]
        end_ts = entry.get("end_ts", ts)
        if last_end is None:
            current.append(entry)
        else:
            gap = (ts - last_end).total_seconds() / 60.0
            is_new = gap > idle_minutes
            if is_new:
                if current:
                    sessions.append(current)
                current = [entry]
            else:
                current.append(entry)
        last_end = end_ts
    if current:
        sessions.append(current)
    return sessions


def print_sessions(sessions):
    print("[session] chronological game log")
    for idx, sess in enumerate(sessions, 1):
        print(f"---- Session {idx} ---- (length: {len(sess)})")
        for entry in sess:
            gid, dur = _entry_label(entry)
            start = entry["ts"]
            end = entry["end_ts"]
            print(f"  {gid}: {start}-{end}")


def session_metrics(sessions):
    session_records = [[entry["win"] for entry in sess] for sess in sessions if sess]

    count_buckets = defaultdict(lambda: {"matches": 0, "wins": 0})
    after_prev = {True: {"matches": 0, "wins": 0}, False: {"matches": 0, "wins": 0}}
    after_streak = {
        (True, 2): {"matches": 0, "wins": 0},
        (False, 2): {"matches": 0, "wins": 0},
    }

    for results in session_records:
        n = len(results)
        count_buckets[n]["matches"] += n
        count_buckets[n]["wins"] += sum(results)
        for idx, win in enumerate(results):
            if idx >= 1:
                prev = results[idx - 1]
                after_prev[prev]["matches"] += 1
                after_prev[prev]["wins"] += 1 if win else 0
            if idx >= 2:
                if results[idx - 1] and results[idx - 2]:
                    after_streak[(True, 2)]["matches"] += 1
                    after_streak[(True, 2)]["wins"] += 1 if win else 0
                if (not results[idx - 1]) and (not results[idx - 2]):
                    after_streak[(False, 2)]["matches"] += 1
                    after_streak[(False, 2)]["wins"] += 1 if win else 0

    def rate(d):
        return {k: {**v, "win_rate": (v["wins"] / v["matches"] * 100) if v["matches"] else 0.0} for k, v in d.items()}

    return {
        "session_counts": rate(count_buckets),
        "after_prev": rate(after_prev),
        "after_streak": rate(after_streak),
    }


def nth_game_winrates(sessions):
    buckets = defaultdict(lambda: {"matches": 0, "wins": 0})
    for sess in sessions:
        results = [entry["win"] for entry in sess]
        for idx, win in enumerate(results, start=1):
            buckets[idx]["matches"] += 1
            buckets[idx]["wins"] += 1 if win else 0
    return {k: {**v, "win_rate": (v["wins"] / v["matches"] * 100) if v["matches"] else 0.0} for k, v in buckets.items()}


def print_session_analytics(matches_by_user, mode_filter=None):
    applied_filter = mode_filter if mode_filter is not None else SESSION_MODE_FILTER
    for user_id, matches in matches_by_user.items():
        prepared, parse_fail, filtered_out = prepare_user_matches(matches, user_id, mode_filter=applied_filter)
        cache_total = len(matches)
        print(f"[{user_id}] Total cached matches: {cache_total}")
        print(f"[{user_id}] Eligible matches: {len(prepared)} (filtered out: {filtered_out}, parse-fail: {parse_fail})")
        sessions = group_sessions(prepared)
        print(f"[{user_id}] Sessions with eligible games: {len(sessions)}")
        print("-")
        # print_sessions(sessions)

        metrics = session_metrics(sessions)
        nth_stats = nth_game_winrates(sessions)

        print(
            f"[{user_id}] (analytics) cached: {cache_total}, eligible: {len(prepared)}, sessions: {len(sessions)}"
        )
        print("Winrate by session match count:")
        for count in sorted(metrics["session_counts"].keys()):
            row = metrics["session_counts"][count]
            print(f"  {count} games: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")

        print("Winrate after previous result:")
        for prev in [True, False]:
            label = "after win" if prev else "after loss"
            row = metrics["after_prev"].get(prev, {"win_rate": 0, "wins": 0, "matches": 0})
            print(f"  {label}: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")

        print("Winrate after streak of 2:")
        for key in [(True, 2), (False, 2)]:
            label = "after 2 wins" if key[0] else "after 2 losses"
            row = metrics["after_streak"].get(key, {"win_rate": 0, "wins": 0, "matches": 0})
            print(f"  {label}: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")

        print("Winrate by nth game in session:")
        for n in sorted(nth_stats.keys()):
            row = nth_stats[n]
            print(f"  Game {n}: {row['win_rate']:.1f}% ({row['wins']} / {row['matches']})")
        print("-")


# --- Orchestration ---


def refresh_matches(user_id: str, max_pages: int = None):
    cache_path = cache_path_for(user_id)
    cached_matches = load_cached_matches(cache_path)
    current_status = load_status(user_id)
    
    if cache_path.exists():
        print(f"[{user_id}] Loaded {len(cached_matches)} cached matches from {cache_path}.")
    else:
        print(f"[{user_id}] No cache found yet; starting fresh.")

    known_ids = {m.get("game_id") for m in cached_matches if m.get("game_id")}
    new_matches, fetch_complete, reached_known, reached_end, last_page, timed_out = fetch_new_matches(user_id, known_ids=known_ids, max_pages=max_pages)
    print(f"[{user_id}] New matches fetched: {len(new_matches)} (Complete: {fetch_complete}, Reached known: {reached_known}, Reached end: {reached_end}, Last page: {last_page}, Timed out: {timed_out})")

    # Determine overall completeness
    if not fetch_complete:
        overall_complete = False
    elif reached_end:
        overall_complete = True
    elif reached_known:
        # We reached a known match, so we are as complete as we were before
        overall_complete = current_status.get("is_complete", True)
    else:
        # This case should be covered by is_complete = False in fetch_new_matches if it hits max_pages
        overall_complete = False

    save_status(user_id, {
        "is_complete": overall_complete, 
        "last_refresh": dt.datetime.now().isoformat(),
        "last_page_fetched": max(current_status.get("last_page_fetched", 0), last_page)
    })

    if new_matches:
        updated_matches = new_matches + cached_matches
        print(f"[{user_id}] Merging {len(new_matches)} new matches with {len(cached_matches)} cached matches...")
    else:
        updated_matches = cached_matches
        print(f"[{user_id}] No new matches added; cache is already up to date.")

    save_matches(updated_matches, cache_path)
    print(f"[{user_id}] Saved {len(updated_matches)} total matches to {cache_path}.")
    print(f"[{user_id}] Total matches available locally: {len(updated_matches)}")
    return updated_matches


def backfill_history(user_id: str, max_pages: int = None):
    cache_path = cache_path_for(user_id)
    current_status = load_status(user_id)
    
    start_page = current_status.get("last_page_fetched", 0) + 1
    
    print(f"[{user_id}] Backfilling history starting from page {start_page}...")
    
    batch_size = 20
    remaining_pages = max_pages if max_pages else MAX_FETCH_PAGES
    
    while remaining_pages > 0:
        cached_matches = load_cached_matches(cache_path)
        known_ids = {m.get("game_id") for m in cached_matches if m.get("game_id")}
        chunk = min(batch_size, remaining_pages)
        
        new_matches, fetch_complete, reached_known, reached_end, last_page, timed_out = fetch_new_matches(
            user_id, 
            known_ids=known_ids, 
            start_page=start_page,
            max_pages=chunk, 
            timeout_seconds=45, # Keep per-batch timeout manageable for web requests
            stop_at_known=False
        )
        
        print(f"[{user_id}] Batch result: fetched {len(new_matches)} matches. Last page: {last_page}, Timed out: {timed_out}, End: {reached_end}")

        if new_matches:
            # Multi-process safety: re-load before merge
            cached_matches = load_cached_matches(cache_path)
            known_ids = {m.get("game_id") for m in cached_matches if m.get("game_id")}
            unique_new = [m for m in new_matches if m.get("game_id") not in known_ids]
            if unique_new:
                updated_matches = cached_matches + unique_new
                save_matches(updated_matches, cache_path)
                print(f"[{user_id}] Saved {len(unique_new)} new matches.")

        # Update status incrementally
        overall_complete = current_status.get("is_complete", False)
        if fetch_complete and reached_end:
            overall_complete = True
        elif timed_out:
            overall_complete = False
        # If fetch_complete is False but not timed_out, it might be due to hit max_pages (chunk size)
        elif not fetch_complete and not reached_end:
             # We hit chunk limit, but not end. overall is still partial.
             overall_complete = False

        save_status(user_id, {
            "is_complete": overall_complete, 
            "last_refresh": dt.datetime.now().isoformat(),
            "last_page_fetched": max(current_status.get("last_page_fetched", 0), last_page)
        })
        
        if reached_end or timed_out:
            break
            
        # Prepare for next batch
        start_page = last_page + 1
        remaining_pages -= (last_page - (start_page - 1) + 1)
        if last_page < start_page - 1: # Safety break if no progress made
            break

    return load_cached_matches(cache_path)


def main():
    matches_by_user = {}
    for user_id in USER_IDS:
        matches_by_user[user_id] = refresh_matches(user_id)

    print("\n=== Ranked RM 1v1 analytics ===")
    print_ranked_analytics(matches_by_user)

    session_scope = "all modes" if not SESSION_MODE_FILTER else f"modes: {SESSION_MODE_FILTER}"
    print(f"\n=== Session analytics ({session_scope}) ===")
    print_session_analytics(matches_by_user, mode_filter=SESSION_MODE_FILTER)

    rm_filter = ["RM 1v1"]
    print("\n=== Session analytics (RM 1v1 only) ===")
    print_session_analytics(matches_by_user, mode_filter=rm_filter)


if __name__ == "__main__":
    main()
