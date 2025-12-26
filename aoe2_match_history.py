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
    return DATA_DIR / f"matches_{user_id}.json"


# Mappings from Relic API (Age II)
# Correct Civilization List matching Relic API indices (Alphabetical order of standard civs)
# Derived from reverse-engineering API IDs 26->Lithuanians and 41->Teutons
CIV_LIST = [
    "Armenians", "Aztecs", "Bengalis", "Berbers", "Bohemians", "Britons", 
    "Bulgarians", "Burgundians", "Burmese", "Byzantines", "Celts", "Chinese", 
    "Cumans", "Dravidians", "Ethiopians", "Franks", "Georgians", "Goths", 
    "Gurjaras", "Hindustanis", "Huns", "Incas", "Italians", "Japanese", 
    "Khmer", "Koreans", "Lithuanians", "Magyars", "Malay", "Malians", 
    "Mayans", "Mongols", "Persians", "Poles", "Portuguese", "Romans", 
    "Saracens", "Sicilians", "Slavs", "Spanish", "Tatars", "Teutons", 
    "Turks", "Vietnamese", "Vikings"
]

CIV_MAP = {i: name for i, name in enumerate(CIV_LIST)}

MODE_MAP = {
    0: "Custom",
    1: "RM 1v1", 2: "RM 1v1", 3: "RM 2v2", 4: "RM 3v3", 5: "RM 4v4",
    6: "RM 1v1", 7: "RM 2v2", 8: "RM 3v3", 9: "RM 4v4",
    10: "FFA",
    26: "EW 1v1", 27: "EW 2v2", 28: "EW 3v3", 29: "EW 4v4",
    60: "Custom DM 1v1", 61: "Custom DM Team",
    66: "RM 1v1", 67: "RM 2v2", 68: "RM 3v3", 69: "RM 4v4",
    86: "RM 1v1", 87: "RM 2v2", 88: "RM 3v3", 89: "RM 4v4",
    120: "Custom", 121: "Custom", 122: "Custom", 123: "Custom", 124: "Custom", 125: "Custom"
}

def clean_map_name(name: str) -> str:
    if not name: return "Unknown"
    # Strip .rms/.rms2 extensions
    name = re.sub(r"\.rms\d*$", "", name, flags=re.IGNORECASE)
    # Title-case unless it's a known custom placeholder
    if name.lower() != "my map":
        name = name.title()
    return name.strip()

def determine_win(team: dict, player: dict) -> bool:
    # Ranked games: use elo_change if present
    if player.get("elo_change"):
        return player["elo_change"] > 0
    # Unranked: use outcome if present
    if "outcome" in player: # outcome 1 = Win, 0 = Loss
        # Ensure it's explicitly 1 or 0, not None
        return player["outcome"] == 1
    # Fallback to team won flag
    return team.get("won", False)

ID_MAPPING_FILE = DATA_DIR / "id_mappings.json"

def load_id_mappings():
    if ID_MAPPING_FILE.exists():
        with ID_MAPPING_FILE.open("r") as f:
            return json.load(f)
    return {}

def save_id_mapping(insights_id, relic_id):
    mappings = load_id_mappings()
    mappings[insights_id] = relic_id
    ID_MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
    with ID_MAPPING_FILE.open("w") as f:
        json.dump(mappings, f, indent=2)

def get_relic_id(insights_id):
    """
    Tries to find the Relic profile_id for a given AoE2 Insights user ID.
    """
    # Check cache first
    mappings = load_id_mappings()
    if insights_id in mappings:
        return mappings[insights_id]

    # Check if the insights_id is already a working Relic ID (like TheViper 196240)
    # We'll try to fetch recent matches for it.
    print(f"Checking if {insights_id} is already a Relic ID...")
    test_url = f"https://aoe-api.reliclink.com/community/leaderboard/getRecentMatchHistory?title=age2&profile_ids=[{insights_id}]&count=1"
    try:
        resp = SESSION.get(test_url, timeout=10, verify=False)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("matchHistoryStats"):
                print(f"Verified {insights_id} as a Relic ID.")
                save_id_mapping(insights_id, insights_id)
                return insights_id
    except:
        pass

    # Scrape Insights profile to find "Game Id"
    url = f"https://www.aoe2insights.com/user/{insights_id}/"
    print(f"Scraping Insights profile {url} to find Relic ID...")
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code == 200:
            # Look for <small class="badge text-bg-secondary">Game Id: 598457</small>
            match = re.search(r"Game Id: (\d+)", resp.text)
            if match:
                relic_id = match.group(1)
                print(f"Found Relic ID: {relic_id}")
                save_id_mapping(insights_id, relic_id)
                return relic_id
    except:
        pass

    return insights_id # Fallback to original if all fails


def get_native_matches(relic_id: str, start: int = 0, count: int = 100):
    url = f"https://aoe-api.reliclink.com/community/leaderboard/getRecentMatchHistory?title=age2&profile_ids=[{relic_id}]&start={start}&count={count}"
    try:
        # Relic API uses a self-signed or invalid cert for aoe-api.reliclink.com sometimes
        # or it's valid for *.worldsedgelink.com. We'll use verify=False for now as it's common for this API.
        resp = SESSION.get(url, timeout=30, verify=False)
        if resp.status_code != 200:
            print(f"Error fetching from native API: {resp.status_code}")
            return None
        return resp.json()
    except Exception as e:
        print(f"Exception fetching from native API: {e}")
        return None


def parse_native_match(raw_match, profiles_map):
    match_id = str(raw_match.get("id"))
    match_type_id = raw_match.get("matchtype_id")
    mode = MODE_MAP.get(match_type_id, f"Mode {match_type_id}")
    
    raw_map = raw_match.get("mapname", "Unknown Map")
    map_name = clean_map_name(raw_map)
    
    start_ts = raw_match.get("startgametime")
    end_ts = raw_match.get("completiontime")
    
    start_dt = dt.datetime.fromtimestamp(start_ts) if start_ts else None
    end_dt = dt.datetime.fromtimestamp(end_ts) if end_ts else None
    
    duration_str = None
    if start_ts and end_ts:
        diff = end_ts - start_ts
        h = diff // 3600
        m = (diff % 3600) // 60
        s = diff % 60
        if h > 0:
            duration_str = f"{h}h {m}m {s}s"
        else:
            duration_str = f"{m}m {s}s"

    teams_data = defaultdict(list)
    members = raw_match.get("matchhistorymember", [])
    
    # Pre-scan members to determine team winners if possible
    # (Though we determine per-player win status now)
    
    for member in members:
        p_id = str(member.get("profile_id"))
        p_info = profiles_map.get(p_id, {})
        team_id = member.get("teamid")
        
        civ_id = member.get("civilization_id")
        civ_name = CIV_MAP.get(civ_id, f"Civ {civ_id}")
        
        old_r = member.get("oldrating")
        new_r = member.get("newrating")
        elo_change = (new_r - old_r) if (new_r is not None and old_r is not None) else None
        
        # We construct a partial player object to use our helper
        # Note: 'outcome' is typically 1 (win) or 0 (loss) in API
        # We pass the raw member dict which contains 'outcome'
        
        player_obj = {
            "player_id": p_id,
            "player_name": p_info.get("alias", p_info.get("name", "Unknown")),
            "civ_id": civ_id,
            "civ": civ_name,
            "elo": old_r,
            "elo_change": elo_change,
            "strategy": None,
            "won": False, # Placeholder
            # Internal fields for determining win
            "outcome": member.get("outcome")
        }
        
        teams_data[team_id].append(player_obj)
    
    teams = []
    for t_id in sorted(teams_data.keys()):
        players = teams_data[t_id]
        # First determine win for each player
        team_obj = {"won": False} # Mutable for fallback
        
        # Check if any player has definite win
        for p in players:
            p["won"] = determine_win(team_obj, p)
            # Remove internal fields before saving
            p.pop("outcome", None)
            
        # Refine team won status: if any player won, team won?
        # Or usually team outcome is shared.
        # We'll set team "won" if majority won, but really we rely on individual player win for stats
        team_won = any(p["won"] for p in players)
        
        teams.append({
            "won": team_won,
            "players": players
        })

    return {
        "game_id": match_id,
        "mode": mode,
        "map": map_name,
        "duration": duration_str,
        "start_datetime": format_dt(start_dt),
        "end_datetime": format_dt(end_dt),
        "teams": teams,
    }


def normalize_match(match):
    if not isinstance(match, dict):
        return None
    game_id = match.get("game_id") or match.get("match_id")
    
    raw_map = match.get("map") or match.get("map_name") or "Unknown Map"
    map_name = clean_map_name(raw_map)
    
    teams = match.get("teams") or []
    normalized_teams = []
    for team in teams:
        players = []
        for p in team.get("players", []):
            players.append(
                {
                    "player_id": p.get("player_id") or p.get("id"),
                    "player_name": p.get("player_name") or p.get("name"),
                    "civ_id": p.get("civ_id"),
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
        "map": map_name,
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
    # Deduplicate by game_id
    unique_matches = {}
    for m in matches:
        gid = m.get("game_id")
        if gid:
            # If we see it again, the later one in the list (newer fetch) might be fresher
            unique_matches[gid] = m
    
    sorted_matches = sorted(unique_matches.values(), key=match_sort_key, reverse=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(sorted_matches, f, ensure_ascii=False, indent=2)


def fetch_new_matches(insights_id: str, known_ids=None, start_page: int = 0, max_pages: int = None):
    if max_pages is None:
        max_pages = 10
    
    relic_id = get_relic_id(insights_id)
    known_ids = set(known_ids or [])
    all_new_matches = []
    seen_ids = set()
    reached_known = False
    
    for i in range(max_pages):
        start = i * 100
        print(f"Fetching native matches {start} to {start + 100} for user {insights_id} (relic:{relic_id})...")
        data = get_native_matches(relic_id, start=start, count=100)
        
        if not data or not data.get("matchHistoryStats"):
            print("No more matches found.")
            break
            
        profiles = {str(p["profile_id"]): p for p in data.get("profiles", [])}
        matches = data.get("matchHistoryStats")
        
        for m in matches:
            game_id = str(m.get("id"))
            if game_id in known_ids:
                print(f"Encountered cached match {game_id}; stopping.")
                reached_known = True
                break
            
            if game_id in seen_ids:
                continue
            
            parsed = parse_native_match(m, profiles)
            all_new_matches.append(parsed)
            seen_ids.add(game_id)
            
        if reached_known:
            break
            
    return all_new_matches


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

    # Get relic ID for the user to match against native API data
    relic_id = get_relic_id(user_id)

    for match in matches:
        if match.get("mode") not in ["RM 1v1", "1v1"]:
            continue
        teams = match.get("teams") or []
        user_team_idx = None
        user_player = None
        for idx, team in enumerate(teams):
            for p in team.get("players", []):
                # Match against either Insights ID or Relic ID
                p_id = p.get("player_id")
                if p_id == user_id or p_id == relic_id:
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
    relic_id = get_relic_id(user_id)
    teams = match.get("teams") or []
    user_team_idx = None
    user_player = None
    for idx, t in enumerate(teams):
        for p in t.get("players", []):
            p_id = p.get("player_id")
            if p_id == user_id or p_id == relic_id:
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
    if cache_path.exists():
        print(f"[{user_id}] Loaded {len(cached_matches)} cached matches from {cache_path}.")
    else:
        print(f"[{user_id}] No cache found yet; starting fresh.")

    known_ids = {m.get("game_id") for m in cached_matches if m.get("game_id")}
    new_matches = fetch_new_matches(user_id, known_ids=known_ids, max_pages=max_pages)
    print(f"[{user_id}] New matches fetched: {len(new_matches)}")

    if new_matches:
        # Create a dict to merge and preserve latest
        all_map = {m["game_id"]: m for m in cached_matches}
        for m in new_matches:
            all_map[m["game_id"]] = m
        updated_matches = list(all_map.values())
        print(f"[{user_id}] Merging {len(new_matches)} new matches with {len(cached_matches)} cached matches...")
    else:
        updated_matches = cached_matches
        print(f"[{user_id}] No new matches added; cache is already up to date.")

    save_matches(updated_matches, cache_path)
    # The count might have changed if there were duplicates in old cache
    final_matches = load_cached_matches(cache_path)
    print(f"[{user_id}] Saved {len(final_matches)} unique matches to {cache_path}.")
    print(f"[{user_id}] Total matches available locally: {len(final_matches)}")
    return final_matches


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
def dedupe_cache_file(path: Path) -> None:
    if not path.exists():
        return
    matches = load_cached_matches(path)
    # Deduplicate by game_id
    uniq = {m["game_id"]: m for m in matches}
    # Sort just in case
    merged = list(uniq.values())
    merged.sort(key=match_sort_key, reverse=True)
    save_matches(merged, path)
    print(f"Deduped {path}: {len(matches)} -> {len(merged)}")
