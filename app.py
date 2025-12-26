from flask import Flask, render_template, request, jsonify, redirect, url_for
import requests
from bs4 import BeautifulSoup
import os
import json
import threading
from pathlib import Path

# Import logic from match history script
import aoe2_match_history as mh

app = Flask(__name__)

# Global dictionary to track backfill jobs: {user_id: {"status": "running", "page": 0, "count": 0}}
BACKFILL_STATUS = {}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
})

def search_aoe2_player(name):
    url = "https://www.aoe2insights.com/search/"
    try:
        resp = SESSION.get(url, params={'q': name}, timeout=10)
        if resp.status_code != 200:
            return []
        
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        seen_ids = set()
        
        for anchor in soup.select("a[href^='/user/']"):
            href = anchor.get("href")
            parts = [p for p in href.split("/") if p]
            if len(parts) >= 2 and parts[0] == 'user':
                user_id = parts[1]
                if user_id.isdigit() and user_id not in seen_ids:
                    container = anchor.find_parent("div", class_="card-body") or anchor.find_parent("div")
                    name_node = container.select_one(".h4") if container else None
                    
                    if name_node:
                        player_name = name_node.get_text(strip=True)
                    else:
                        player_name = anchor.get_text(strip=True) or anchor.get("title")
                    
                    if not player_name:
                        img = container.select_one("img") if container else None
                        if img:
                            player_name = img.get("alt")
                    
                    if player_name and player_name.lower() != 'login':
                        results.append({"name": player_name, "id": user_id})
                        seen_ids.add(user_id)
        return results
    except Exception as e:
        print(f"Error searching for player: {e}")
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    query = request.form.get('query', '')
    if not query:
        return jsonify([])
    players = search_aoe2_player(query)
    return render_template('search_results.html', players=players, query=query)

def get_player_context(user_id):
    # Ensure user_id is a string for the match history logic
    user_id = str(user_id)
    cache_path = mh.cache_path_for(user_id)
    matches = mh.load_cached_matches(cache_path)
    # Sort newest first for display
    matches.sort(key=mh.match_sort_key, reverse=True)
    
    stats = None
    sessions_data = None
    
    if matches:
        stats = mh.compute_ranked_stats(matches, user_id)
        if stats:
            stats['opponents_list'] = mh.sorted_items_list(stats['opponents'], key_fn=lambda r: (-r['matches'], -r['wins']))
            stats['civs_list'] = mh.sorted_items_list(stats['civs'], key_fn=lambda r: (-r['matches'], -r['wins']))
            stats['opp_civs_list'] = mh.sorted_items_list(stats['opp_civs'], key_fn=lambda r: (-r['matches'], -r['wins']))
            stats['maps_list'] = mh.sorted_items_list(stats['maps'], key_fn=lambda r: (-r['matches'], -r['wins']))
            stats['duration_list'] = mh.sorted_items_list(stats['duration'], key_fn=lambda r: (-r['matches'], -r['wins']))
            
        # Session analytics
        prepared, _, _ = mh.prepare_user_matches(matches, user_id, mode_filter=["RM 1v1"])
        if prepared:
            sessions = mh.group_sessions(prepared)
            metrics = mh.session_metrics(sessions)
            nth_stats = mh.nth_game_winrates(sessions)
            sessions_data = {
                "count": len(sessions),
                "metrics": metrics,
                "nth_stats": nth_stats
            }

    return {
        "user_id": user_id,
        "matches": matches[:100], # show last 100 in table for stability
        "stats": stats,
        "sessions_data": sessions_data
    }

@app.route('/user/<int:user_id>')
def player_profile(user_id):
    context = get_player_context(user_id)
    context["player_name"] = request.args.get('name', f"Player {user_id}")
    context["fetch_status"] = mh.load_status(str(user_id))
    return render_template('player.html', **context)

@app.route('/user/<int:user_id>/stats_partial')
def player_stats_partial(user_id):
    context = get_player_context(user_id)
    # We don't need player_name or fetch_status for the partial stats view typically,
    # unless they are used in player_content.html (which stats is, fetch_status maybe not)
    # Checking player_content.html: it uses stats, sessions_data, matches, user_id.
    # It does NOT use fetch_status or player_name (header is in player.html).
    return render_template('player_content.html', **context)

@app.route('/user/<int:user_id>/refresh', methods=['POST'])
def refresh_player(user_id):
    user_id = str(user_id)
    # Prevent concurrent refresh/backfill for the same user across processes
    if mh.is_file_locked(mh.status_path_for(user_id)):
        return jsonify({"status": "error", "message": "A data operation is currently in progress for this user. Please wait."}), 409
        
    try:
        mh.refresh_matches(user_id) # Full fetch (up to 2000 pages)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def run_backfill(user_id):
    user_id = str(user_id)
    try:
        # Status is now initialized in backfill_player to avoid race conditions
        def status_callback(matches, page):
            if user_id in BACKFILL_STATUS:
                BACKFILL_STATUS[user_id]["page"] = page
                BACKFILL_STATUS[user_id]["count"] = len(matches)

        mh.backfill_history(user_id, status_callback=status_callback)
        BACKFILL_STATUS[user_id]["status"] = "finished"
    except Exception as e:
        print(f"Backfill error for {user_id}: {e}")
        BACKFILL_STATUS[user_id] = {"status": "error", "message": str(e)}
    # We keep the status for a while so the frontend can see 'finished' or 'error'

@app.route('/user/<user_id>/backfill', methods=['POST'])
def backfill_player(user_id):
    user_id = str(user_id)
    # Prevent concurrent refresh/backfill for the same user across processes
    if mh.is_file_locked(mh.status_path_for(user_id)):
        return jsonify({"status": "running", "message": "A data operation is already in progress"}), 202

    # Initialize status in main thread to avoid race condition with polling
    current_status = mh.load_status(user_id)
    start_page = current_status.get("last_page_fetched", 0)
    BACKFILL_STATUS[user_id] = {"status": "running", "page": start_page, "count": 0}

    # Start in background thread
    thread = threading.Thread(target=run_backfill, args=(user_id,))
    thread.start()

    return jsonify({"status": "started"}), 202

@app.route('/user/<int:user_id>/backfill/status', methods=['GET'])
def backfill_status(user_id):
    user_id = str(user_id)
    
    # Check if ANY process currently has the backfill lock
    is_running = mh.is_file_locked(mh.status_path_for(user_id))
    
    if is_running:
        # If running, prefer the live disk status for the page/count
        status = mh.load_status(user_id)
        return jsonify({
            "status": "running",
            "page": status.get("last_page_fetched", 0),
            "count": status.get("num_matches", 0)
        })
    else:
        # Check if the in-memory state has anything (useful for final success state)
        # but otherwise default to not_running.
        status = BACKFILL_STATUS.get(user_id, {"status": "not_running"})
        return jsonify(status)

if __name__ == '__main__':
    app.run(debug=False, port=5000, host='127.0.0.1')
