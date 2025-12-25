from flask import Flask, render_template, request, jsonify, redirect, url_for
import requests
from bs4 import BeautifulSoup
import os
import json
from pathlib import Path

# Import logic from match history script
import aoe2_match_history as mh

app = Flask(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
})

def search_aoe2_player(name):
    url = f"https://www.aoe2insights.com/search/?q={name}"
    try:
        resp = SESSION.get(url, timeout=10)
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

@app.route('/user/<user_id>')
def player_profile(user_id):
    player_name = request.args.get('name', f"Player {user_id}")
    cache_path = mh.cache_path_for(user_id)
    matches = mh.load_cached_matches(cache_path)
    # Sort newest first for display
    matches.sort(key=mh.match_sort_key, reverse=True)
    
    stats = None
    sessions_data = None
    if matches:
        stats = mh.compute_ranked_stats(matches, user_id)
        # Convert some stats for easy template iteration
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

    return render_template('player.html', 
                           user_id=user_id, 
                           player_name=player_name, 
                           matches=matches[:20], # Show last 20 matches
                           stats=stats,
                           sessions_data=sessions_data)

@app.route('/user/<user_id>/refresh', methods=['POST'])
def refresh_player(user_id):
    try:
        mh.refresh_matches(user_id)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
