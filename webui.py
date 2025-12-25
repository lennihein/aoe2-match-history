from __future__ import annotations

from flask import Flask, redirect, render_template_string, request, url_for

from aoe2_match_history import (
    cache_path_for,
    load_cached_matches,
    match_sort_key,
    refresh_matches,
    user_outcome,
)

app = Flask(__name__)

PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AoE2 Match History</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; background: #f7f7f7; color: #1f1f1f; }
      header { margin-bottom: 1.5rem; }
      form { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; }
      input[type="text"] { padding: 0.5rem; font-size: 1rem; min-width: 14rem; }
      button { padding: 0.5rem 1rem; font-size: 1rem; cursor: pointer; }
      .card { background: #fff; padding: 1rem 1.5rem; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
      table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
      th, td { text-align: left; padding: 0.5rem; border-bottom: 1px solid #e3e3e3; }
      th { background: #f1f1f1; }
      .badge { padding: 0.2rem 0.5rem; border-radius: 999px; font-size: 0.85rem; }
      .badge.win { background: #e4f7e7; color: #1b6c2e; }
      .badge.loss { background: #ffe6e6; color: #a53434; }
      .muted { color: #666; }
    </style>
  </head>
  <body>
    <header>
      <h1>AoE2 Match History</h1>
      <p class="muted">Browse match history for a player ID from aoe2insights.com.</p>
    </header>

    <section class="card">
      <form method="get" action="{{ url_for('index') }}">
        <label for="player_id"><strong>Player ID</strong></label>
        <input id="player_id" name="player_id" type="text" placeholder="e.g. 12559976" value="{{ player_id or '' }}" />
        <label>
          <input type="checkbox" name="refresh" value="1" {% if refresh %}checked{% endif %} /> Refresh from aoe2insights
        </label>
        <button type="submit">Search</button>
      </form>

      {% if player_id %}
        {% if matches %}
          <p><strong>{{ matches|length }}</strong> matches loaded for player <strong>{{ player_id }}</strong>.</p>
          <table>
            <thead>
              <tr>
                <th>Start</th>
                <th>End</th>
                <th>Mode</th>
                <th>Map</th>
                <th>Duration</th>
                <th>Result</th>
              </tr>
            </thead>
            <tbody>
              {% for match in matches %}
                <tr>
                  <td>{{ match.start_datetime or 'Unknown' }}</td>
                  <td>{{ match.end_datetime or 'Unknown' }}</td>
                  <td>{{ match.mode or 'Unknown' }}</td>
                  <td>{{ match.map or 'Unknown' }}</td>
                  <td>{{ match.duration or 'Unknown' }}</td>
                  <td>
                    {% if match.result is not none %}
                      <span class="badge {{ 'win' if match.result else 'loss' }}">{{ 'Win' if match.result else 'Loss' }}</span>
                    {% else %}
                      <span class="muted">Unknown</span>
                    {% endif %}
                  </td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        {% else %}
          <p class="muted">No cached matches yet. Try checking “Refresh from aoe2insights”.</p>
        {% endif %}
      {% else %}
        <p class="muted">Enter a player ID to begin.</p>
      {% endif %}
    </section>
  </body>
</html>
"""


def _load_matches(player_id: str, refresh: bool):
    if refresh:
        return refresh_matches(player_id)
    cache_path = cache_path_for(player_id)
    return load_cached_matches(cache_path)


@app.route("/")
def index():
    player_id = request.args.get("player_id", "").strip()
    refresh = request.args.get("refresh") == "1"
    matches = []
    if player_id:
        matches = _load_matches(player_id, refresh=refresh)
        matches = sorted(matches, key=match_sort_key, reverse=True)

    view_matches = []
    for match in matches:
        result, _ = user_outcome(match, player_id) if player_id else (None, None)
        view_matches.append(
            {
                **match,
                "result": result,
            }
        )

    return render_template_string(
        PAGE_TEMPLATE,
        player_id=player_id,
        refresh=refresh,
        matches=view_matches,
    )


@app.route("/player/<player_id>")
def player_redirect(player_id: str):
    return redirect(url_for("index", player_id=player_id))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
