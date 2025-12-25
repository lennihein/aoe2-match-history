from flask import Flask, render_template, request, jsonify
import requests
from bs4 import BeautifulSoup

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
