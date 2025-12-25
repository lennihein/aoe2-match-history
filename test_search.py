
import requests
from bs4 import BeautifulSoup
import re

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
})

def search_aoe2_player(name):
    print(f"Searching for: {name}")
    url = f"https://www.aoe2insights.com/search/?q={name}"
    resp = SESSION.get(url, timeout=10)
    print(f"Status code: {resp.status_code}")
    if resp.status_code != 200:
        return []
    
    soup = BeautifulSoup(resp.text, "lxml")
    results = []
    
    # Each player result seems to be in a div with some class
    # Based on the curl, look for links starting with /user/
    seen_ids = set()
    
    # It seems the search results are in cards
    # Let's find all user links
    for anchor in soup.select("a[href^='/user/']"):
        href = anchor.get("href")
        parts = [p for p in href.split("/") if p]
        if len(parts) >= 2 and parts[0] == 'user':
            user_id = parts[1]
            if user_id.isdigit() and user_id not in seen_ids:
                # The name is usually in a p.h4 sibling or parent
                container = anchor.find_parent("div", class_="card-body") or anchor.find_parent("div")
                name_node = container.select_one(".h4") if container else None
                
                if name_node:
                    player_name = name_node.get_text(strip=True)
                else:
                    player_name = anchor.get_text(strip=True) or anchor.get("title")
                
                if not player_name:
                    # check alt tags in images
                    img = container.select_one("img") if container else None
                    if img:
                        player_name = img.get("alt")
                
                if player_name and player_name.lower() != 'login':
                    results.append({"name": player_name, "id": user_id})
                    seen_ids.add(user_id)
    
    return results

if __name__ == "__main__":
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "TheViper"
    players = search_aoe2_player(query)
    for p in players:
        print(f"Name: {p['name']}, ID: {p['id']}")
