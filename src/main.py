"""
Actor que calcula Engagement Rate (ER) de perfis públicos do Instagram.

• Não requer login nem cookies.
• Usa endpoint público `https://i.instagram.com/api/v1/users/web_profile_info/?username=<user>`
  (header X-IG-App-ID).
• Para cada perfil: pega seguidores + 12 posts → soma likes+comments → ER%.
• Grava cada linha no dataset e exibe progresso via log + set_status_message.
"""

from __future__ import annotations
from apify import Actor
import httpx
import math
import asyncio
import importlib.metadata

IG_ENDPOINT = (
    "https://i.instagram.com/api/v1/users/web_profile_info/"
    "?username={username}"
)

HEADERS = {
    "x-ig-app-id": "936619743392459",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
}

async def fetch_profile(client: httpx.AsyncClient, username: str) -> dict:
    """Baixa JSON público do perfil usando um cliente com proxy pré-configurado."""
    url = IG_ENDPOINT.format(username=username)
    try:
        r = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", {}).get("user")
        if not data:
            return {"username": username, "error": "perfil inexistente/privado"}

        followers = data.get("edge_followed_by", {}).get("count", 0)
        edges = data.get("edge_owner_to_timeline_media", {}).get("edges", [])[:12]

        likes_total = 0
        comments_total = 0
        for edge in edges:
            node = edge.get("node", {})
            likes_total += node.get("edge_liked_by", {}).get("count", 0)
            comments_total += node.get("edge_media_to_comment", {}).get("count", 0)

        n_posts = max(len(edges), 1)
        er = ((likes_total + comments_total) / n_posts) / followers * 100 if followers else 0

        return {
            "username": username,
            "followers": followers,
            "posts_analyzed": n_posts,
            "avg_likes": math.floor(likes_total / n_posts),
            "avg_comments": math.floor(comments_total / n_posts),
            "engagement_rate_pct": round(er, 2),
            "error": None,
        }
    except httpx.HTTPStatusError as e:
        return {"username": username, "error": f"HTTP Error: {e.response.status_code}"}
    except Exception as e:
        return {"username": username, "error": f"{type(e).__name__}: {e}"}

async def main() -> None:
    async with Actor:
        Actor.log.info(f"httpx version: {importlib.metadata.version('httpx')}")

        inp = await Actor.get_input() or {}
        usernames: list[str] = inp.get("usernames", [])
        if not usernames:
            raise ValueError("Input 'usernames' (uma lista de perfis) é obrigatório.")

        proxy_configuration = await Actor.create_proxy_configuration(groups=['RESIDENTIAL'])

        for idx, username in enumerate(usernames, 1):
            clean_username = username.strip("@ ")
            if not clean_username:
                continue

            session_id = f'session_{clean_username}'
            proxy_url = await proxy_configuration.new_url(session_id=session_id)
            
            transport = httpx.AsyncProxyTransport.from_url(proxy_url)
            async with httpx.AsyncClient(transport=transport) as http:
                result = await fetch_profile(http, clean_username)

            row = {
                "username": clean_username,
                "followers": 0,
                "posts_analyzed": 0,
                "avg_likes": 0,
                "avg_comments": 0,
                "engagement_rate_pct": 0.0,
                "error": None,
            }
            row.update(result)
            await Actor.push_data(row)

            msg = f"{idx}/{len(usernames)} → {clean_username}"
            if result.get("error"):
                msg += f" ❌ ({result['error']})"
            else:
                msg += " ✔"
            
            Actor.log.info(msg)
            await Actor.set_status_message(msg)

            await asyncio.sleep(0.2)
