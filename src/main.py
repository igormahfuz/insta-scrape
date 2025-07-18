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
import os
from apify_client import ApifyClient


IG_ENDPOINT = (
    "https://i.instagram.com/api/v1/users/web_profile_info/"
    "?username={username}"
)
FIELDS = [
    "username",
    "followers",
    "posts_analyzed",
    "avg_likes",
    "avg_comments",
    "engagement_rate_%",
    "error"            # manter mesmo se quase sempre None
]


HEADERS = {
    # App‑ID usado pelos apps oficiais (público)
    "x-ig-app-id": "936619743392459",
    # User‑Agent “normal” para evitar bloqueios triviais
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
}


async def fetch_profile(client: httpx.AsyncClient, username: str) -> dict:
    """Baixa JSON público do perfil e devolve dicionário com ER."""
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
            "engagement_rate_%": round(er, 2),
            "error": None,
        }
    except httpx.HTTPStatusError as e:
        return {"username": username, "error": f"HTTP Error: {e.response.status_code}"}
    except Exception as e:
        # Adiciona o tipo da exceção para facilitar o debug
        return {"username": username, "error": f"{type(e).__name__}: {e}"}


async def main() -> None:
    async with Actor:
        # 1. Lê o input
        inp = await Actor.get_input() or {}
        usernames: list[str] = inp.get("usernames", [])
        if not usernames:
            raise ValueError("Input 'usernames' (uma lista de perfis) é obrigatório.")

        # 2. Configura o proxy
        # O Actor.new_client() já usa as configurações de proxy do input automaticamente
        proxy_configuration = await Actor.create_proxy_configuration()
        async with Actor.new_client() as http:
            
            # 3. Processa cada perfil
            for idx, username in enumerate(usernames, 1):
                # Pega o nome de usuário limpo, sem @ ou espaços
                clean_username = username.strip("@ ")
                if not clean_username:
                    continue

                result = await fetch_profile(http, clean_username)

                # Garante que todas as colunas existem, mesmo em caso de erro
                row = {field: None for field in FIELDS}
                row.update(result)
                await Actor.push_data(row)

                # Atualiza o status para sabermos o progresso
                msg = f"{idx}/{len(usernames)} → {clean_username}"
                if result.get("error"):
                    msg += f" ❌ ({result['error']})"
                else:
                    msg += " ✔"
                
                Actor.log.info(msg)
                await Actor.set_status_message(msg)

                # Pausa para não sobrecarregar o servidor (opcional, mas recomendado)
                await asyncio.sleep(0.2)