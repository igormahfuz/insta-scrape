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

# --- Função de Lógica Principal com Retentativas ---

async def fetch_profile(client: httpx.AsyncClient, username: str, proxy_url: str) -> dict:
    """
    Baixa JSON público do perfil com lógica de retentativa para erros de rede.
    """
    url = IG_ENDPOINT.format(username=username)
    
    MAX_RETRIES = 3
    BASE_DELAY_SECONDS = 2
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            r = await client.get(url, headers=HEADERS, proxy=proxy_url, follow_redirects=True, timeout=30)
            r.raise_for_status()
            
            data = r.json().get("data", {}).get("user")
            if not data:
                return {"username": username, "error": "perfil inexistente/privado"}

            followers = data.get("edge_followed_by", {}).get("count", 0)
            edges = data.get("edge_owner_to_timeline_media", {}).get("edges", [])[:12]

            total_engagement_score = 0
            for edge in edges:
                node = edge.get("node", {})
                post_score = node.get("edge_liked_by", {}).get("count", 0) + node.get("edge_media_to_comment", {}).get("count", 0)
                if node.get('is_video', False):
                    post_score += node.get('video_view_count', 0)
                total_engagement_score += post_score

            n_posts = max(len(edges), 1)
            avg_engagement_score = total_engagement_score / n_posts
            er = (avg_engagement_score / followers) * 100 if followers else 0

            return {
                "username": username,
                "followers": followers,
                "posts_analyzed": n_posts,
                "avg_engagement_score": math.floor(avg_engagement_score),
                "engagement_rate_pct": round(er, 2),
                "error": None,
            }
        
        except (httpx.HTTPStatusError, httpx.ProxyError, httpx.ReadTimeout) as e:
            last_error = e
            Actor.log.warning(f"Tentativa {attempt + 1}/{MAX_RETRIES} falhou para '{username}': {type(e).__name__}. Retentando...")
            
            # Exponential backoff: 2s, 4s, 8s
            delay = BASE_DELAY_SECONDS * (2 ** attempt)
            await asyncio.sleep(delay)
        
        except Exception as e:
            # Erros inesperados não são retentados
            return {"username": username, "error": f"Erro inesperado: {type(e).__name__}: {e}"}

    # Se todas as tentativas falharem, retorna o último erro conhecido
    return {"username": username, "error": f"Falha após {MAX_RETRIES} tentativas: {type(last_error).__name__}"}


# --- Função para Processamento Concorrente (sem modificação) ---

async def process_and_save_username(
    client: httpx.AsyncClient,
    username: str,
    proxy_config,
    semaphore: asyncio.Semaphore
) -> dict:
    """
    Adquire o semáforo, busca o perfil, salva os dados no Actor e retorna o resultado.
    """
    async with semaphore:
        session_id = f'session_{username}'
        proxy_url = await proxy_config.new_url(session_id=session_id)
        
        result = await fetch_profile(client, username, proxy_url)

        row = {
            "username": username,
            "followers": 0,
            "posts_analyzed": 0,
            "avg_engagement_score": 0,
            "engagement_rate_pct": 0.0,
            "error": None,
        }
        row.update(result)
        
        await Actor.push_data(row)
        return result

# --- Função Principal Modificada para Concorrência (sem modificação) ---

async def main() -> None:
    async with Actor:
        Actor.log.info(f"httpx version: {importlib.metadata.version('httpx')}")

        inp = await Actor.get_input() or {}
        usernames: list[str] = inp.get("usernames", [])
        if not usernames:
            raise ValueError("Input 'usernames' (uma lista de perfis) é obrigatório.")

        CONCURRENCY = 20
        semaphore = asyncio.Semaphore(CONCURRENCY)
        
        proxy_configuration = await Actor.create_proxy_configuration(groups=['RESIDENTIAL'])
        
        total_usernames = len(usernames)
        processed_count = 0
        
        Actor.log.info(f"Iniciando processamento de {total_usernames} usernames com concorrência de {CONCURRENCY}.")

        async with httpx.AsyncClient() as client:
            tasks = []
            for username in usernames:
                clean_username = username.strip("@ ")
                if not clean_username:
                    total_usernames -= 1
                    continue
                
                task = process_and_save_username(client, clean_username, proxy_configuration, semaphore)
                tasks.append(task)

            for future in asyncio.as_completed(tasks):
                result = await future
                processed_count += 1
                
                username_processed = result.get('username', 'N/A')
                msg = f"{processed_count}/{total_usernames} → {username_processed}"
                
                if result.get("error"):
                    msg += f" ❌ ({result['error']})"
                else:
                    msg += " ✔"
                
                Actor.log.info(msg)
                await Actor.set_status_message(msg)
        
        Actor.log.info("Processamento concluído.")
