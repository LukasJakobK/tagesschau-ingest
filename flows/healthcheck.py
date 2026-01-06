import os
import asyncio
import libsql_client

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

async def main():
    db_url = require_env("TURSO_DB_URL")
    token = require_env("TURSO_AUTH_TOKEN")

    client = libsql_client.create_client(url=db_url, auth_token=token)
    try:
        result = await client.execute("SELECT COUNT(*) AS cnt FROM articles")
        print("✅ Connected to Turso")
        print("📊 Articles in DB:", result.rows[0]["cnt"])
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())

