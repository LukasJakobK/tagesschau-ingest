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

    # 🔴 WICHTIG: libsql:// -> https:// (kein WebSocket im CI)
    http_url = db_url.replace("libsql://", "https://")

    client = libsql_client.create_client(
        url=http_url,
        auth_token=token,
    )

    try:
        result = await client.execute("SELECT COUNT(*) AS cnt FROM articles")
        cnt = result.rows[0]["cnt"]

        print("✅ Connected to Turso")
        print("📊 Articles in DB:", cnt)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())


