import os
import asyncio
from src.tagesschau_client import TagesschauClient


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


async def main():
    print("=== POST CLEANUP: RECOMPUTE METADATA FROM JSON CONFIGS ===")

    # Sicherstellen, dass Secrets da sind
    require_env("TURSO_DB_URL")
    require_env("TURSO_AUTH_TOKEN")

    # Client als Resolver + DB-Access benutzen
    client = TagesschauClient(
        api_config_path="config/api_config.json",
        regions_path="config/regions.json",
        source_regions_path="config/source_regions.json",
        url_region_keywords_path="config/url_region_keywords.json",
        filters_path="config/filters.json",
        connect_db=True,
    )

    db = await client._connect()

    # 🔴 WICHTIG: rowid mit selektieren (technischer Zeilenanker)
    rs = await db.execute("""
        SELECT
            rowid,
            url
        FROM articles
    """)

    total = len(rs.rows)
    print(f"Processing {total} rows...")

    updated = 0

    try:
        for row in rs.rows:
            rowid = row["rowid"]
            url = row["url"]

            # 🔁 EXAKT DIE GLEICHE LOGIK WIE BEIM INGEST
            source, region_by_source, region_by_url, subregion_by_url = (
                client.recompute_metadata_from_url(url)
            )

            await db.execute(
                """
                UPDATE articles
                SET
                    source = ?,
                    region_by_source = ?,
                    region_by_url = ?,
                    subregion_by_url = ?
                WHERE rowid = ?
                """,
                [
                    source,
                    region_by_source,
                    region_by_url,
                    subregion_by_url,
                    rowid,
                ],
            )

            updated += 1
            if updated % 200 == 0:
                print(f"  updated {updated}/{total} rows...")

    finally:
        await client._close()

    print(f"Done. Updated {updated} rows.")
    print("=== POST CLEANUP FINISHED ===")


if __name__ == "__main__":
    asyncio.run(main())
