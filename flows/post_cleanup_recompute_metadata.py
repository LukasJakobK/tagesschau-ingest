import sys
from pathlib import Path

# --------------------------------------------------
# Make project root importable (so "src" works everywhere)
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# --------------------------------------------------
# Imports
# --------------------------------------------------
import os
import asyncio
from src.tagesschau_client import TagesschauClient


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


# --------------------------------------------------
# Main
# --------------------------------------------------
async def main():
    print("=== POST CLEANUP: RECOMPUTE METADATA FROM JSON CONFIGS ===")

    # Ensure secrets exist
    require_env("TURSO_DB_URL")
    require_env("TURSO_AUTH_TOKEN")

    # Client as resolver + DB access
    client = TagesschauClient(
        api_config_path="config/api_config.json",
        regions_path="config/regions.json",
        source_regions_path="config/source_regions.json",
        url_region_keywords_path="config/url_region_keywords.json",
        filters_path="config/filters.json",
    )

    db = await client._connect()

    # Fetch all rows (use rowid as technical anchor)
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

            # Recompute metadata using EXACT same logic as ingest
            source = client._source_from_url(url)
            region_by_source = client._region_by_source(source)
            region_by_url, subregion_by_url = client._region_by_url(url)

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
        # Always close DB cleanly
        if client._db is not None:
            await client._db.close()
            client._db = None

    print(f"Done. Updated {updated} rows.")
    print("=== POST CLEANUP FINISHED ===")


# --------------------------------------------------
# Entrypoint
# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
