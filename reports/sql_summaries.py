import os
import asyncio
import json
from pathlib import Path
import libsql_client
from datetime import datetime


OUTPUT_PATH = Path("sql_summary.json")


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


async def main():
    db_url = require_env("TURSO_DB_URL").replace("libsql://", "https://")
    token = require_env("TURSO_AUTH_TOKEN")

    db = libsql_client.create_client(url=db_url, auth_token=token)

    print("Computing SQL summary...")

    summary = {}
    summary["generated_at"] = datetime.utcnow().isoformat()

    # --------------------------------------------------
    # 1) Distinct external_ids
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT external_id) AS n FROM articles")
    summary["distinct_external_ids"] = rs.rows[0]["n"]

    # --------------------------------------------------
    # 2) Distinct sources
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT source) AS n FROM articles")
    summary["distinct_sources"] = rs.rows[0]["n"]

    # --------------------------------------------------
    # 3) Count per source
    # --------------------------------------------------
    rs = await db.execute("""
        SELECT source, COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY source
        ORDER BY n DESC
    """)

    per_source = {}
    for row in rs.rows:
        per_source[row["source"] or "—"] = row["n"]

    summary["per_source"] = per_source

    # --------------------------------------------------
    # 4) Per ressort
    # --------------------------------------------------
    rs = await db.execute("""
        SELECT ressort, COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY ressort
        ORDER BY n DESC
    """)

    per_ressort = {}
    for row in rs.rows:
        per_ressort[row["ressort"] or "NULL"] = row["n"]

    summary["per_ressort"] = per_ressort

    # --------------------------------------------------
    # 5) Per region_by_url / subregion
    # --------------------------------------------------
    rs = await db.execute("""
        SELECT
            region_by_url,
            subregion_by_url AS subregion,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_url, subregion_by_url
        ORDER BY n DESC
    """)

    per_region_url = []
    for row in rs.rows:
        per_region_url.append({
            "region_by_url": row["region_by_url"] or "—",
            "subregion_by_url": row["subregion"] or "—",
            "count": row["n"],
        })

    summary["per_region_by_url"] = per_region_url

    # --------------------------------------------------
    # 6) Per region_by_source
    # --------------------------------------------------
    rs = await db.execute("""
        SELECT
            region_by_source,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_source
        ORDER BY n DESC
    """)

    per_region_by_source = {}
    for row in rs.rows:
        per_region_by_source[row["region_by_source"] or "NULL"] = row["n"]

    summary["per_region_by_source"] = per_region_by_source

    # --------------------------------------------------
    # Write JSON
    # --------------------------------------------------
    OUTPUT_PATH.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False)
    )

    print("SQL summary written to:", OUTPUT_PATH.resolve())

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
