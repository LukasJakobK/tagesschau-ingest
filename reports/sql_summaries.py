import os
import asyncio
import json
from pathlib import Path
import libsql_client


CACHE_PATH = Path(".cache/sql_summary.json")
CACHE_PATH.parent.mkdir(exist_ok=True)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def fmt(n):
    if n is None:
        return "—"
    if isinstance(n, int):
        return f"{n:,}".replace(",", ".")
    return str(n)


async def main():
    db_url = require_env("TURSO_DB_URL").replace("libsql://", "https://")
    token = require_env("TURSO_AUTH_TOKEN")

    db = libsql_client.create_client(url=db_url, auth_token=token)

    # --------------------------------------------------
    # 0) Check max ingest_date first (cheap query)
    # --------------------------------------------------
    rs = await db.execute("SELECT MAX(ingest_date) AS d FROM articles")
    max_ingest_date = rs.rows[0]["d"]

    # --------------------------------------------------
    # 1) If cache exists and still valid → print and exit
    # --------------------------------------------------
    if CACHE_PATH.exists():
        cached = json.loads(CACHE_PATH.read_text())
        if cached.get("max_ingest_date") == max_ingest_date:
            print("\n==============================")
            print(" TAGESSCHAU SQL SUMMARY (CACHED) ")
            print("==============================\n")
            print(cached["text"])
            await db.close()
            return

    # --------------------------------------------------
    # 2) Otherwise: recompute everything
    # --------------------------------------------------
    lines = []

    def out(s=""):
        print(s)
        lines.append(s)

    out("\n==============================")
    out(" TAGESSCHAU SQL SUMMARY ")
    out("==============================\n")

    # --------------------------------------------------
    # 1) Distinct external_ids
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT external_id) AS n FROM articles")
    n_distinct_external_ids = rs.rows[0]["n"]
    out(f"Distinct external_ids: {fmt(n_distinct_external_ids)}")

    # --------------------------------------------------
    # 2) Distinct sources
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT source) AS n FROM articles")
    n_sources = rs.rows[0]["n"]
    out(f"Distinct sources:      {fmt(n_sources)}")

    # --------------------------------------------------
    # 3) Count per source (distinct external_ids)
    # --------------------------------------------------
    out("\n--- Distinct external_ids per source ---")
    rs = await db.execute("""
        SELECT
            source,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY source
        ORDER BY n DESC
    """)

    for row in rs.rows:
        src = row["source"] or "—"
        n = row["n"]
        out(f"{src:<20} {fmt(n)}")

        # Debug drilldown for suspicious small counts
        if n <= 2:
            rs2 = await db.execute("""
                SELECT
                    external_id,
                    title,
                    published_at,
                    url,
                    ingest_date
                FROM articles
                WHERE source = ?
            """, [src])

            for r in rs2.rows:
                out(f"    → external_id={r['external_id']}")
                out(f"      title={r['title']}")
                out(f"      published_at={r['published_at']}")
                out(f"      url={r['url']}")
                out(f"      ingest_date={r['ingest_date']}")


    # --------------------------------------------------
    # 4) Distinct ressorts + count per ressort (distinct external_ids)
    # --------------------------------------------------
    out("\n--- Distinct external_ids per ressort ---")
    rs = await db.execute("""
        SELECT
            ressort,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY ressort
        ORDER BY n DESC
    """)
    for row in rs.rows:
        res = row["ressort"] or "NULL"
        out(f"{res:<30} {fmt(row['n'])}")

    # --------------------------------------------------
    # 5) Distinct count per region_by_url + subregion_by_url
    # --------------------------------------------------
    out("\n--- Distinct external_ids per region_by_url / subregion_by_url ---")
    rs = await db.execute("""
        SELECT
            region_by_url,
            subregion_by_url AS subregion,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_url, subregion_by_url
        ORDER BY n DESC
    """)
    for row in rs.rows:
        region = row["region_by_url"] or "—"
        subregion = row["subregion"] or "—"
        out(f"{region:<25} {subregion:<25} {fmt(row['n'])}")

    # --------------------------------------------------
    # 6) Distinct count per region_by_source (based on distinct external_ids)
    # --------------------------------------------------
    out("\n--- Distinct external_ids per region_by_source ---")
    rs = await db.execute("""
        SELECT
            region_by_source,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_source
        ORDER BY n DESC
    """)
    for row in rs.rows:
        rbs = row["region_by_source"] or "NULL"
        out(f"{rbs:<30} {fmt(row['n'])}")

    # --------------------------------------------------
    # 7) Max ingest_date
    # --------------------------------------------------
    out(f"\nMax ingest_date: {max_ingest_date}")

    out("\n==============================")
    out(" END SUMMARY ")
    out("==============================\n")

    # --------------------------------------------------
    # 3) Write cache
    # --------------------------------------------------
    CACHE_PATH.write_text(json.dumps(
        {
            "max_ingest_date": max_ingest_date,
            "text": "\n".join(lines),
        },
        indent=2,
        ensure_ascii=False,
    ))

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
