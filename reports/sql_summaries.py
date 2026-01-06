import os
import asyncio
import libsql_client


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

    print("\n==============================")
    print(" TAGESSCHAU SQL SUMMARY ")
    print("==============================\n")

    # --------------------------------------------------
    # 1) Distinct external_ids
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT external_id) AS n FROM articles")
    n_distinct_external_ids = rs.rows[0]["n"]
    print(f"Distinct external_ids: {fmt(n_distinct_external_ids)}")

    # --------------------------------------------------
    # 2) Distinct sources
    # --------------------------------------------------
    rs = await db.execute("SELECT COUNT(DISTINCT source) AS n FROM articles")
    n_sources = rs.rows[0]["n"]
    print(f"Distinct sources:      {fmt(n_sources)}")

    # --------------------------------------------------
    # 3) Count per source (distinct external_ids)
    # --------------------------------------------------
    print("\n--- Distinct external_ids per source ---")
    rs = await db.execute("""
        SELECT
            source,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY source
        ORDER BY n DESC
    """)
    for row in rs.rows:
        print(f"{row['source']:<20} {fmt(row['n'])}")

    # --------------------------------------------------
    # 4) Distinct ressorts + count per ressort (distinct external_ids)
    # --------------------------------------------------
    print("\n--- Distinct external_ids per ressort ---")
    rs = await db.execute("""
        SELECT
            ressort,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY ressort
        ORDER BY n DESC
    """)
    for row in rs.rows:
        print(f"{(row['ressort'] or 'NULL'):<30} {fmt(row['n'])}")

    # --------------------------------------------------
    # 5) Distinct count per region_by_url + subregion_by_url
    # --------------------------------------------------
    print("\n--- Distinct external_ids per region_by_url / subregion_by_url ---")
    rs = await db.execute("""
        SELECT
            region_by_url,
            COALESCE(subregion_by_url, '—') AS subregion,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_url, subregion_by_url
        ORDER BY n DESC
    """)
    for row in rs.rows:
        print(f"{row['region_by_url']:<25} {row['subregion']:<25} {fmt(row['n'])}")

    # --------------------------------------------------
    # 6) Distinct count per region_by_source (based on distinct external_ids)
    # --------------------------------------------------
    print("\n--- Distinct external_ids per region_by_source ---")
    rs = await db.execute("""
        SELECT
            region_by_source,
            COUNT(DISTINCT external_id) AS n
        FROM articles
        GROUP BY region_by_source
        ORDER BY n DESC
    """)
    for row in rs.rows:
        print(f"{(row['region_by_source'] or 'NULL'):<30} {fmt(row['n'])}")

    # --------------------------------------------------
    # 7) Max ingest_date
    # --------------------------------------------------
    rs = await db.execute("SELECT MAX(ingest_date) AS d FROM articles")
    max_ingest_date = rs.rows[0]["d"]
    print("\nMax ingest_date:", max_ingest_date)

    await db.close()

    print("\n==============================")
    print(" END SUMMARY ")
    print("==============================\n")


if __name__ == "__main__":
    asyncio.run(main())
