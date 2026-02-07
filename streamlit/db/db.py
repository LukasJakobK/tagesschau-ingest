import os
import asyncio
import pandas as pd
import libsql_client
from dotenv import load_dotenv

# Load .env once, early
load_dotenv()

def _get_db_config() -> tuple[str, str]:
    """
    Read env vars lazily. This avoids crashing at import time.
    """
    url = os.environ.get("TURSO_DB_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        raise RuntimeError(
            "Missing TURSO_DB_URL or TURSO_AUTH_TOKEN. "
            "Ensure they are set in the environment or in a .env file in the working directory."
        )

    url = url.replace("libsql://", "https://")
    return url, token


async def _run_query_async(sql: str) -> pd.DataFrame:
    url, token = _get_db_config()
    db = libsql_client.create_client(url=url, auth_token=token)
    rs = await db.execute(sql)
    rows = rs.rows
    await db.close()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def run_query(sql: str) -> pd.DataFrame:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        return loop.run_until_complete(_run_query_async(sql))
    return asyncio.run(_run_query_async(sql))


def get_distinct_values(column_sql: str, limit: int = 500) -> list[str]:
    sql = f"""
    SELECT DISTINCT {column_sql} AS val
    FROM articles
    WHERE {column_sql} IS NOT NULL
      AND TRIM({column_sql}) != ''
    ORDER BY val
    LIMIT {limit}
    """
    df = run_query(sql)
    if df.empty or "val" not in df.columns:
        return []
    return [str(x) for x in df["val"].tolist() if x is not None and str(x).strip() != ""]


def get_min_max_published_at() -> tuple[str | None, str | None]:
    sql = """
    SELECT
        SUBSTR(MIN(published_at), 1, 10) AS min_date,
        SUBSTR(MAX(published_at), 1, 10) AS max_date
    FROM articles
    WHERE published_at IS NOT NULL
    """
    df = run_query(sql)
    if df.empty:
        return None, None
    row = df.iloc[0]
    return (row["min_date"], row["max_date"])


def get_subregions_for_region(region_value: str, limit: int = 500) -> list[str]:
    region_value = str(region_value).replace("'", "''")
    sql = f"""
    SELECT DISTINCT subregion_by_url AS val
    FROM articles
    WHERE subregion_by_url IS NOT NULL
      AND TRIM(subregion_by_url) != ''
      AND region_by_url = '{region_value}'
    ORDER BY val
    LIMIT {limit}
    """
    df = run_query(sql)
    if df.empty or "val" not in df.columns:
        return []
    return [str(x) for x in df["val"].tolist() if x is not None and str(x).strip() != ""]
