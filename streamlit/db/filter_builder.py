import os
import asyncio
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import libsql_client

# ----------------------------------
# Env & DB
# ----------------------------------

load_dotenv()

TURSO_DB_URL = os.environ.get("TURSO_DB_URL")
TURSO_AUTH_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

if not TURSO_DB_URL or not TURSO_AUTH_TOKEN:
    st.error("❌ TURSO_DB_URL or TURSO_AUTH_TOKEN not set in environment")
    st.stop()

TURSO_DB_URL = TURSO_DB_URL.replace("libsql://", "https://")

# ----------------------------------
# Async DB helpers
# ----------------------------------

async def _run_query_async(sql: str) -> pd.DataFrame:
    db = libsql_client.create_client(
        url=TURSO_DB_URL,
        auth_token=TURSO_AUTH_TOKEN,
    )
    rs = await db.execute(sql)
    rows = rs.rows
    await db.close()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        return loop.run_until_complete(_run_query_async(sql))
    else:
        return asyncio.run(_run_query_async(sql))


# ----------------------------------
# Load filter values from DB
# ----------------------------------

@st.cache_data(show_spinner=True)
def load_filter_values():
    sql = """
    SELECT
        DISTINCT
        COALESCE(region_by_api, region_by_source, region_by_url) AS region,
        subregion_by_url AS subregion,
        source,
        ressort
    FROM articles
    WHERE fulltext IS NOT NULL
    """
    df = run_query(sql)
    return df


df_filters = load_filter_values()

# Clean lists
regions = sorted([r for r in df_filters["region"].dropna().unique().tolist() if r])
sources = sorted([r for r in df_filters["source"].dropna().unique().tolist() if r])
ressorts = sorted([r for r in df_filters["ressort"].dropna().unique().tolist() if r])

# ----------------------------------
# Streamlit UI
# ----------------------------------

st.set_page_config(layout="wide")
st.title("🧱 News SQL Filter Builder")

# ----------------------------------
# Session state
# ----------------------------------

if "filters" not in st.session_state:
    st.session_state.filters = []

if "sql_override" not in st.session_state:
    st.session_state.sql_override = ""

# ----------------------------------
# Structured Dropdown Filters
# ----------------------------------

st.subheader("🧭 Structured Filters (from DB)")

c1, c2, c3, c4 = st.columns(4)

with c1:
    sel_region = st.selectbox("Region", [""] + regions)

with c2:
    if sel_region:
        subregions = (
            df_filters[df_filters["region"] == sel_region]["subregion"]
            .dropna()
            .unique()
            .tolist()
        )
        subregions = sorted([s for s in subregions if s])
    else:
        subregions = []

    sel_subregion = st.selectbox("Subregion", [""] + subregions)

with c3:
    sel_source = st.selectbox("Source", [""] + sources)

with c4:
    sel_ressort = st.selectbox("Ressort", [""] + ressorts)

# Button to apply structured filters
if st.button("➕ Apply structured filters"):
    if sel_region:
        st.session_state.filters.append(
            {"field": "region", "operator": "=", "value": sel_region}
        )
    if sel_subregion:
        # subregion is directly in DB, but not in FIELD_SQL yet → add it
        st.session_state.filters.append(
            {
                "field": "subregion_by_url",
                "operator": "=",
                "value": sel_subregion,
            }
        )
    if sel_source:
        st.session_state.filters.append(
            {"field": "source", "operator": "=", "value": sel_source}
        )
    if sel_ressort:
        st.session_state.filters.append(
            {"field": "ressort", "operator": "=", "value": sel_ressort}
        )

    st.rerun()

# ----------------------------------
# Available fields & operators
# ----------------------------------

FIELDS = {
    "source": ["=", "!="],
    "ressort": ["=", "!="],
    "published_at": [">=", "<=", "BETWEEN"],
    "region": ["="],
    "title": ["LIKE"],
    "url": ["LIKE"],
    "subregion_by_url": ["="],
}

FIELD_SQL = {
    "source": "source",
    "ressort": "ressort",
    "published_at": "published_at",
    "region": "COALESCE(region_by_api, region_by_source, region_by_url)",
    "title": "title",
    "url": "url",
    "subregion_by_url": "subregion_by_url",
}

# ----------------------------------
# Manual Filter Builder
# ----------------------------------

st.subheader("➕ Add Manual Filter")

col1, col2, col3, col4 = st.columns([2, 2, 3, 1])

with col1:
    field = st.selectbox("Field", list(FIELDS.keys()))

with col2:
    operator = st.selectbox("Operator", FIELDS[field])

with col3:
    value = st.text_input("Value", placeholder="e.g. DE or 2026-01-01,2026-01-31")

with col4:
    if st.button("Add"):
        if value.strip():
            st.session_state.filters.append(
                {
                    "field": field,
                    "operator": operator,
                    "value": value.strip(),
                }
            )
        else:
            st.warning("Value must not be empty.")

# ----------------------------------
# Show active filters
# ----------------------------------

st.subheader("🧩 Active Filters")

if not st.session_state.filters:
    st.info("No filters yet.")
else:
    for i, f in enumerate(st.session_state.filters):
        c1, c2, c3, c4, c5 = st.columns([2, 2, 3, 1, 1])

        with c1:
            st.write(f["field"])
        with c2:
            st.write(f["operator"])
        with c3:
            st.write(f["value"])
        with c4:
            if st.button("⬆️", key=f"up_{i}") and i > 0:
                st.session_state.filters[i - 1], st.session_state.filters[i] = (
                    st.session_state.filters[i],
                    st.session_state.filters[i - 1],
                )
                st.rerun()
        with c5:
            if st.button("❌", key=f"del_{i}"):
                st.session_state.filters.pop(i)
                st.rerun()

# ----------------------------------
# Build SQL WHERE
# ----------------------------------

def build_where(filters: list[dict]) -> str:
    clauses = []

    for f in filters:
        col = FIELD_SQL[f["field"]]
        op = f["operator"]
        val = f["value"].replace("'", "''")

        if op == "LIKE":
            clauses.append(f"{col} LIKE '%{val}%'")

        elif op == "BETWEEN":
            parts = [p.strip() for p in val.split(",")]
            if len(parts) == 2:
                clauses.append(f"{col} BETWEEN '{parts[0]}' AND '{parts[1]}'")

        else:
            clauses.append(f"{col} {op} '{val}'")

    if not clauses:
        return "1=1"

    return " AND ".join(clauses)


where_clause = build_where(st.session_state.filters)

# ----------------------------------
# SQL Generator
# ----------------------------------

generated_sql = f"""
SELECT *
FROM articles
WHERE {where_clause}
LIMIT 100
""".strip()

# ----------------------------------
# SQL Editor
# ----------------------------------

st.subheader("🧾 SQL Editor")

if not st.session_state.sql_override:
    st.session_state.sql_override = generated_sql

sql_editor = st.text_area(
    "✍️ Edit SQL (this will be executed)",
    value=st.session_state.sql_override,
    height=200,
)

st.session_state.sql_override = sql_editor

c1, c2 = st.columns([1, 3])

with c1:
    if st.button("🔁 Reset to generated SQL"):
        st.session_state.sql_override = generated_sql
        st.rerun()

with c2:
    st.caption("⚠️ The SQL above is what will be executed on the DB")

sql_to_run = st.session_state.sql_override.strip() or generated_sql

# ----------------------------------
# Run Query
# ----------------------------------

st.subheader("▶️ Run Query")

if st.button("🚀 Execute on DB"):
    with st.spinner("Querying Turso..."):
        try:
            df = run_query(sql_to_run)
            st.success(f"✅ Returned {len(df)} rows")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error("❌ Query failed")
            st.exception(e)
