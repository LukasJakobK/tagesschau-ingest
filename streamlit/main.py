import streamlit as st
import os

from db.filters import build_where
from ui.ui import render_ui

st.set_page_config(layout="wide")
st.title("🧱 News SQL Filter Builder")

# render_ui initialisiert den State selbst
where_clause = build_where(st.session_state.get("filters", []))

generated_sql = f"""
SELECT *
FROM articles
WHERE {where_clause}
LIMIT 100
""".strip()

render_ui(generated_sql)
