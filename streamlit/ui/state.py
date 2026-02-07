import streamlit as st

def init_state():
    if "filters" not in st.session_state:
        st.session_state.filters = []

    if "sql_override" not in st.session_state:
        st.session_state.sql_override = ""

    if "last_generated_sql" not in st.session_state:
        st.session_state.last_generated_sql = ""
