# ui/ui.py
import streamlit as st
from datetime import date
import os
import json

from llm.rag_runner import run_rag_sync
from ui.plotting import plot_articles_over_time
from db.filters import FIELD_NAMES, FIELD_SQL, get_operators, get_ui_type
from db.db import (
    run_query,
    get_distinct_values,
    get_min_max_published_at,
    get_subregions_for_region,
)

# -------------------------------------------------
# Config
# -------------------------------------------------
SCHEMA_PATH = "config/article_schema.json"
PROMPTS_PATH = "config/prompts.json"
TOPICS_PATH = "config/topics.json"


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -------------------------------------------------
# State
# -------------------------------------------------
def init_state(generated_sql: str):
    defaults = dict(
        filters=[],
        current_region_for_subregion=None,
        sql_override=generated_sql,
        sql_is_manual=False,
        sql_editor_version=0,
        last_df=None,
        rag_result=None,
    )
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)


# -------------------------------------------------
# Commands (Callbacks)
# -------------------------------------------------
def cmd_add_filter(field, operator, value):
    if str(value).strip():
        st.session_state.filters.append(
            dict(field=field, operator=operator, value=str(value))
        )


def cmd_delete_filter(idx: int):
    st.session_state.filters.pop(idx)


def cmd_run_db(sql: str):
    st.session_state.last_df = run_query(sql)
    st.session_state.rag_result = None


def cmd_run_rag(df, *, schema, prompts, query, topic, top_k, max_docs):
    st.session_state.rag_result = run_rag_sync(
        df=df.head(int(max_docs)),
        schema=schema,
        prompts=prompts,
        query=query,
        topic=topic,
        openai_key=os.environ["OPEN_AI_KEY"],
        qdrant_url=os.environ["QADRANT_ENDPOINT"],
        qdrant_api_key=os.environ["QADRANT_API_KEY"],
        top_k=int(top_k),
        max_docs_to_index=int(max_docs),
    )


def cmd_reset_sql(generated_sql: str):
    st.session_state.sql_override = generated_sql
    st.session_state.sql_is_manual = False
    st.session_state.sql_editor_version += 1


# -------------------------------------------------
# Header
# -------------------------------------------------
def render_header():
    st.title("🧠 News Analyst")
    min_d, max_d = get_min_max_published_at()
    c1, c2 = st.columns(2)
    c1.metric("Earliest", min_d or "n/a")
    c2.metric("Latest", max_d or "n/a")
    st.divider()


# -------------------------------------------------
# Filter value renderers
# -------------------------------------------------
def render_dropdown(field, *_):
    values = get_distinct_values(FIELD_SQL[field])
    return st.selectbox("Value", values) if values else ""


def render_region(field, *_):
    values = get_distinct_values(FIELD_SQL["region_by_url"])
    value = st.selectbox("Value", values) if values else ""
    if value:
        st.session_state.current_region_for_subregion = value
    return value


def render_subregion(field, *_):
    region = st.session_state.current_region_for_subregion
    if not region:
        st.info("Select region first")
        return ""
    values = get_subregions_for_region(region)
    return st.selectbox("Value", values) if values else ""


def render_date(_, operator):
    min_d, max_d = get_min_max_published_at()
    min_d, max_d = map(date.fromisoformat, (min_d, max_d))

    if operator == "BETWEEN":
        start, end = st.date_input("Date range", (min_d, max_d))
        return f"{start},{end}"

    return st.date_input("Date", min_d).isoformat()


def render_text(*_):
    return st.text_input("Value")


VALUE_RENDERERS = {
    "dropdown": render_dropdown,
    "region_by_url": render_region,
    "subregion_dropdown": render_subregion,
    "date": render_date,
    "text": render_text,
}


# -------------------------------------------------
# Filters
# -------------------------------------------------
def render_filters():
    st.subheader("🧱 Filters")

    field = st.selectbox("Field", FIELD_NAMES)
    operator = st.selectbox("Operator", get_operators(field))

    ui_type = get_ui_type(field)
    renderer = VALUE_RENDERERS.get(ui_type, render_text)
    value = renderer(field, operator)

    st.button(
        "➕ Add filter",
        use_container_width=True,
        on_click=cmd_add_filter,
        args=(field, operator, value),
    )

    st.divider()
    st.subheader("🧩 Active filters")

    for i, f in enumerate(st.session_state.filters):
        cols = st.columns([6, 1])
        cols[0].write(f"`{f['field']}` {f['operator']} **{f['value']}**")
        cols[1].button("❌", key=f"del_{i}", on_click=cmd_delete_filter, args=(i,))


# -------------------------------------------------
# SQL Editor
# -------------------------------------------------
def render_sql_editor(generated_sql: str) -> str:
    st.subheader("🧾 Query & Results")

    with st.expander("SQL (advanced)", expanded=False):
        st.button(
            "Reset SQL",
            use_container_width=True,
            on_click=cmd_reset_sql,
            args=(generated_sql,),
        )

        if not st.session_state.sql_is_manual:
            st.session_state.sql_override = generated_sql

        sql = st.text_area(
            "SQL",
            st.session_state.sql_override,
            height=180,
            key=f"sql_editor_{st.session_state.sql_editor_version}",
        )

        if sql != st.session_state.sql_override:
            st.session_state.sql_override = sql
            st.session_state.sql_is_manual = True

    return st.session_state.sql_override.strip() or generated_sql


# -------------------------------------------------
# Plot
# -------------------------------------------------
def render_plot_tab(sql: str):
    c1, c2, c3 = st.columns(3)
    with c1:
        chart_type = st.selectbox("Chart type", ["line", "bar"])
    with c2:
        granularity = st.selectbox("Granularity", ["day", "week", "month"])
    with c3:
        show_trend = st.checkbox("Show trend")

    plot_articles_over_time(
        sql=sql,
        enabled=True,
        chart_type=chart_type,
        granularity=granularity,
        show_trend=show_trend,
    )


# -------------------------------------------------
# RAG
# -------------------------------------------------
def render_rag_tab(df):
    prompts = load_json(PROMPTS_PATH)
    topics_cfg = load_json(TOPICS_PATH)
    schema = load_json(SCHEMA_PATH)

    prompt_key = st.selectbox("Prompt template", list(prompts.keys()))
    prompt_cfg = prompts[prompt_key]

    # Topic category
    topic_keys = list(topics_cfg.keys())
    topic_labels = [topics_cfg[k]["label"] for k in topic_keys]
    topic_idx = st.selectbox(
        "Topic category",
        range(len(topic_keys)),
        format_func=lambda i: topic_labels[i],
    )
    topic_meta = topics_cfg[topic_keys[topic_idx]]

    default_topic = topic_meta.get("default_topic", "")

    manual_topic = st.text_input(
        "Topic (optional – überschreibt Default)",
        value="",
    ).strip()

    is_manual_override = bool(manual_topic)
    final_topic = manual_topic if is_manual_override else default_topic

    if is_manual_override:
        st.warning(
            f"✏️ Manuelles Topic aktiv – Default wird überschrieben:\n\n**{final_topic}**"
        )
    else:
        st.info(
            f"📌 Standard-Topic aus Kategorie wird verwendet:\n\n**{final_topic}**"
        )

    question = st.text_area(
        "Question",
        prompt_cfg.get("default_question", ""),
        height=120,
    ).strip()

    c1, c2 = st.columns(2)
    with c1:
        top_k = st.number_input("Top-K", 1, 20, 5)
    with c2:
        max_docs = st.number_input("Max docs", 50, 5000, 2000)

    st.button(
        "🧠 Run RAG",
        use_container_width=True,
        on_click=cmd_run_rag,
        kwargs=dict(
            df=df,
            schema=schema,
            prompts=prompts,
            query=question,
            topic=final_topic,
            top_k=int(top_k),
            max_docs=int(max_docs),
        ),
    )

    render_rag_result()


def render_rag_result():
    rag = st.session_state.rag_result
    if not rag:
        return

    st.markdown("### Summary")
    st.write(rag.get("summary", ""))

    if rag.get("claims"):
        st.markdown("### Claims & Sources")
        for c in rag["claims"]:
            st.write(f"- {c.get('text','')} {c.get('sources', [])}")

    docs = rag.get("docs", [])
    if docs:
        with st.expander("Top documents"):
            for i, d in enumerate(docs, 1):
                st.markdown(f"**[{i}] {d.get('title','')}**")
                st.caption(f"{d.get('published_at','')} • {d.get('url','')}")
                txt = d.get("text") or ""
                st.write(txt[:800] + ("..." if len(txt) > 800 else ""))

    with st.expander("Raw model output"):
        st.code(rag.get("raw", ""), language="text")


# -------------------------------------------------
# Main
# -------------------------------------------------
def render_ui(generated_sql: str) -> str:
    render_header()
    init_state(generated_sql)

    left, right = st.columns([1, 2], gap="large")

    with left:
        render_filters()

    with right:
        sql = render_sql_editor(generated_sql)

        st.button(
            "🚀 Run on DB",
            use_container_width=True,
            on_click=cmd_run_db,
            args=(sql,),
        )

        if st.session_state.last_df is not None:
            df = st.session_state.last_df
            tab1, tab2, tab3 = st.tabs(["📄 Table", "📈 Plot", "🧠 LLM (RAG)"])

            with tab1:
                st.dataframe(df, use_container_width=True)

            with tab2:
                render_plot_tab(sql)

            with tab3:
                render_rag_tab(df)

    return sql
