import pandas as pd
import streamlit as st
from db.db import run_query


def plot_articles_over_time(
    *,
    sql: str,
    enabled: bool = True,
    chart_type: str = "line",
    granularity: str = "day",
    show_trend: bool = False,
):
    if not enabled:
        st.info("Plot disabled.")
        return

    # --- Build aggregation SQL ---
    if granularity == "day":
        date_expr = "date(published_at)"
    elif granularity == "week":
        date_expr = "strftime('%Y-%W', published_at)"
    elif granularity == "month":
        date_expr = "strftime('%Y-%m', published_at)"
    else:
        date_expr = "date(published_at)"

    agg_sql = f"""
    SELECT
        {date_expr} AS period,
        COUNT(*) AS cnt
    FROM ({sql})
    GROUP BY period
    ORDER BY period
    """

    df = run_query(agg_sql)

    if df.empty:
        st.warning("No data for plot.")
        return

    df["period"] = df["period"].astype(str)
    df["cnt"] = df["cnt"].astype(int)
    df = df.set_index("period")

    # --- Trend (moving average) ---
    if show_trend:
        if granularity == "day":
            window = 7
        elif granularity == "week":
            window = 4
        else:
            window = 3

        df["trend"] = df["cnt"].rolling(window=window, min_periods=1).mean()

    # --- Plot ---
    st.subheader("📈 Articles over time")

    if chart_type == "bar":
        st.bar_chart(df[["cnt"]] if not show_trend else df[["cnt", "trend"]])
    else:
        st.line_chart(df[["cnt"]] if not show_trend else df[["cnt", "trend"]])
