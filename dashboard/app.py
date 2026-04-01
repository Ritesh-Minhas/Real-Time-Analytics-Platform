import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB = os.getenv("PG_DB", "rt_analytics")
PG_USER = os.getenv("PG_USER", "rt_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rt_pass")

ENGINE = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

st.set_page_config(page_title="Real-Time Analytics Platform", layout="wide")

# auto refresh every 5 seconds
st.caption("Auto-refresh: 5s")
st.query_params["refresh"] = "5"
st.autorefresh = st.experimental_rerun  # placeholder guard for older versions

refresh = st.empty()
# Streamlit >= 1.18 has st.experimental_rerun; use st.rerun if present
try:
    from streamlit_autorefresh import st_autorefresh  # optional
except Exception:
    st_autorefresh = None

if st_autorefresh:
    st_autorefresh(interval=5000, key="rt_refresh")
else:
    # lightweight manual refresh button
    if st.button("Refresh now"):
        st.rerun()

st.title("📈 Real-Time Data Analytics Platform")
st.write("Sources: **Stock simulator** + **Social trend simulator**")

# ---- helpers ----
def read_df(sql: str, params=None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# ---- KPIs ----
kpi_sql = """
SELECT
  (SELECT COUNT(*) FROM bronze_raw_events WHERE ingest_ts >= NOW() - INTERVAL '5 minutes') AS bronze_5m,
  (SELECT COUNT(*) FROM silver_clean_events WHERE ingest_ts >= NOW() - INTERVAL '5 minutes') AS silver_5m,
  (SELECT COUNT(*) FROM gold_minute_aggregates WHERE window_start >= NOW() - INTERVAL '60 minutes') AS gold_60m
"""
kpis = read_df(kpi_sql).iloc[0]

c1, c2, c3 = st.columns(3)
c1.metric("Bronze events (last 5m)", int(kpis["bronze_5m"]))
c2.metric("Silver events (last 5m)", int(kpis["silver_5m"]))
c3.metric("Gold rows (last 60m)", int(kpis["gold_60m"]))

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(
    ["Overview", "Stocks", "Social Trends", "Data Quality", "Alerts", "Evaluation", "Batch vs Stream", "System Health"]
)

with tab1:
    st.subheader("Latest Gold Aggregates (last 15 minutes)")
    df = read_df("""
        SELECT window_start, source, key, count, avg_value, min_value, max_value
        FROM gold_minute_aggregates
        WHERE window_start >= NOW() - INTERVAL '15 minutes'
        ORDER BY window_start DESC, source, key
        LIMIT 200
    """)
    st.dataframe(df, use_container_width=True)

with tab2:
    st.subheader("Stock: last 60 minutes (per symbol)")
    sym = st.selectbox("Select symbol", ["AAPL","MSFT","GOOGL","AMZN","TSLA","NVDA"], index=0)

    df = read_df("""
        SELECT window_start, count, avg_value, min_value, max_value
        FROM gold_minute_aggregates
        WHERE source='stock'
          AND key=:sym
          AND window_start >= NOW() - INTERVAL '60 minutes'
        ORDER BY window_start
    """, {"sym": sym})

    if df.empty:
        st.info("No data yet. Keep producers + processors running.")
    else:
        st.line_chart(df.set_index("window_start")[["avg_value"]])
        st.area_chart(df.set_index("window_start")[["count"]])
        st.dataframe(df.tail(20), use_container_width=True)

with tab3:
    st.subheader("Social: last 60 minutes (per topic)")
    topic = st.selectbox("Select topic", ["AI","Python","Cricket","Bollywood","Elections","Crypto","Space","Climate","Gaming","Startups"], index=0)

    df = read_df("""
        SELECT window_start, count, avg_value AS avg_mentions, min_value, max_value
        FROM gold_minute_aggregates
        WHERE source='social'
          AND key=:topic
          AND window_start >= NOW() - INTERVAL '60 minutes'
        ORDER BY window_start
    """, {"topic": topic})

    if df.empty:
        st.info("No data yet. Keep producers + processors running.")
    else:
        st.line_chart(df.set_index("window_start")[["avg_mentions"]])
        st.area_chart(df.set_index("window_start")[["count"]])
        st.dataframe(df.tail(20), use_container_width=True)


with tab4:
    st.subheader("DQ Health Score (0–100)")

    health = read_df("""
        SELECT source, health_score_0_100, pass_checks, warn_checks, fail_checks, last_run_ts
        FROM dq_source_health
        ORDER BY source;
    """)

    if health.empty:
        st.info("No DQ health yet. Ensure dq_runner is running and dq_results has data.")
    else:
        # Show metrics per source
        cols = st.columns(len(health))
        for i, row in enumerate(health.itertuples(index=False)):
            label = f"{row.source.upper()} Health"
            value = int(row.health_score_0_100)
            cols[i].metric(
                label,
                f"{value}/100",
                f"PASS {row.pass_checks} | WARN {row.warn_checks} | FAIL {row.fail_checks}"
            )

        st.dataframe(health, use_container_width=True)

    st.divider()

    st.subheader("Most recent FAIL/WARN checks")
    issues = read_df("""
        SELECT source, check_name, status, total_rows, failed_rows, created_ts
        FROM dq_latest_scored
        WHERE status IN ('FAIL', 'WARN')
        ORDER BY created_ts DESC, source, check_name
        LIMIT 50;
    """)
    if issues.empty:
        st.success("No WARN/FAIL checks right now ✅")
    else:
        st.dataframe(issues, use_container_width=True)

    st.divider()


    st.subheader("Data Quality Status (latest per source)")
    df = read_df("""
        WITH latest AS (
          SELECT DISTINCT ON (source, check_name)
            source, check_name, status, total_rows, failed_rows, details_json, created_ts
          FROM dq_results
          ORDER BY source, check_name, created_ts DESC
        )
        SELECT * FROM latest
        ORDER BY source, check_name;
    """)
    if df.empty:
        st.info("No DQ results yet. Run: python -m services.processing.dq_runner")
    else:
        st.dataframe(df, use_container_width=True)

    st.subheader("DQ Run History (last 100)")
    hist = read_df("""
        SELECT source, check_name, status, total_rows, failed_rows, created_ts
        FROM dq_results
        ORDER BY created_ts DESC
        LIMIT 100;
    """)
    st.dataframe(hist, use_container_width=True)

with tab5:
    st.subheader("Real-Time Anomaly Alerts")

    # ---- Filters ----
    c1, c2, c3, c4 = st.columns(4)

    source = c1.selectbox(
        "Source",
        ["all", "stock", "social"],
        index=0,
        key="tab5_source"
    )

    min_score = c2.slider(
        "Minimum anomaly score",
        0.0,
        10.0,
        3.5,
        0.5,
        key="tab5_min_score"
    )

    limit = c3.selectbox(
        "Rows",
        [50, 100, 200, 500],
        index=1,
        key="tab5_limit"
    )

    method = c4.selectbox(
        "Detection Method",
        ["all", "EWMA_Z", "IFOR"],
        index=0,
        key="tab5_method"
    )

    # ---- Build SQL filters ----
    where = "WHERE score >= :min_score"
    params = {
        "min_score": float(min_score),
        "limit": int(limit)
    }

    if source != "all":
        where += " AND source = :source"
        params["source"] = source

    if method != "all":
        where += " AND method = :method"
        params["method"] = method

    # ---- Fetch alerts ----
    alerts = read_df(f"""
        SELECT
            alert_id,
            created_ts,
            source,
            key,
            method,
            model_version,
            metric_name,
            anomaly_type,
            score,
            actual_value,
            expected_value,
            event_ts,
            details_json
        FROM alerts_anomalies
        {where}
        ORDER BY created_ts DESC
        LIMIT :limit
    """, params)

    # ---- Display Alerts ----
    if alerts.empty:
        st.info("No alerts yet. Let the system run for a few minutes or inject an anomaly for demo.")
    else:
        st.dataframe(alerts, use_container_width=True)

        # -----------------------------
        # Root Cause Explanation Viewer
        # -----------------------------
        st.divider()
        st.subheader("Root Cause Explanation")

        selected_alert = st.selectbox(
            "Select alert",
            alerts["alert_id"],
            key="tab5_selected_alert"
        )

        alert_row = alerts[alerts["alert_id"] == selected_alert].iloc[0]

        details = alert_row["details_json"]

        if isinstance(details, str):
            import json
            try:
                details = json.loads(details)
            except Exception:
                details = {}

        explanation = details.get("explanation", [])

        if explanation:
            for e in explanation:
                st.write("•", e)
        else:
            st.info("No explanation available for this alert.")

        # -----------------------------
        # Alert Score Distribution
        # -----------------------------
        st.divider()
        st.subheader("Alert Score Distribution")

        if "score" in alerts.columns:
            score_chart = alerts[["score"]]
            st.bar_chart(score_chart)

        # -----------------------------
        # Severity Score Distribution
        # -----------------------------
        st.divider()
        st.subheader("Alert Severity Distribution")

        def extract_severity(x):
            if isinstance(x, dict):
                return x.get("severity_score")
            try:
                import json
                return json.loads(x).get("severity_score")
            except Exception:
                return None

        sev_df = alerts.copy()

        # If severity_score column doesn't exist, extract from details_json
        if "severity_score" not in sev_df.columns:
            sev_df["severity_score"] = sev_df["details_json"].apply(extract_severity)

        sev_df = sev_df.dropna(subset=["severity_score"])

        if sev_df.empty:
            st.info("No severity score data available.")
        else:
            st.bar_chart(sev_df.set_index("alert_id")[["severity_score"]])

        # -----------------------------
        # Real-Time Alert Rate
        # -----------------------------

        st.divider()
        st.subheader("Real-Time Alert Rate")

        c5, c6 = st.columns(2)
        rate_minutes = c5.selectbox("Lookback window (minutes)", [15, 30, 60, 120], index=2)
        group_mode = c6.selectbox("Group alert rate by", ["method", "source"], index=0)

        if group_mode == "method":
            rate_df = read_df("""
                SELECT
                    date_trunc('minute', created_ts) AS minute_bucket,
                    method AS group_value,
                    COUNT(*) AS alert_count
                FROM alerts_anomalies
                WHERE created_ts >= NOW() - (:mins || ' minutes')::interval
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """, {"mins": str(rate_minutes)})
        else:
            rate_df = read_df("""
                SELECT
                    date_trunc('minute', created_ts) AS minute_bucket,
                    source AS group_value,
                    COUNT(*) AS alert_count
                FROM alerts_anomalies
                WHERE created_ts >= NOW() - (:mins || ' minutes')::interval
                GROUP BY 1, 2
                ORDER BY 1, 2;
            """, {"mins": str(rate_minutes)})

        if rate_df.empty:
            st.info("No alert-rate data yet for the selected window.")
        else:
            rate_pivot = rate_df.pivot(
                index="minute_bucket",
                columns="group_value",
                values="alert_count"
            ).fillna(0)

            st.line_chart(rate_pivot)

            st.write("Alert rate detail:")
            st.dataframe(rate_df, use_container_width=True)
        
        # -----------------------------
        # Alert Per Minute
        # -----------------------------

        st.subheader("Total Alerts Per Minute")
        total_rate_df = read_df("""
            SELECT
                date_trunc('minute', created_ts) AS minute_bucket,
                COUNT(*) AS alert_count
            FROM alerts_anomalies
            WHERE created_ts >= NOW() - (:mins || ' minutes')::interval
            GROUP BY 1
            ORDER BY 1;
        """, {"mins": str(rate_minutes)})

        if total_rate_df.empty:
            st.info("No total alert-rate data yet.")
        else:
            st.area_chart(total_rate_df.set_index("minute_bucket")[["alert_count"]])

    # -----------------------------
    # Alert Counts Summary
    # -----------------------------
    st.divider()
    st.subheader("Alert Counts (last 60 minutes)")

    counts = read_df("""
        SELECT source, anomaly_type, COUNT(*) AS cnt
        FROM alerts_anomalies
        WHERE created_ts >= NOW() - INTERVAL '60 minutes'
        GROUP BY source, anomaly_type
        ORDER BY source, anomaly_type
    """)

    if counts.empty:
        st.info("No alerts recorded in the last hour.")
    else:
        st.dataframe(counts, use_container_width=True)


with tab6:
    st.subheader("Platform Evaluation (latest runs)")

    latest = read_df("""
        SELECT *
        FROM evaluation_runs
        WHERE created_ts >= NOW() - INTERVAL '2 hours'
        ORDER BY created_ts DESC
        LIMIT 300;
    """)

    if latest.empty:
        st.info("No evaluation runs yet. Start: python -m services.evaluation.evaluation_runner")
    else:
        latest_per_source = read_df("""
            SELECT DISTINCT ON (source, method)
              source, method, eps, latency_p50_seconds, latency_p95_seconds,
              tp, fp, fn, precision, recall, f1, created_ts, details_json
            FROM evaluation_runs
            ORDER BY source, method, created_ts DESC;
        """)

        st.subheader("Latest Method-wise Metrics")
        st.dataframe(latest_per_source, use_container_width=True)

        if not latest_per_source.empty:
            st.subheader("Method Comparison Charts")

            metric_df = latest_per_source[["source", "method", "precision", "recall", "f1"]].copy()

            st.write("### F1 Score by Method")
            f1_chart = metric_df.pivot(index="source", columns="method", values="f1")
            st.bar_chart(f1_chart)

            st.write("### Precision by Method")
            precision_chart = metric_df.pivot(index="source", columns="method", values="precision")
            st.bar_chart(precision_chart)

            st.write("### Recall by Method")
            recall_chart = metric_df.pivot(index="source", columns="method", values="recall")
            st.bar_chart(recall_chart)

            def extract_alert_count(x):
                if isinstance(x, dict):
                    return x.get("pred_alert_count")
                try:
                    import json
                    return json.loads(x).get("pred_alert_count")
                except Exception:
                    return None

            st.write("### Alert Volume by Method")
            alert_chart_df = latest_per_source.copy()
            alert_chart_df["pred_alert_count"] = alert_chart_df["details_json"].apply(extract_alert_count)
            alerts_chart = alert_chart_df.pivot(index="source", columns="method", values="pred_alert_count")
            st.bar_chart(alerts_chart)

            st.write("### p50 Latency by Method")
            lat50_chart = latest_per_source.pivot(index="source", columns="method", values="latency_p50_seconds")
            st.bar_chart(lat50_chart)

            st.write("### p95 Latency by Method")
            lat95_chart = latest_per_source.pivot(index="source", columns="method", values="latency_p95_seconds")
            st.bar_chart(lat95_chart)

        st.divider()
        st.write("Latest evaluation rows:")
        st.dataframe(latest, use_container_width=True)

        csv_bytes = latest.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download evaluation CSV",
            data=csv_bytes,
            file_name="evaluation_runs.csv",
            mime="text/csv"
        )
        
with tab7:
    st.subheader("Stream vs Batch Consistency")

    c1, c2, c3, c4 = st.columns(4)
    source = c1.selectbox("Source", ["all", "stock", "social"], index=0, key="tab7_source")
    hours = c2.selectbox("Lookback (hours)", [1, 2, 6, 12, 24], index=2, key="tab7_hours")
    status_filter = c3.selectbox("Status", ["all", "MATCH", "WARN", "FAIL"], index=0, key="tab7_status_filter")

    tag_options = [
        "all",
        "late_arrivals_high",
        "dedupe_mismatch_suspected",
        "stream_missing_or_lag",
        "batch_missing_or_not_run",
        "value_distribution_shift",
        "near_real_time_window"
    ]
    tag_filter = c4.selectbox("Root Cause Tag", tag_options, index=0, key="tab7_tag_filter")

    where = "WHERE window_start >= NOW() - (:hours || ' hours')::interval"
    params = {"hours": str(hours)}

    if source != "all":
        where += " AND source = :source"
        params["source"] = source

    if status_filter != "all":
        where += " AND status = :status"
        params["status"] = status_filter

    if tag_filter != "all":
        where += " AND :tag = ANY(root_cause_tags)"
        params["tag"] = tag_filter

    summary = read_df(f"""
        SELECT status, COUNT(*) AS cnt
        FROM stream_batch_comparison
        {where}
        GROUP BY status
        ORDER BY status;
    """, params)

    st.write("Summary (rows by status):")
    st.dataframe(summary, use_container_width=True)

    st.divider()

    st.subheader("Details")
    details = read_df(f"""
        SELECT window_start, source, key, status,
               stream_count, batch_count, count_diff, count_diff_pct,
               stream_avg, batch_avg, avg_diff, avg_diff_pct,
               root_cause_tags
        FROM stream_batch_comparison
        {where}
        ORDER BY window_start DESC, source, key
        LIMIT 500;
    """, params)

    if details.empty:
        st.info("No comparison rows yet. Ensure batch_recompute + compare_stream_vs_batch are running.")
    else:
        st.dataframe(details, use_container_width=True)
        st.write("Unique root cause tags in result:")
        if not details.empty:
            unique_tags = set()
            for tags in details["root_cause_tags"]:
                if tags:
                    unique_tags.update(tags)
            st.write(sorted(unique_tags))

        csv_bytes = details.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download stream_vs_batch CSV",
            data=csv_bytes,
            file_name="stream_vs_batch_comparison.csv",
            mime="text/csv"
        )

with tab8:
    st.subheader("System Health")

    m = read_df("""
        SELECT metric_ts, metric_name, metric_value, labels_json
        FROM pipeline_metrics
        WHERE metric_ts >= NOW() - INTERVAL '30 minutes'
        ORDER BY metric_ts DESC
        LIMIT 200;
    """)
    if m.empty:
        st.info("No metrics yet. Start: python -m services.monitoring.metrics_job")
    else:
        st.dataframe(m, use_container_width=True)

    st.divider()
    st.subheader("DLQ (last 50)")
    dlq = read_df("""
        SELECT created_ts, error_type, error_message, source, kafka_topic, kafka_partition, kafka_offset
        FROM dlq_events
        ORDER BY created_ts DESC
        LIMIT 50;
    """)
    if dlq.empty:
        st.success("DLQ is empty ✅")
    else:
        st.dataframe(dlq, use_container_width=True)
