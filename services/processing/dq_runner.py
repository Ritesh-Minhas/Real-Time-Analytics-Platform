import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from services.common.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

DQ_INSERT_SQL = """
INSERT INTO dq_results
(run_id, window_start, window_end, source, check_name, status, total_rows, failed_rows, details_json)
VALUES (:run_id, :window_start, :window_end, :source, :check_name, :status, :total_rows, :failed_rows, CAST(:details_json AS jsonb))
"""

SILVER_FETCH_SQL = """
SELECT event_id, source, event_ts, key, value, attributes_json, ingest_ts
FROM silver_clean_events
WHERE event_ts >= NOW() - make_interval(mins => :minutes)
"""

def utc_now():
    return datetime.now(timezone.utc)

def status_from_rate(fail_rate: float, warn_threshold: float, fail_threshold: float) -> str:
    if fail_rate >= fail_threshold:
        return "FAIL"
    if fail_rate >= warn_threshold:
        return "WARN"
    return "PASS"

def run_checks(df: pd.DataFrame, source: str, window_minutes: int):
    """
    Returns list of rows to insert into dq_results.
    """
    results = []
    run_id = str(uuid.uuid4())
    window_end = utc_now()
    window_start = window_end - pd.Timedelta(minutes=window_minutes)

    total = int(len(df))

    def add_result(check_name: str, status: str, failed_rows: int, details: dict):
        results.append({
            "run_id": run_id,
            "window_start": window_start,
            "window_end": window_end,
            "source": source,
            "check_name": check_name,
            "status": status,
            "total_rows": total,
            "failed_rows": int(failed_rows),
            "details_json": json.dumps(details)
        })

    # If no data, mark as WARN (pipeline may be stopped)
    if total == 0:
        add_result(
            "data_presence_last_5m",
            "WARN",
            0,
            {"message": "No rows found in last 5 minutes for this source. Are producers/processors running?"}
        )
        return results

    # 1) Completeness: required fields non-null
    required_cols = ["event_id", "event_ts", "key", "value"]
    missing_mask = df[required_cols].isna().any(axis=1)
    missing_cnt = int(missing_mask.sum())
    missing_rate = missing_cnt / total
    add_result(
        "required_fields_not_null",
        status_from_rate(missing_rate, warn_threshold=0.01, fail_threshold=0.05),
        missing_cnt,
        {"missing_rate": missing_rate, "required_cols": required_cols}
    )

    # 2) Duplicates by event_id
    dup_cnt = int(df["event_id"].duplicated().sum())
    dup_rate = dup_cnt / total
    add_result(
        "duplicate_event_id_rate",
        status_from_rate(dup_rate, warn_threshold=0.005, fail_threshold=0.02),
        dup_cnt,
        {"duplicate_rate": dup_rate}
    )

    # 3) Late arrivals: ingest_ts - event_ts > 30 seconds
    try:
        df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True)
        lateness_sec = (df["ingest_ts"] - df["event_ts"]).dt.total_seconds()
        late_mask = lateness_sec > 30
        late_cnt = int(late_mask.sum())
        late_rate = late_cnt / total
        add_result(
            "late_events_over_30s_rate",
            status_from_rate(late_rate, warn_threshold=0.02, fail_threshold=0.10),
            late_cnt,
            {
                "late_rate": late_rate,
                "threshold_seconds": 30,
                "lateness_p95_seconds": float(lateness_sec.quantile(0.95)) if total > 0 else None
            }
        )
    except Exception as e:
        add_result(
            "late_events_over_30s_rate",
            "WARN",
            0,
            {"message": "Could not compute lateness.", "error": str(e)}
        )

    # 4) Value range checks by source
    if source == "stock":
        # price must be >0 and not absurdly high for simulator
        bad_mask = (df["value"] <= 0) | (df["value"] > 5000)
        bad_cnt = int(bad_mask.sum())
        bad_rate = bad_cnt / total
        add_result(
            "stock_price_range",
            status_from_rate(bad_rate, warn_threshold=0.001, fail_threshold=0.01),
            bad_cnt,
            {"min_allowed": 0, "max_allowed": 5000, "bad_rate": bad_rate}
        )

    if source == "social":
        # mentions should be >=0, cap for sanity
        bad_mask = (df["value"] < 0) | (df["value"] > 100000)
        bad_cnt = int(bad_mask.sum())
        bad_rate = bad_cnt / total
        add_result(
            "social_mentions_range",
            status_from_rate(bad_rate, warn_threshold=0.001, fail_threshold=0.01),
            bad_cnt,
            {"min_allowed": 0, "max_allowed": 100000, "bad_rate": bad_rate}
        )

    # 5) Key cardinality sanity (too many unique keys can indicate data explosion)
    unique_keys = int(df["key"].nunique())
    add_result(
        "unique_key_count_sanity",
        "PASS" if unique_keys <= 200 else "WARN",
        0,
        {"unique_keys": unique_keys, "warn_if_gt": 200}
    )

    # 6) Overall summary check (roll-up of all checks above)
    # Score: PASS=1, WARN=0.5, FAIL=0
    status_to_score = {"PASS": 1.0, "WARN": 0.5, "FAIL": 0.0}
    check_scores = []
    check_statuses = []

    for r in results:
        if r["check_name"] == "dq_overall_summary":
            continue
        s = r["status"]
        check_statuses.append(s)
        check_scores.append(status_to_score.get(s, 0.0))

    overall_score = sum(check_scores) / max(1, len(check_scores))
    # Overall status logic: if any FAIL → FAIL; else if any WARN → WARN; else PASS
    if "FAIL" in check_statuses:
        overall_status = "FAIL"
    elif "WARN" in check_statuses:
        overall_status = "WARN"
    else:
        overall_status = "PASS"

    add_result(
        "dq_overall_summary",
        overall_status,
        0,
        {
            "overall_score_0_1": overall_score,
            "overall_score_0_100": round(100 * overall_score),
            "pass": check_statuses.count("PASS"),
            "warn": check_statuses.count("WARN"),
            "fail": check_statuses.count("FAIL"),
            "note": "Roll-up score across DQ checks for last 5 minutes"
        }
    )

    return results

def main(poll_minutes: int = 5, every_seconds: int = 15):
    print(f"[dq_runner] Running DQ every {every_seconds}s over last {poll_minutes} minutes...")
    while True:
        with ENGINE.connect() as conn:
            df = pd.read_sql(text(SILVER_FETCH_SQL), conn, params={"minutes": int(poll_minutes)})

        # split per source
        for source in ["stock", "social"]:
            sdf = df[df["source"] == source].copy()
            results = run_checks(sdf, source, poll_minutes)

            if results:
                with ENGINE.begin() as conn:
                    conn.execute(text(DQ_INSERT_SQL), results)

            latest_status = {r["check_name"]: r["status"] for r in results}
            print(f"[dq_runner] source={source} total={len(sdf)} checks={latest_status}")

        time.sleep(every_seconds)

if __name__ == "__main__":
    main()
