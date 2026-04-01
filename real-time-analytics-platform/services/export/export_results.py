import os
import pandas as pd
from sqlalchemy import create_engine

PG_HOST="localhost"
PG_PORT="5433"
PG_DB="rt_analytics"
PG_USER="rt_user"
PG_PASSWORD="rt_pass"

engine=create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

EXPORT_DIR="exports"
os.makedirs(EXPORT_DIR,exist_ok=True)

tables={
"alerts":"alerts_anomalies",
"dq":"dq_results",
"evaluation":"evaluation_runs",
"stream_batch":"stream_batch_comparison"
}

for name,table in tables.items():
    df=pd.read_sql(f"SELECT * FROM {table}",engine)
    path=f"{EXPORT_DIR}/{name}.csv"
    df.to_csv(path,index=False)
    print("exported",path)