"""
Q2: Pull Logfire traces into DuckDB with dlt, count tables.
Q3: Query input token usage.

Run AFTER main_solution.py has sent at least one trace to Logfire.

Requires in .env (root):
  LOGFIRE_READ_TOKEN
"""

import os
import json
import dlt
import duckdb
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv("/Users/dt00035/projects/personal/reference/llm-zoomcamp/.env")
load_dotenv(override=False)

READ_TOKEN = os.environ.get("LOGFIRE_READ_TOKEN", "")


@dlt.resource(name="spans", write_disposition="replace")
def logfire_spans():
    """Pull spans from Logfire using the query client."""
    from logfire.query_client import LogfireQueryClient

    client = LogfireQueryClient(read_token=READ_TOKEN)
    min_ts = datetime.now(timezone.utc) - timedelta(days=7)
    rows = client.query_json_rows(
        """
        SELECT
            trace_id,
            span_id,
            parent_span_id,
            span_name,
            start_timestamp,
            end_timestamp,
            duration,
            attributes
        FROM records
        ORDER BY start_timestamp DESC
        LIMIT 1000
        """,
        min_timestamp=min_ts,
    )
    yield from rows["rows"]


@dlt.source(name="logfire_traces")
def logfire_source():
    yield logfire_spans()


def run():
    pipeline = dlt.pipeline(
        pipeline_name="logfire_pipeline",
        destination="duckdb",
        dataset_name="agent_traces",
    )

    info = pipeline.run(logfire_source())
    print(info)

    # Q2: count tables
    conn = duckdb.connect("logfire_pipeline.duckdb")
    count = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'agent_traces'
    """).fetchone()[0]
    print(f"\nQ2: Tables in agent_traces schema = {count}")

    # Show table names
    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'agent_traces'
        ORDER BY table_name
    """).fetchall()
    print("Tables:", [t[0] for t in tables])

    # Q3: find input token usage
    for (table_name,) in tables:
        try:
            cols = conn.execute(
                f"SELECT * FROM agent_traces.{table_name} LIMIT 1"
            ).description
            col_names = [d[0] for d in cols]
            print(f"\n{table_name} columns: {col_names}")

            # Check if attributes column exists
            if "attributes" in col_names:
                rows = conn.execute(
                    f"SELECT attributes FROM agent_traces.{table_name}"
                ).fetchall()
                total_input_tokens = 0
                for (attrs_raw,) in rows:
                    if attrs_raw is None:
                        continue
                    attrs = attrs_raw if isinstance(attrs_raw, dict) else json.loads(attrs_raw)
                    tokens = attrs.get("gen_ai.usage.input_tokens") or attrs.get("input_tokens")
                    if tokens:
                        total_input_tokens += int(tokens)
                        print(f"  Found input_tokens={tokens} in {table_name}")
                if total_input_tokens:
                    print(f"\nQ3: Total input tokens = {total_input_tokens}")
        except Exception as e:
            print(f"  Error reading {table_name}: {e}")


if __name__ == "__main__":
    run()
