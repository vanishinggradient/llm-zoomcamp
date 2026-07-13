"""HW5: Monitoring - Solution using OpenTelemetry + SQLite + Groq"""

import os
import sqlite3

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

load_dotenv("/Users/dt00035/projects/personal/reference/llm-zoomcamp/.env")

from gitsource import GithubRepositoryDataReader
from minsearch import Index
from rag_helper import RAGBase

# ── Load documents ────────────────────────────────────────────────────────────
print("Loading documents...")
reader = GithubRepositoryDataReader(
    repo_owner="DataTalksClub",
    repo_name="llm-zoomcamp",
    commit_id="8c1834d",
    allowed_extensions={"md"},
    filename_filter=lambda path: "/lessons/" in path,
)
documents = [file.parse() for file in reader.read()]
print(f"Loaded {len(documents)} documents")

index = Index(text_fields=["content"], keyword_fields=["filename"])
index.fit(documents)

groq_client = OpenAI(
    api_key=os.environ["GROQ_API_KEY"],
    base_url="https://api.groq.com/openai/v1"
)

# ── SQLiteSpanExporter (exact schema from homework) ───────────────────────────
class SQLiteSpanExporter(SpanExporter):
    def __init__(self, db_path="traces.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS spans (
                name TEXT,
                start_time INTEGER,
                end_time INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cost REAL
            )
        """)
        self.conn.commit()

    def export(self, spans):
        for span in spans:
            attrs = dict(span.attributes or {})
            self.conn.execute(
                "INSERT INTO spans VALUES (?, ?, ?, ?, ?, ?)",
                (
                    span.name,
                    span.start_time,
                    span.end_time,
                    attrs.get("input_tokens"),
                    attrs.get("output_tokens"),
                    attrs.get("cost"),
                ),
            )
        self.conn.commit()
        return SpanExportResult.SUCCESS

    def shutdown(self):
        self.conn.close()

    def force_flush(self):
        return True


# ── Single TracerProvider with SQLite exporter ────────────────────────────────
DB_PATH = "traces.db"
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

sqlite_exp = SQLiteSpanExporter(DB_PATH)
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(sqlite_exp))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("llm-zoomcamp")


# ── RAGTraced subclass ────────────────────────────────────────────────────────
class RAGTraced(RAGBase):
    def __init__(self, tracer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = tracer

    def llm(self, prompt):
        response = self.llm_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": self.instructions},
                {"role": "user", "content": prompt}
            ]
        )
        return response

    def rag(self, query):
        with self.tracer.start_as_current_span("rag") as rag_span:
            with self.tracer.start_as_current_span("search"):
                search_results = self.search(query)

            prompt = self.build_prompt(query, search_results)

            with self.tracer.start_as_current_span("llm") as llm_span:
                response = self.llm(prompt)
                usage = response.usage
                input_tok = usage.prompt_tokens
                output_tok = usage.completion_tokens
                llm_span.set_attribute("input_tokens", input_tok)
                llm_span.set_attribute("output_tokens", output_tok)
                cost = (input_tok * 0.59 + output_tok * 0.79) / 1_000_000
                llm_span.set_attribute("cost", cost)

            answer = response.choices[0].message.content
            rag_span.set_attribute("question", query)
            return answer


rag = RAGTraced(tracer=tracer, index=index, llm_client=groq_client)
QUERY = "How does the agentic loop keep calling the model until it stops?"

# ── Run 4 times (covers Q1-Q6: first run + 3 more = 4 total) ─────────────────
print(f"\nRunning query 4 times: {QUERY}")
for i in range(4):
    answer = rag.rag(QUERY)
    print(f"Run {i+1}: done")

# ── Query spans table ─────────────────────────────────────────────────────────
df = pd.read_sql("SELECT name, start_time, end_time, input_tokens, output_tokens, cost FROM spans", sqlite_exp.conn)
df["duration_ms"] = (df["end_time"] - df["start_time"]) / 1_000_000

print("\n--- All spans in DB ---")
print(df[["name", "duration_ms", "input_tokens"]].to_string())

# Q1: spans per trace = total rows / 4 runs
spans_per_trace = len(df) // 4
print(f"\nQ1: Spans per trace = {spans_per_trace}")

# Q2: input tokens for LLM span (first run)
first_llm = df[df["name"] == "llm"].iloc[0]
print(f"Q2: input_tokens = {int(first_llm['input_tokens'])}")

# Q3: LLM duration
print(f"Q3: LLM call duration = {first_llm['duration_ms']:.0f}ms")

# Q4: unique span names
print(f"Q4: span names = {df['name'].unique().tolist()}")

# Q5: total time per span type (excl. rag)
non_rag = df[df["name"] != "rag"].groupby("name")["duration_ms"].sum()
print(f"Q5: total duration excl. rag:\n{non_rag}")
print(f"    slowest: {non_rag.idxmax()}")

# Q6: input tokens across 4 LLM calls
llm_tokens = df[df["name"] == "llm"]["input_tokens"].tolist()
print(f"\nQ6: input tokens per run = {llm_tokens}")
diff = max(llm_tokens) - min(llm_tokens)
pct = diff / min(llm_tokens) * 100 if min(llm_tokens) > 0 else 0
print(f"    max diff = {diff} tokens ({pct:.1f}%)")
if diff == 0:
    q6 = "They are identical"
elif pct <= 10:
    q6 = "Within 10% of each other"
elif pct <= 50:
    q6 = "Within 50% of each other"
else:
    q6 = "They vary more than 50%"
print(f"    Q6 answer: {q6}")

print("\n" + "="*60)
print("FINAL ANSWERS")
print("="*60)
print(f"Q1: {spans_per_trace} spans per trace")
print(f"Q2: {int(first_llm['input_tokens'])} input tokens")
print(f"Q3: {first_llm['duration_ms']:.0f}ms")
print(f"Q4: {df['name'].unique().tolist()}")
print(f"Q5: {non_rag.idxmax()}")
print(f"Q6: {q6}")
