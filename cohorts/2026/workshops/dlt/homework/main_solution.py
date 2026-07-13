"""
dlt Workshop Homework — Solution

Uses Groq (llama-3.3-70b-versatile) instead of OpenAI.

Before running:
  1. Set LOGFIRE_TOKEN in .env (write token from your Logfire project)
  2. Run: uv run python main_solution.py
  3. Check Logfire dashboard, count spans → Q1
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Root .env has the actual tokens; local .env may override with blanks — load root first
load_dotenv("/Users/dt00035/projects/personal/reference/llm-zoomcamp/.env")
load_dotenv(override=False)  # local .env won't overwrite already-set vars

# logfire uses LOGFIRE_TOKEN; root .env uses LOGFIRE_WRITE_TOKEN
if not os.environ.get("LOGFIRE_TOKEN") and os.environ.get("LOGFIRE_WRITE_TOKEN"):
    os.environ["LOGFIRE_TOKEN"] = os.environ["LOGFIRE_WRITE_TOKEN"]

import logfire
from minsearch import Index
from pydantic_ai import Agent, RunContext

from ingest import build_index, load_faq_data

# ── Logfire instrumentation ────────────────────────────────────────────────────
# LOGFIRE_TOKEN must be set in .env
logfire.configure()
logfire.instrument_pydantic_ai()

# ── Agent (Groq) ───────────────────────────────────────────────────────────────
INSTRUCTIONS = """
You're a course teaching assistant.
You're given a question from a course student and your task is to answer it.

If you want to look up information, use the search function.
Use as many keywords from the user question as possible when making first requests.

Make multiple searches. First perform search, analyze the results
and then perform more searches.

The question has to be about the course or its logistics, offtopic questions
shouldn't be answered. If the search returns nothing, it's likely an off-topic question.
If you can't answer the question using FAQ, don't do it yourself. Only use the
facts from the FAQ database.

At the end, ask if there are other areas that the user wants to explore.
""".strip()


@dataclass
class SearchDeps:
    index: Index


faq_agent = Agent(
    'groq:llama-3.3-70b-versatile',
    deps_type=SearchDeps,
    instructions=INSTRUCTIONS,
)


@faq_agent.tool
def search(ctx: RunContext[SearchDeps], query: str) -> str:
    """Search the FAQ database for entries matching the given query."""
    results = ctx.deps.index.search(
        query,
        num_results=5,
        boost_dict={'question': 3.0, 'section': 0.5},
        filter_dict={'course': 'llm-zoomcamp'},
    )
    return results


def main():
    print("Loading FAQ data...")
    documents = load_faq_data()
    index = build_index(documents)
    deps = SearchDeps(index=index)

    # Q1: run this query and count spans in Logfire
    question = 'How do I run Ollama locally?'
    print(f"\nQuery: {question!r}")

    result = faq_agent.run_sync(question, deps=deps)
    print(f"\nAnswer:\n{result.output}")
    print("\n→ Open Logfire dashboard and count spans for this trace (Q1)")


if __name__ == '__main__':
    main()
