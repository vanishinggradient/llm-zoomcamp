"""
HW4: Evaluation - Solution Script
Answers all 6 questions and prints results.
Uses Groq (llama-3.3-70b) for Q1 since no OpenAI key available.
"""

import json
import os
import sys
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel
from tqdm.auto import tqdm

# Load env
load_dotenv("/Users/dt00035/projects/personal/reference/llm-zoomcamp/.env")

# ─── Q1: Generate questions for first 3 pages ────────────────────────────────

print("\n=== SETUP: Loading documents ===")
from gitsource import GithubRepositoryDataReader, chunk_documents

reader = GithubRepositoryDataReader(
    repo_owner="DataTalksClub",
    repo_name="llm-zoomcamp",
    commit_id="8c1834d",
    allowed_extensions={"md"},
    filename_filter=lambda path: "/lessons/" in path,
)
documents = [file.parse() for file in reader.read()]
print(f"Loaded {len(documents)} documents")

# ─── Q1 ───────────────────────────────────────────────────────────────────────
print("\n=== Q1: Average input tokens for first 3 pages ===")

TARGET_PAGES = [
    "01-agentic-rag/lessons/01-intro.md",
    "01-agentic-rag/lessons/02-environment.md",
    "01-agentic-rag/lessons/03-rag.md",
]

first_3 = [d for d in documents if d["filename"] in TARGET_PAGES]
first_3_sorted = sorted(first_3, key=lambda d: TARGET_PAGES.index(d["filename"]))
print(f"Found pages: {[d['filename'] for d in first_3_sorted]}")

data_gen_instructions = """
You emulate a student who is taking our LLM course.
You are given one lesson page from the course.
Formulate 5 questions this student might ask that are answered by this page.

Rules:
- The page should contain the answer to each question.
- Make the questions complete and not too short.
- Use as few words as possible from the page; don't copy its phrasing.
- The questions should resemble how people actually ask things online:
  not too formal, not too short, not too long.
- Ask about the content of the lesson, not about its formatting or filename.
""".strip()

class Questions(BaseModel):
    questions: list[str]

groq_key = os.environ.get("GROQ_API_KEY")

from openai import OpenAI as _OAI
groq_client = _OAI(api_key=groq_key, base_url="https://api.groq.com/openai/v1")

token_counts = []
for doc in first_3_sorted:
    user_prompt = json.dumps({"filename": doc["filename"], "content": doc["content"]})

    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": data_gen_instructions + "\n\nRespond with JSON only."},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )

    input_tokens = response.usage.prompt_tokens
    token_counts.append(input_tokens)
    print(f"  {doc['filename']}: {input_tokens} input tokens")

avg_tokens = sum(token_counts) / len(token_counts)
print(f"\nQ1 Answer: Average input tokens = {avg_tokens:.0f}")

# ─── Build search indices ──────────────────────────────────────────────────────
print("\n=== SETUP: Building search indices ===")
from minsearch import Index
from minsearch.vector import VectorSearch
from embedder import Embedder

chunks = chunk_documents(documents, size=2000, step=1000)
print(f"Created {len(chunks)} chunks")

# Text index
txt_index = Index(text_fields=["content"], keyword_fields=["filename", "start"])
txt_index.fit(chunks)
print("Text index built")

# Vector index
embedder = Embedder()
contents = [c["content"] for c in chunks]
X = embedder.encode_batch(contents)
print(f"Embeddings shape: {X.shape}")

vec_index = VectorSearch(keyword_fields=["filename", "start"])
vec_index.fit(X, chunks)
print("Vector index built")

def text_search(query, num_results=5):
    return txt_index.search(query, num_results=num_results)

def vector_search(query, num_results=5):
    q_vec = embedder.encode(query)
    return vec_index.search(q_vec, num_results=num_results)

def rrf(result_lists, k=60, num_results=5):
    scores, docs = {}, {}
    for results in result_lists:
        for rank, doc in enumerate(results):
            key = (doc["filename"], doc["start"])
            scores[key] = scores.get(key, 0) + 1 / (k + rank)
            docs[key] = doc
    ranked = sorted(scores, key=scores.get, reverse=True)
    return [docs[key] for key in ranked[:num_results]]

def hybrid_search(query, k=60, num_results=5):
    return rrf([text_search(query, 10), vector_search(query, 10)], k=k, num_results=num_results)

# ─── Load ground truth ────────────────────────────────────────────────────────
print("\n=== SETUP: Loading ground truth ===")
df = pd.read_csv("ground-truth.csv")
ground_truth = df.to_dict(orient="records")
print(f"Loaded {len(ground_truth)} ground truth records")
print(f"First question: {ground_truth[0]['question']}")
print(f"Expected filename: {ground_truth[0]['filename']}")

# ─── Q2 & Q3 ──────────────────────────────────────────────────────────────────
print("\n=== Q2: First result with text_search ===")
q = ground_truth[0]["question"]
text_results = text_search(q)
q2_answer = text_results[0]["filename"]
print(f"Q2 Answer: {q2_answer}")
print(f"  Top 3 text results: {[r['filename'] for r in text_results[:3]]}")

print("\n=== Q3: First result with vector_search ===")
vec_results = vector_search(q)
q3_answer = vec_results[0]["filename"]
print(f"Q3 Answer: {q3_answer}")
print(f"  Top 3 vector results: {[r['filename'] for r in vec_results[:3]]}")

# ─── Evaluation helpers ───────────────────────────────────────────────────────
def compute_relevance(q_record, search_fn):
    filename = q_record["filename"]
    results = search_fn(q_record["question"])
    return [int(r["filename"] == filename) for r in results]

def hit_rate(relevance_total):
    return sum(1 for r in relevance_total if 1 in r) / len(relevance_total)

def mrr(relevance_total):
    total = 0.0
    for r in relevance_total:
        for rank, val in enumerate(r):
            if val == 1:
                total += 1 / (rank + 1)
                break
    return total / len(relevance_total)

def evaluate(ground_truth, search_fn, desc=""):
    relevance_total = [compute_relevance(q, search_fn) for q in tqdm(ground_truth, desc=desc)]
    return {"hit_rate": hit_rate(relevance_total), "mrr": mrr(relevance_total)}

# ─── Q4: text_search hit rate ─────────────────────────────────────────────────
print("\n=== Q4: Evaluating text_search ===")
text_eval = evaluate(ground_truth, text_search, "text_search")
print(f"Q4 Answer: Hit Rate = {text_eval['hit_rate']:.4f}")
print(f"  MRR = {text_eval['mrr']:.4f}")

# ─── Q5: vector_search MRR ───────────────────────────────────────────────────
print("\n=== Q5: Evaluating vector_search ===")
vec_eval = evaluate(ground_truth, vector_search, "vector_search")
print(f"Q5 Answer: MRR = {vec_eval['mrr']:.4f}")
print(f"  Hit Rate = {vec_eval['hit_rate']:.4f}")

# ─── Q6: hybrid_search k tuning ──────────────────────────────────────────────
print("\n=== Q6: Tuning hybrid_search k values ===")
hybrid_results = {}
for k in [1, 50, 100, 200]:
    result = evaluate(ground_truth, lambda q, k=k: hybrid_search(q, k=k), f"hybrid k={k}")
    hybrid_results[k] = result
    print(f"  k={k}: MRR={result['mrr']:.4f}, Hit Rate={result['hit_rate']:.4f}")

best_k = max(hybrid_results, key=lambda k: hybrid_results[k]["mrr"])
# If tie, pick smallest k
max_mrr = hybrid_results[best_k]["mrr"]
tied_ks = [k for k in [1, 50, 100, 200] if hybrid_results[k]["mrr"] == max_mrr]
q6_answer = min(tied_ks)
print(f"Q6 Answer: Best k = {q6_answer} (MRR={max_mrr:.4f})")

# ─── SUMMARY ─────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("FINAL ANSWERS")
print("="*60)
print(f"Q1: Average input tokens = {avg_tokens:.0f}")
print(f"Q2: text_search first result = {q2_answer}")
print(f"Q3: vector_search first result = {q3_answer}")
print(f"Q4: text_search Hit Rate = {text_eval['hit_rate']:.4f}")
print(f"Q5: vector_search MRR = {vec_eval['mrr']:.4f}")
print(f"Q6: Best k for hybrid_search = {q6_answer}")
