"""
Adapted from:
https://raw.githubusercontent.com/DataTalksClub/llm-zoomcamp/main/01-agentic-rag/code/rag_helper.py

Changes from original:
- Uses chat.completions API (works with OpenAI, Groq, and any OpenAI-compatible provider)
- search(): removed FAQ-specific boost_dict and filter_dict
- build_context(): uses filename/content instead of section/question/answer
- llm(): returns full response so callers can read usage
- rag(): returns (answer, usage) tuple — usage.prompt_tokens = input tokens
"""

INSTRUCTIONS = """
Your task is to answer questions from course participants
based on the provided context.

Use the context to find relevant information and provide accurate answers.
If the answer is not found in the context, respond with "I don't know."
""".strip()

PROMPT_TEMPLATE = """
QUESTION: {question}

CONTEXT:
{context}
""".strip()


class RAGBase:
    def __init__(
        self,
        index,
        llm_client,
        instructions=INSTRUCTIONS,
        prompt_template=PROMPT_TEMPLATE,
        model="llama-3.3-70b-versatile",
    ):
        self.index = index
        self.llm_client = llm_client
        self.instructions = instructions
        self.prompt_template = prompt_template
        self.model = model

    def search(self, query, num_results=5):
        return self.index.search(query, num_results=num_results)

    def build_context(self, search_results):
        lines = []
        for doc in search_results:
            lines.append(f"File: {doc['filename']}")
            lines.append(doc["content"])
            lines.append("")
        return "\n".join(lines).strip()

    def build_prompt(self, query, search_results):
        context = self.build_context(search_results)
        return self.prompt_template.format(question=query, context=context)

    def llm(self, prompt):
        """Send prompt to LLM. Returns full response (with .usage.prompt_tokens)."""
        response = self.llm_client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self.instructions},
                {"role": "user", "content": prompt},
            ],
        )
        return response

    def rag(self, query):
        """Run RAG pipeline. Returns (answer: str, usage: object).
        usage.prompt_tokens = input tokens sent to the model.
        """
        search_results = self.search(query)
        prompt = self.build_prompt(query, search_results)
        response = self.llm(prompt)
        return response.choices[0].message.content, response.usage
