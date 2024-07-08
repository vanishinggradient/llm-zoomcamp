import requests
from elasticsearch import Elasticsearch
from tqdm.auto import tqdm
import tiktoken


def get_data(url):
    """
    Fetch JSON data from the provided URL.
    
    Parameters:
    url (str): The URL to fetch the JSON data from.
    
    Returns:
    dict: The parsed JSON data.
    
    Raises:
    ValueError: If the response is not valid JSON.
    HTTPError: If the HTTP request returned an unsuccessful status code.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None
    except ValueError as e:
        print(f"Invalid JSON: {e}")
        return None

def load_data(data):
    documents = []

    for course in data:
        course_name = course['course']

        for doc in course['documents']:
            doc['course'] = course_name
            documents.append(doc)

    return documents

def create_index(index_name):
    es = Elasticsearch("http://localhost:9200")
    es.info()

    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "section": {"type": "text"},
                "question": {"type": "text"},
                "course": {"type": "keyword"} 
            }
        }
    }

    # TODO if index exists then skip or return a response index exists
    response = es.indices.create(index=index_name, body=index_settings)
    return response

def index_documents(documents, index_name):
    es = Elasticsearch("http://localhost:9200")
    es.info()

    for doc in tqdm(documents):
        es.index(index=index_name, document=doc)


def search(index_name, search_query):

    es = Elasticsearch("http://localhost:9200")
    es.info()

    response = es.search(index=index_name, body=search_query)

    for hit in response['hits']['hits']:
        doc = hit['_source']
        print(f"Section: {doc['section']}")
        print(f"Question: {doc['question']}")
        print(f"Answer: {doc['text'][:60]}...\n")

    return response

def retrieve_documents(query, index_name="course-questions", max_results=3):
    es = Elasticsearch("http://localhost:9200")
    
    search_query = {
        "size": max_results,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": query,
                        "fields": ["question^3", "text", "section"],
                        "type": "best_fields"
                    }
                },
                "filter": {
                    "term": {
                        "course": "data-engineering-zoomcamp"
                    }
                }
            }
        }
    }
    
    response = es.search(index=index_name, body=search_query)
    documents = [hit['_source'] for hit in response['hits']['hits']]
    return documents

def build_context(documents):
    context = ""

    for doc in documents:
        doc_str = f"Section: {doc['section']}\nQuestion: {doc['question']}\nAnswer: {doc['text']}\n\n"
        context += doc_str
    
    context = context.strip()
    return context

def build_prompt(user_question, documents):
    context = build_context(documents)
    return f"""
    QUESTION: {user_question}

    CONTEXT:

    {context}
    """.strip()

def ask_openai(role, prompt, model="gpt-3.5-turbo"):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": role},
            {"role": "user", "content": prompt}
        ]
    )
    answer = response.choices[0].message.content
    return answer

def main():
    """
    Main function to fetch and process data.
    """
    url = 'https://github.com/DataTalksClub/llm-zoomcamp/blob/main/01-intro/documents.json?raw=1'
    data = get_data(url)
    # print(len(data))
    documents = load_data(data)
    print(len(documents))
    index_name = "course-questions"

    # response = create_index(index_name)
    # print(response)
    # response = index_documents(documents, index_name)
    # print(response)

    user_question = "How do I execute a command in a running docker container?"
    course_name = "machine-learning-zoomcamp"

    search_query = {
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": user_question,
                        "fields": ["question^4", "text"],
                        "type": "best_fields"
                    }
                }
            }
        }
    }

    response = search(index_name, search_query)
    print(response)

    course_name = "machine-learning-zoomcamp"

    search_query = {
        "size": 3,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": user_question,
                        "fields": ["question^4", "text"],
                        "type": "best_fields"
                    }
                },
                "filter": {
                    "term": {
                        "course": course_name
                    }
                }
            }
        }
    }

    response = search(index_name, search_query)
    print(type(response))
    print(len(response))

    # for hit in response['hits']['hits']:
    #     doc = hit['_source']
    #     print(f"Section: {doc['section']}\nQuestion: {doc['question']}\nAnswer: {doc['text']}\n\n")

    documents = [hit['_source'] for hit in response['hits']['hits']]

    context_template = """
    Q: {question}
    A: {text}
    """.strip()

    context = ""

    for doc in documents:
        doc_str = f"\nQ: {doc['question']}\nA: {doc['text']}".strip()
        context += doc_str + "\n"

    context = context.strip()
    print(context)

    role = """
    You're a course teaching assistant.
    Answer the user QUESTION based on CONTEXT - the documents retrieved from our FAQ database.
    Don't use other information outside of the provided CONTEXT.  
    """.strip()

    question = user_question

    prompt = f"You're a course teaching assistant. Answer the QUESTION based on the CONTEXT from the FAQ database.\nUse only the facts from the CONTEXT when answering the QUESTION.\n\nQUESTION: {question}\nCONTEXT:\n{context}".strip()

    print(len(prompt))

    encoding = tiktoken.encoding_for_model("gpt-4o")
    encoded_prompt = encoding.encode(prompt)
    print(len(encoded_prompt))
    #decoded_prompt = encoding.decode_single_token_bytes(63482)
    # print(len(decoded_prompt))


    



if __name__ == "__main__":
    main()