import requests
from elasticsearch import Elasticsearch
from tqdm.auto import tqdm


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
    response = es.indices.create(index=index_name, body=index_settings)
    return response

def index_documents(documents, index_name):
    es = Elasticsearch("http://localhost:9200")
    es.info()

    for doc in tqdm(documents):
        es.index(index=index_name, document=doc)


def search(index_name, user_question):

    es = Elasticsearch("http://localhost:9200")
    es.info()

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
                        "course": "machine-learning-zoomcamp"
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, body=search_query)

    for hit in response['hits']['hits']:
        doc = hit['_source']
        print(f"Section: {doc['section']}")
        print(f"Question: {doc['question']}")
        print(f"Answer: {doc['text'][:60]}...\n")

    return response

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
    response = search(index_name, user_question)
    print(response)


if __name__ == "__main__":
    main()