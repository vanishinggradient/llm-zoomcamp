import requests
from tqdm.auto import tqdm 
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch

model_name = "multi-qa-distilbert-cos-v1"
embedding_model = SentenceTransformer(model_name)

user_question = "I just discovered the course. Can I still join it?"

user_question_embedding = embedding_model.encode(user_question)

print(user_question_embedding[0])

def load_documents():
    base_url = 'https://github.com/DataTalksClub/llm-zoomcamp/blob/main'
    relative_url = '03-vector-search/eval/documents-with-ids.json'
    docs_url = f'{base_url}/{relative_url}?raw=1'
    docs_response = requests.get(docs_url)
    documents = docs_response.json()
    return documents

docs = load_documents()

print(len(docs))

docs[0]

print(type(docs))

course_name = "machine-learning-zoomcamp"
data = []
for doc in docs:
    if doc["course"] == course_name:
        data.append(doc)
print(len(data))

embeddings = []

for doc in data:
    question = doc["question"]
    text = doc["text"]
    qa_text = f'{question} {text}'
    qa_embedding = embedding_model.encode(qa_text)
    embeddings.append(qa_embedding)
    
print(len(embeddings))

X = np.array(embeddings)
print(X.shape)

print(len(user_question_embedding))

v = np.array(user_question_embedding)
print(v.shape)

scores = X.dot(v)

print(type(scores))

max_value = np.max(scores)
print(max_value)

class VectorSearchEngine():
    def __init__(self, documents, embeddings):
        self.documents = documents
        self.embeddings = embeddings

    def search(self, v_query, num_results=10):
        scores = self.embeddings.dot(v_query)
        idx = np.argsort(-scores)[:num_results]
        return [self.documents[i] for i in idx]

search_engine = VectorSearchEngine(documents=data, embeddings=X)

search_engine.search(v, num_results=5)

base_url = 'https://github.com/DataTalksClub/llm-zoomcamp/blob/main'
relative_url = '03-vector-search/eval/ground-truth-data.csv'
ground_truth_url = f'{base_url}/{relative_url}?raw=1'

df_ground_truth = pd.read_csv(ground_truth_url)
df_ground_truth = df_ground_truth[df_ground_truth.course == 'machine-learning-zoomcamp']
ground_truth = df_ground_truth.to_dict(orient='records')

print(type(ground_truth))
print(ground_truth[0])

sum = 0
for i in range(len(ground_truth)):
    question = ground_truth[i]['question']
    doc_id = ground_truth[i]['document']
    # print(question, doc_id)
    question_embedding = embedding_model.encode(question)
    results = search_engine.search(question_embedding, num_results=5)
    for hit in results:
        if hit['id'] == doc_id:
            sum += 1
            break

hit_rate = float(sum/len(ground_truth))

print(sum)
print(len(ground_truth))
print(hit_rate)


es_client = Elasticsearch('http://localhost:9200')
es_client.info()

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
            "course": {"type": "keyword"},
            "text_vector": {"type": "dense_vector", "dims": 768, "index": True, "similarity": "cosine"},
        }
    }
}

index_name = "course-questions"

es_client.indices.create(index=index_name, body=index_settings)

for doc in tqdm(embeddings):
    try:
        es_client.index(index=index_name, document=doc)
    except Exception as e:
        print(e)

