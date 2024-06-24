from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import hashlib
import os 
from pymongo import MongoClient
import weaviate
import uvicorn
from datetime import datetime

from utils import generate_query_hash, store_query_in_weaviate, store_response_in_mongodb, search_similar_query

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client.cache_db
collection = db.cache

# Weaviate client
headers = {
    "X-OpenAI-Api-Key": os.getenv("OPEN_API_KEY")
}  # Replace with your OpenAI API key

weaviate_client = weaviate.connect(headers=headers)

class QueryRequest(BaseModel):
    query: str
    response: str

@app.post("/query")
async def handle_query(request: QueryRequest):
    query = request.query
    response = request.response

    # Generate unique query hash
    query_hash = generate_query_hash(query)

    # Store the query in Weaviate
    try:
        store_query_in_weaviate(weaviate_client, query, query_hash)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store query in Weaviate: {str(e)}")

    # Store the response in MongoDB
    try:
        store_response_in_mongodb(collection, query, response, query_hash)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store response in MongoDB: {str(e)}")

    return {"query_hash": query_hash, "status": "success"}

class SearchRequest(BaseModel):
    query: str

@app.post("/search")
async def search_query(request: SearchRequest):
    query = request.query

    # Perform semantic search in Weaviate
    try:
        similar_query_hash = search_similar_query(weaviate_client, query)
        if not similar_query_hash:
            return {"status": "No similar query found with sufficient similarity"}

        # Retrieve response from MongoDB using the query hash
        response_doc = collection.find_one({"query_hash": similar_query_hash})
        if not response_doc:
            raise HTTPException(status_code=404, detail="Response not found in MongoDB")

        return {"query": response_doc["query"], "response": response_doc["response"], "status": "success"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform search: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
