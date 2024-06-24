import hashlib
from pymongo.collection import Collection
import weaviate
from datetime import datetime

def generate_query_hash(query: str) -> str:
    """Generate a unique hash for the given query."""
    return hashlib.sha256(query.encode()).hexdigest()

def store_query_in_weaviate(client: weaviate.Client, query: str, query_hash: str):
    """Store the query in the Weaviate database."""
    client.data_object.create(
        {
            "query": query,
        },
        class_name="Query"
    )

def store_response_in_mongodb(collection: Collection, query: str, response: str, query_hash: str):
    """Store the response in MongoDB."""
    collection.insert_one({
        "query": query,
        "response": response,
        "query_hash": query_hash,
        "timestamp": datetime.utcnow().isoformat()
    })

def search_similar_query(client: weaviate.Client, query: str) -> str:
    """Perform a semantic search in Weaviate to find a similar query."""
    response = client.query.get("Query", ["query_hash", "_additional { certainty }"]).with_near_text({"concepts": [query]}).with_limit(1).do()

    if response and response["data"]["Get"]["Query"]:
        result = response["data"]["Get"]["Query"][0]
        certainty = result["_additional"]["certainty"]
        if certainty > 0.75:
            return result["query_hash"]

    return None
