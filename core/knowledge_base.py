import json
import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from loguru import logger

from core.config import get_settings

settings = get_settings()

_client = None
_collection = None


def get_kb():
    global _client, _collection
    if _client is None:
        _client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        embedding_fn = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        _collection = _client.get_or_create_collection(
            name="campaign_knowledge",
            embedding_function=embedding_fn,
        )
    
    return _collection


def add_to_kb(doc_id: str, content: dict, metadata: dict = None):
    collection = get_kb()
    try:
        collection.upsert(
            documents=[json.dumps(content)],
            ids=[doc_id],
            metadatas=[metadata or {}],
        )
        logger.info(f"Knowledge base updated: {doc_id}")
    except Exception as e:
        logger.warning(f"KB add failed: {e}")


def query_kb(query: str, n_results: int = 3) -> list[str]:
    collection = get_kb()
    try:
        count = collection.count()
        if count == 0:
            return []
        
        results = collection.query(
            query_texts=[query],
            n_results=min(n_results, count),
        )
        return results["documents"][0] if results["documents"] else []
    except Exception as e:
        logger.warning(f"KB query failed: {e}")
        return []