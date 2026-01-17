import chromadb
import os
from sentence_transformers import SentenceTransformer

class RAGStore:
    def __init__(self, chroma_dir: str):
            self.client = chromadb.PersistentClient(path=chroma_dir)
            self.collection = self.client.get_or_create_collection(
                name="iceberg_inference_rag",
                metadata={"hnsw:space": "cosine"}
            )
            
            # Prioritize getting from EMBEDDING_MODEL_PATH env var set by Docker
            model_path = os.getenv("EMBEDDING_MODEL_PATH", "/app/models/embedding_model")
            print(f"[rag_store] Loading embedding model from: {model_path}")
            
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Model not found at {model_path}. Check Docker build process.")
                
            self.embedder = SentenceTransformer(model_path)
    def upsert_documents(self, docs, batch_size: int = 5000):
        """Use upsert to update or insert new, avoiding data duplication"""
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i+batch_size]
            texts = [d["text"] for d in batch]
            ids = [d["id"] for d in batch]
            metadatas = [d.get("metadata", {}) for d in batch]
            embeddings = self.embedder.encode(texts).tolist()
            
            self.collection.upsert(
                documents=texts,
                metadatas=metadatas,
                ids=ids,
                embeddings=embeddings
            )
            print(f"[rag_store] Upserted batch {i//batch_size+1}, size={len(batch)}")

    def query(self, question: str, k: int = 6, filter_metadata: dict = None):
        """STEP 3: Support Hybrid Search (combine search with Metadata filtering)"""
        query_emb = self.embedder.encode([question]).tolist()[0]
        
        # Execute query with 'where' parameter to filter exact metadata (cam_id, date...)
        res = self.collection.query(
            query_embeddings=[query_emb],
            n_results=k,
            where=filter_metadata
        )
        
        hits = []
        if res["ids"] and len(res["ids"][0]) > 0:
            for i in range(len(res["ids"][0])):
                hits.append({
                    "id": res["ids"][0][i],
                    "text": res["documents"][0][i],
                    "metadata": res["metadatas"][0][i],
                    "distance": res["distances"][0][i] if "distances" in res else None
                })
        return hits