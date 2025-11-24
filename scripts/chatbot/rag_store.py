import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

class RAGStore:
    def __init__(self, chroma_dir: str):
        self.client = chromadb.Client(Settings(
            persist_directory=chroma_dir,
            anonymized_telemetry=False,
        ))
        try:
            self.client.delete_collection("iceberg_inference_rag")
            print("[rag_store] Deleted old collection iceberg_inference_rag")
        except Exception:
            print("[rag_store] No old collection to delete")

        self.collection = self.client.create_collection(
            name="iceberg_inference_rag",
            metadata={"hnsw:space": "cosine"}
        )
        self.embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def upsert_documents(self, docs, batch_size: int = 5000):
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

    def query(self, question: str, k: int = 6):
        query_emb = self.embedder.encode([question]).tolist()[0]
        res = self.collection.query(
            query_embeddings=[query_emb],
            n_results=k
        )
        hits = []
        for i in range(len(res["ids"][0])):
            hits.append({
                "id": res["ids"][0][i],
                "text": res["documents"][0][i],
                "metadata": res["metadatas"][0][i],
                "distance": res["distances"][0][i] if "distances" in res else None
            })
        return hits
