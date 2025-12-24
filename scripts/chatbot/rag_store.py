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
            
            # Ưu tiên lấy từ biến môi trường EMBEDDING_MODEL_PATH do Docker thiết lập
            model_path = os.getenv("EMBEDDING_MODEL_PATH", "/app/models/embedding_model")
            print(f"[rag_store] Loading embedding model from: {model_path}")
            
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Không tìm thấy model tại {model_path}. Kiểm tra lại quá trình build Docker.")
                
            self.embedder = SentenceTransformer(model_path)
    def upsert_documents(self, docs, batch_size: int = 5000):
        """Sử dụng upsert để cập nhật hoặc thêm mới, tránh trùng lặp dữ liệu"""
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
        """BƯỚC 3: Hỗ trợ tìm kiếm kết hợp lọc Metadata (Hybrid Search)"""
        query_emb = self.embedder.encode([question]).tolist()[0]
        
        # Thực hiện truy vấn với tham số 'where' để lọc chính xác metadata (cam_id, date...)
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