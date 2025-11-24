import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import google.generativeai as genai
from rag_store import RAGStore
from ingest import run_ingest

load_dotenv()

CHROMA_DIR = os.getenv("CHROMA_DIR", "/data/chroma")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

app = Flask(__name__)
rag = RAGStore(CHROMA_DIR)

# Ingest on startup
docs = []
try:
    docs = run_ingest()
    if docs:
        rag.upsert_documents(docs)
        print(f"[app] Ingest completed and indexed {len(docs)} docs.")
    else:
        print("[app] No documents ingested.")
except Exception as e:
    import traceback
    print("[app] ERROR during ingest/upsert:", e)
    traceback.print_exc()

# Gemini client
model = None
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

def synthesize_answer(question: str, hits: list) -> str:
    if not hits:
        return "Không tìm thấy dữ liệu liên quan."
    context = "\n\n".join([f"- {h['text']}" for h in hits])
    if model:
        prompt = f"""Bạn là trợ lý. Hãy trả lời câu hỏi dựa trên ngữ cảnh.
Câu hỏi: {question}

Ngữ cảnh:
{context}

Trả lời ngắn gọn bằng tiếng Việt, có số liệu cụ thể nếu có."""
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"Lỗi khi gọi Gemini: {e}\n\nNgữ cảnh:\n{context}"
    return f"Các mục liên quan:\n{context}"

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "docs_indexed": len(docs)}), 200

@app.route("/chat", methods=["POST"])
def chat():
    try:
        payload = request.get_json(force=True) or {}
        question = payload.get("question", "").strip()
        if not question:
            return jsonify({"error": "question is required"}), 400

        hits = rag.query(question, k=6)
        answer = synthesize_answer(question, hits)

        return jsonify({
            "question": question,
            "hits": hits,
            "answer": answer
        }), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("[app] Starting Flask server on port 5002...")
    app.run(host="0.0.0.0", port=5002, debug=False)
