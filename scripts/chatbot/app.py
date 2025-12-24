import os
import re
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import google.generativeai as genai
from rag_store import RAGStore
from ingest import run_ingest
import json
from flask_cors import CORS

load_dotenv()

CHROMA_DIR = os.getenv("CHROMA_DIR", "/data/chroma")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

app = Flask(__name__)
CORS(app)
rag = RAGStore(CHROMA_DIR)

def extract_filters(question: str):
    """Hàm bổ trợ trích xuất metadata từ câu hỏi để thực hiện Hybrid Search"""
    filters = {}
    
    # 1. Trích xuất camera_id (ví dụ: cam01, cam02...)
    cam_match = re.search(r'cam\d+', question.lower())
    if cam_match:
        filters["camera_id"] = cam_match.group()
        
    # 2. Trích xuất ngày tháng (định dạng YYYY-MM-DD)
    date_match = re.search(r'\d{4}-\d{2}-\d{2}', question)
    if date_match:
        filters["date"] = date_match.group()
        
    return filters if filters else None

def init_data_incremental():
    """Khởi động: Chỉ nạp những sự kiện mới từ MinIO chưa có trong database"""
    print("[app] Checking for new data in MinIO...")
    try:
        # Lấy danh sách event_id đã tồn tại để tránh trùng lặp
        current_data = rag.collection.get(include=['metadatas'])
        existing_ids = set()
        if current_data and current_data['metadatas']:
            existing_ids = {str(m.get('event_id')) for m in current_data['metadatas'] if m.get('event_id')}
        
        print(f"[app] Found {len(existing_ids)} events already in database.")

        # Chỉ Ingest những dữ liệu chưa có
        new_docs = run_ingest(existing_ids=existing_ids)
        
        if new_docs:
            rag.upsert_documents(new_docs)
            print(f"[app] Successfully indexed {len(new_docs)} NEW documents.")
        else:
            print("[app] Database is up-to-date. No new events added.")
            
    except Exception as e:
        import traceback
        print("[app] ERROR during startup ingest:", e)
        traceback.print_exc()

# Thực hiện nạp dữ liệu ngay khi khởi chạy Server
init_data_incremental()

# Cấu hình Gemini Client
model = None
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

def synthesize_answer(question: str, hits: list) -> str:
    if not hits:
        return "Hệ thống không tìm thấy dữ liệu nào phù hợp với yêu cầu của bạn."
    
    context = "\n\n".join([f"- {h['text']}" for h in hits])
    if model:
        prompt = f"""Bạn là Trợ lý Giám sát An ninh. Hãy trả lời câu hỏi dựa trên các ngữ cảnh sau.
Nếu dữ liệu có số liệu cụ thể (score, camera ID, thời gian), hãy trích dẫn chính xác.
Câu hỏi: {question}

Ngữ cảnh:
{context}

Trả lời chi tiết bằng tiếng Việt, tập trung vào các sự kiện bạo lực được ghi nhận.  Nếu câu hỏi không liên quan đến dữ liệu, hãy trả lời rằng bạn chỉ có thể hỗ trợ các câu hỏi liên quan đến giám sát an ninh."""        
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            return f"Lỗi gọi LLM: {e}\n\nDữ liệu tìm thấy:\n{context}"
    return f"Kết quả truy xuất:\n{context}"

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "docs_indexed": rag.collection.count()}), 200

@app.route("/chat", methods=["POST"])
def chat():
    try:
        raw = request.data.decode("utf-8")
        if not raw:
            return jsonify({"error": "Request body trống"}), 400

        payload = json.loads(raw)

        question = payload.get("question", "").strip()
        if not question:
            return jsonify({"error": "Vui lòng nhập câu hỏi"}), 400

        metadata_filters = extract_filters(question)
        hits = rag.query(question, k=6, filter_metadata=metadata_filters)
        answer = synthesize_answer(question, hits)

        return jsonify({
            "question": question,
            "answer": answer,
            "filters_applied": metadata_filters,
            "data_sources": hits
        }), 200

    except json.JSONDecodeError as e:
        return jsonify({
            "error": "JSON không hợp lệ",
            "raw_body": raw,
            "detail": str(e)
        }), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Server chạy trên cổng 5002 như cấu hình Docker
    app.run(host="0.0.0.0", port=5002, debug=False)