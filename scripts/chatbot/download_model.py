# VioMobileNet/scripts/chatbot/download_model.py
from sentence_transformers import SentenceTransformer
import os

model_name = "sentence-transformers/all-MiniLM-L6-v2"
save_path = "./models/embedding_model"

if not os.path.exists(save_path):
    print(f"--- Đang tải model {model_name} ---")
    model = SentenceTransformer(model_name)
    model.save(save_path)
    print(f"--- Đã lưu model tại {save_path} ---")
else:
    print("--- Model đã tồn tại ---")