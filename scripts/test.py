import requests

# API trả về danh sách alerts
alerts = requests.get("http://localhost:3000/alerts").json()

# MinIO public URL prefix
MINIO_URL = "http://localhost:9000/rtsp-frames"

# Lấy ảnh đầu tiên
first_alert = alerts[0]
frame_path = first_alert["frame_s3_path"]
frame_url = f"{MINIO_URL}/{frame_path}"

print("Ảnh URL:", frame_url)

# Tải ảnh về file
img_data = requests.get(frame_url).content
with open("frame.jpg", "wb") as f:
    f.write(img_data)
print("Đã lưu ảnh vào frame.jpg")
