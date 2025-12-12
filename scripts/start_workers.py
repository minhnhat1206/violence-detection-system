import csv
import requests
import sys
import os

# ================= CẤU HÌNH =================

# Thay thế bằng địa chỉ IP Host thực tế (ví dụ: 192.168.0.200)
# API phải được gọi bằng IP Host vì script này chạy bên ngoài Docker
API_HOST_IP = "192.168.0.200"
API_PORT = 8000
METADATA_FILE = "../data/metadata/camera_registry.csv"

# Địa chỉ API hoàn chỉnh
API_URL = f"http://{API_HOST_IP}:{API_PORT}"

# ================= HÀM XỬ LÝ =================

def load_camera_registry(csv_path):
    """Đọc camera ID và RTSP URL từ file CSV."""
    registry = []
    # Dùng os.path.dirname(__file__) để xác định đường dẫn tương đối
    full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), csv_path)
    
    if not os.path.exists(full_path):
        print(f"LOI: Khong tim thay file metadata tai {full_path}")
        print("Vui long dam bao da chay metadataRTSP.py truoc.")
        return registry

    try:
        with open(full_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                registry.append({
                    'camera_id': row['camera_id'],
                    'rtsp_url': row['rtsp_url']
                })
        return registry
    except Exception as e:
        print(f"LOI khi doc CSV: {e}")
        return []

def start_all_workers(registry):
    """Gửi yêu cầu POST đến API để khởi động từng Worker."""
    
    TOTAL_CAMERAS = 5
    success_count = 0
    
    print(f"--- KICH HOAT {TOTAL_CAMERAS} WORKER AI QUA API ---")
    print(f"Gui den API: {API_URL}")
    print("-------------------------------------------------")
    
    for cam in registry:
        cam_id = cam['camera_id']
        rtsp_url = cam['rtsp_url']
        
        # URL API đầy đủ
        endpoint = f"{API_URL}/camera/start"
        
        # Tham số POST: camera_id và rtsp_url
        params = {
            "camera_id": cam_id,
            "rtsp_url": rtsp_url
        }
        
        try:
            # Dùng timeout để tránh bị treo
            response = requests.post(endpoint, params=params, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                print(f"[SUCCESS] {cam_id}: STARTED (PID: {result.get('pid', 'N/A')})")
                success_count += 1
            else:
                print(f"[ERROR] {cam_id}: LOI - API tra ve Status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            # Loai bo icon/emoji, thay bang [ERROR]
            print(f"[ERROR] {cam_id}: LOI KET NOI - {e}")
            
    print("-------------------------------------------------")
    # Su dung bien TOTAL_CAMERAS
    print(f"TONG KET: {success_count}/{TOTAL_CAMERAS} Workers duoc kich hoat.")
    
    if success_count < TOTAL_CAMERAS:
        print("CHU Y: Neu nhieu Worker that bai, co the Worker Process dang bi loi GPU hoac loi ket noi RTSP noi bo.")


if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    
    camera_list = load_camera_registry(METADATA_FILE)
    if camera_list:
        start_all_workers(camera_list)
    else:
        print("Khong co camera nao duoc load. Da dung.")