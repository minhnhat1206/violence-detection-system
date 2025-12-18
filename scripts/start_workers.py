import csv
import requests
import sys
import os

# ================= CẤU HÌNH =================

# Thay thế bằng địa chỉ IP Host thực tế của bạn
API_HOST_IP = "192.168.0.200"
API_PORT = 8000
METADATA_FILE = "../data/metadata/camera_registry.csv"

# gioi han so luong camera chay AI
TOTAL_TO_START = 8

API_URL = f"http://{API_HOST_IP}:{API_PORT}"

# ================= HÀM XỬ LÝ =================

def load_camera_registry(csv_path):
    """Doc camera ID va RTSP URL tu file CSV."""
    registry = []
    # lay duong dan tuyet doi den file csv
    current_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(current_dir, csv_path)
    
    if not os.path.exists(full_path):
        print(f"LOI: Khong tim thay file metadata tai {full_path}")
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

def start_selected_workers(registry):
    """Gui yeu cau POST den API de khoi dong tung Worker."""
    
    success_count = 0
    total_requested = len(registry)
    
    print(f"--- KICH HOAT {total_requested} WORKER AI QUA API ---")
    print(f"Gui den API: {API_URL}")
    print("-------------------------------------------------")
    
    for cam in registry:
        cam_id = cam['camera_id']
        rtsp_url = cam['rtsp_url']
        endpoint = f"{API_URL}/camera/start"
        
        params = {
            "camera_id": cam_id,
            "rtsp_url": rtsp_url
        }
        
        try:
            response = requests.post(endpoint, params=params, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                print(f"[SUCCESS] {cam_id}: STARTED (PID: {result.get('pid', 'N/A')})")
                success_count += 1
            else:
                print(f"[ERROR] {cam_id}: LOI - Status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] {cam_id}: LOI KET NOI - {e}")
            
    print("-------------------------------------------------")
    print(f"TONG KET: {success_count}/{total_requested} Workers duoc kich hoat.")

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    
    full_registry = load_camera_registry(METADATA_FILE)
    
    if full_registry:
        selected_cameras = full_registry[:TOTAL_TO_START]
        
        start_selected_workers(selected_cameras)
    else:
        print("Khong co camera nao duoc load. Da dung.")