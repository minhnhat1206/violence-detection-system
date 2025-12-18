import os
import random
import subprocess
import time
import csv
import requests
from natsort import natsorted

# ================= CẤU HÌNH =================
VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/Fight"
NON_VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/NonFight"
PLAYLIST_DIR = "/app/data/playlist"
METADATA_FILE = "/app/data/metadata/camera_registry.csv"

API_HOST_IP = "192.168.0.200"
API_PORT = 8000
API_URL = f"http://{API_HOST_IP}:{API_PORT}"

TOTAL_TO_START = 5
SIMULATION_DURATION_MINUTES = 30

# ================= VIDEO MANAGER =================

class VideoManager:
    def __init__(self, v_dir, nv_dir):
        self.v_events = self._load_and_group_videos(v_dir)
        self.nv_events = self._load_and_group_videos(nv_dir)
        print(f"[*] Loaded {len(self.v_events)} violence & {len(self.nv_events)} non-violence events")

    def _load_and_group_videos(self, directory):
        groups = {}
        if not os.path.exists(directory):
            print(f"[!] Warning: Directory {directory} not found")
            return []
        for f in os.listdir(directory):
            if not f.endswith(('.mp4', '.avi')):
                continue
            base = "_".join(f.split('_')[:-1])
            groups.setdefault(base, []).append(os.path.join(directory, f))
        return [natsorted(v) for v in groups.values()]

    def random_event(self, violence=False):
        source = self.v_events if violence else self.nv_events
        return random.choice(source) if source else []

# ================= CAMERA SIM =================

class CameraSimulator:
    def __init__(self, cam_id, rtsp_url, vm):
        self.cam_id = cam_id
        self.rtsp_url = rtsp_url
        self.vm = vm
        self.playlist = f"{PLAYLIST_DIR}/playlist_{cam_id}.txt"

    def generate_playlist(self):
        playlist = []
        current = 0
        target = SIMULATION_DURATION_MINUTES * 60

        while current < target:
            safe = random.randint(300, 600)
            t = 0
            while t < safe:
                ev = self.vm.random_event(False)
                if not ev: break
                playlist.extend(ev)
                t += len(ev) * 5
            current += t

            if random.random() < 0.5:
                ev = self.vm.random_event(True)
                if ev:
                    playlist.extend(ev)
                    current += len(ev) * 5

        with open(self.playlist, "w") as f:
            for p in playlist:
                f.write("file '{}'\n".format(p.replace("\\", "/")))

    def start(self):
        cmd = [
            "ffmpeg", "-re",
            "-f", "concat", "-safe", "0",
            "-i", self.playlist,
            "-c", "copy",
            "-f", "rtsp", "-rtsp_transport", "tcp",
            self.rtsp_url
        ]
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# ================= API =================

def register_camera(cam_id, rtsp_url, risk_level):
    try:
        # Gửi kèm tham số risk_level qua query params
        r = requests.post(
            f"{API_URL}/camera/start",
            params={
                "camera_id": cam_id, 
                "rtsp_url": rtsp_url,
                "risk_level": risk_level 
            },
            timeout=10
        )
        print(f"[API] {cam_id} ({risk_level}): Status {r.status_code}")
    except Exception as e:
        print(f"[API] {cam_id}: ERROR Connecting to API: {e}")

# ================= MAIN =================

def main():
    os.makedirs(PLAYLIST_DIR, exist_ok=True)

    # 1. Load Registry
    if not os.path.exists(METADATA_FILE):
        print(f"[!] Critical Error: Metadata file not found at {METADATA_FILE}")
        return

    with open(METADATA_FILE, newline='', encoding="utf-8") as f:
        registry = list(csv.DictReader(f))

    print(f"[*] Total cameras in registry: {len(registry)}")
    selected = registry[:TOTAL_TO_START]

    vm = VideoManager(VIOLENCE_DIR, NON_VIOLENCE_DIR)
    
    # Danh sách lưu trữ thông tin đầy đủ của camera để quản lý
    camera_processes = []

    print(f"\n--- STARTING {len(selected)} CAMERAS ---")

    for cam_info in selected:
        cid = cam_info["camera_id"]
        curl = cam_info["rtsp_url"]
        crisk = cam_info.get("risk_level", "low") # Lấy mức độ rủi ro, mặc định là low

        # Khởi tạo simulator
        sim = CameraSimulator(cid, curl, vm)
        sim.generate_playlist()
        
        # Chạy FFmpeg
        proc = sim.start()
        
        # Lưu vào danh sách quản lý
        camera_processes.append({
            "camera_id": cid,
            "rtsp_url": curl,
            "risk_level": crisk, # Lưu risk_level để dùng khi restart
            "process": proc,
            "simulator": sim
        })

        print(f"[RTSP] {cid} started -> {curl}")
        
        # Đợi 3 giây để stream khởi tạo ổn định trước khi báo API
        time.sleep(3)
        register_camera(cid, curl, crisk) # <-- Truyền risk_level vào hàm đăng ký

    print("\n[SYSTEM] All camera workers are triggered. Monitoring...")

    try:
        while True:
            time.sleep(10)
            for i, cam in enumerate(camera_processes):
                # Kiểm tra xem tiến trình FFmpeg có bị chết không
                if cam["process"].poll() is not None:
                    cid = cam["camera_id"]
                    curl = cam["rtsp_url"]
                    crisk = cam["risk_level"]
                    print(f"[WARN] {cid} stream stopped. Restarting with URL: {curl}")
                    
                    # Khởi động lại dùng ĐÚNG url của camera đó
                    new_proc = cam["simulator"].start()
                    camera_processes[i]["process"] = new_proc
                    
                    # Đăng ký lại với API (nếu cần thiết khi stream restart)
                    # register_camera(cid, curl, crisk)
                    
    except KeyboardInterrupt:
        print("\n[SYSTEM] Shutting down...")
        for cam in camera_processes:
            cam["process"].kill()
        print("[SYSTEM] Cleaned up all processes.")

if __name__ == "__main__":
    main()
# import os
# import random
# import subprocess
# import time
# import csv
# from natsort import natsorted

# # ================= CẤU HÌNH =================
# VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/Fight"
# NON_VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/NonFight"
# PLAYLIST_DIR = "/app/data/playlist"
# METADATA_FILE = "/app/data/metadata/camera_registry.csv"

# # Chỉ cần số lượng camera muốn giả lập luồng
# TOTAL_TO_START = 8
# SIMULATION_DURATION_MINUTES = 60

# # ================= VIDEO MANAGER =================

# class VideoManager:
#     def __init__(self, v_dir, nv_dir):
#         self.v_events = self._load_and_group_videos(v_dir)
#         self.nv_events = self._load_and_group_videos(nv_dir)
#         print(f"[*] Loaded {len(self.v_events)} violence & {len(self.nv_events)} non-violence events")

#     def _load_and_group_videos(self, directory):
#         groups = {}
#         if not os.path.exists(directory):
#             print(f"[!] Warning: Directory {directory} not found")
#             return []
#         for f in os.listdir(directory):
#             if not f.endswith(('.mp4', '.avi')):
#                 continue
#             base = "_".join(f.split('_')[:-1])
#             groups.setdefault(base, []).append(os.path.join(directory, f))
#         return [natsorted(v) for v in groups.values()]

#     def random_event(self, violence=False):
#         source = self.v_events if violence else self.nv_events
#         return random.choice(source) if source else []

# # ================= CAMERA SIM =================

# class CameraSimulator:
#     def __init__(self, cam_id, rtsp_url, vm):
#         self.cam_id = cam_id
#         self.rtsp_url = rtsp_url
#         self.vm = vm
#         self.playlist = f"{PLAYLIST_DIR}/playlist_{cam_id}.txt"

#     def generate_playlist(self):
#         playlist = []
#         current = 0
#         target = SIMULATION_DURATION_MINUTES * 60

#         while current < target:
#             safe = random.randint(300, 600)
#             t = 0
#             while t < safe:
#                 ev = self.vm.random_event(False)
#                 if not ev: break
#                 playlist.extend(ev)
#                 t += len(ev) * 5
#             current += t

#             if random.random() < 0.5:
#                 ev = self.vm.random_event(True)
#                 if ev:
#                     playlist.extend(ev)
#                     current += len(ev) * 5

#         with open(self.playlist, "w") as f:
#             for p in playlist:
#                 f.write("file '{}'\n".format(p.replace("\\", "/")))

#     def start(self):
#         cmd = [
#             "ffmpeg", "-re",
#             "-f", "concat", "-safe", "0",
#             "-i", self.playlist,
#             "-c", "copy",
#             "-f", "rtsp", "-rtsp_transport", "tcp",
#             self.rtsp_url
#         ]
#         return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# # ================= MAIN =================

# def main():
#     os.makedirs(PLAYLIST_DIR, exist_ok=True)

#     # 1. Load Registry
#     if not os.path.exists(METADATA_FILE):
#         print(f"[!] Critical Error: Metadata file not found at {METADATA_FILE}")
#         return

#     with open(METADATA_FILE, newline='', encoding="utf-8") as f:
#         registry = list(csv.DictReader(f))

#     print(f"[*] Total cameras in registry: {len(registry)}")
#     selected = registry[:TOTAL_TO_START]

#     vm = VideoManager(VIOLENCE_DIR, NON_VIOLENCE_DIR)
    
#     camera_processes = []

#     print(f"\n--- STARTING RTSP PUSH FOR {len(selected)} CAMERAS ---")

#     for cam_info in selected:
#         cid = cam_info["camera_id"]
#         curl = cam_info["rtsp_url"]

#         # Khởi tạo simulator & Tạo danh sách phát
#         sim = CameraSimulator(cid, curl, vm)
#         sim.generate_playlist()
        
#         # Chỉ chạy FFmpeg để đẩy luồng (Không gọi API)
#         proc = sim.start()
        
#         camera_processes.append({
#             "camera_id": cid,
#             "rtsp_url": curl,
#             "process": proc,
#             "simulator": sim
#         })

#         print(f"[RTSP] {cid} pushing -> {curl}")
        
#         # Delay nhẹ để tránh overload I/O khi khởi động nhiều FFmpeg cùng lúc
#         time.sleep(1)

#     print("\n[SYSTEM] All RTSP streams are active. Monitoring for crashes...")

#     try:
#         while True:
#             time.sleep(10)
#             for i, cam in enumerate(camera_processes):
#                 # Tự động khởi động lại nếu luồng FFmpeg bị ngắt
#                 if cam["process"].poll() is not None:
#                     cid = cam["camera_id"]
#                     curl = cam["rtsp_url"]
#                     print(f"[WARN] {cid} stream stopped. Restarting: {curl}")
                    
#                     new_proc = cam["simulator"].start()
#                     camera_processes[i]["process"] = new_proc
                    
#     except KeyboardInterrupt:
#         print("\n[SYSTEM] Stopping all streams...")
#         for cam in camera_processes:
#             cam["process"].kill()
#         print("[SYSTEM] Cleaned up.")

# if __name__ == "__main__":
#     main()