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

# API_HOST_IP = "192.168.0.200"
API_HOST_IP = "viomobilenet_api"
API_PORT = 8000
API_URL = f"http://{API_HOST_IP}:{API_PORT}"

TOTAL_TO_START = 8
SIMULATION_DURATION_MINUTES = 60

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
    def __init__(self, cam_id, rtsp_url, vm, risk_level):
        self.cam_id = cam_id
        self.rtsp_url = rtsp_url
        self.vm = vm
        self.risk_level = risk_level
        self.playlist = f"{PLAYLIST_DIR}/playlist_{cam_id}.txt"

    def generate_playlist(self):
        playlist = []
        current = 0
        target = SIMULATION_DURATION_MINUTES * 60

        config = {
            "high": {"prob": 0.8, "min_safe": 60, "max_safe": 120},
            "medium": {"prob": 0.5, "min_safe": 300, "max_safe": 600},
            "low": {"prob": 0.2, "min_safe": 900, "max_safe": 1800}
        }
        cfg = config.get(self.risk_level.lower(), config["low"])

        while current < target:
            # 1. Đoạn video AN TOÀN (Non-Violence)
            # Thêm logic loop cho clip an toàn để tránh chuyển cảnh quá nhanh
            safe_time_target = random.randint(cfg["min_safe"], cfg["max_safe"])
            t_safe = 0
            while t_safe < safe_time_target:
                ev = self.vm.random_event(False)
                if not ev: break
                
                # Loop mỗi clip an toàn 3 lần để hình ảnh ổn định
                for _ in range(3):
                    playlist.extend(ev)
                    t_safe += len(ev) * 5 # Giả định mỗi clip dài 5s
            current += t_safe

            # 2. Đoạn video BẠO LỰC (Violence)
            if random.random() < cfg["prob"]:
                ev = self.vm.random_event(True)
                if ev:
                    # YÊU CẦU: Kéo dài 1 - 2 phút (60s - 120s)
                    # Giả định mỗi clip gốc dài 5s -> cần 12 đến 24 lần lặp
                    loop_count = random.randint(12, 24)
                    
                    # Thêm vào playlist
                    for _ in range(loop_count):
                        playlist.extend(ev)
                    
                    actual_duration_sec = loop_count * 5
                    current += actual_duration_sec
                    
                    print(f"[SIM] {self.cam_id}: Injected bạo lực dài {actual_duration_sec}s (~{actual_duration_sec/60:.1f} phút)")

        with open(self.playlist, "w") as f:
            for p in playlist:
                f.write("file '{}'\n".format(p.replace("\\", "/")))

    def start(self):
        cmd = [
            "ffmpeg", "-re",
            "-f", "concat", "-safe", "0",
            "-i", self.playlist,
            "-c:v", "libx264",
            "-profile:v", "baseline",
            "-level", "3.1",
            "-bf", "0",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-pix_fmt", "yuv420p",
            "-an",
            "-f", "rtsp", "-rtsp_transport", "tcp",
            self.rtsp_url
        ]
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# ================= API & MAIN =================

def register_camera(cam_id, rtsp_url, risk_level):
    try:
        r = requests.post(
            f"{API_URL}/camera/start",
            params={"camera_id": cam_id, "rtsp_url": rtsp_url, "risk_level": risk_level},
            timeout=10
        )
        print(f"[API] {cam_id} ({risk_level}): Status {r.status_code}")
    except Exception as e:
        print(f"[API] {cam_id}: ERROR: {e}")

def main():
    os.makedirs(PLAYLIST_DIR, exist_ok=True)
    if not os.path.exists(METADATA_FILE): return

    with open(METADATA_FILE, newline='', encoding="utf-8") as f:
        registry = list(csv.DictReader(f))

    selected = registry[:TOTAL_TO_START]
    vm = VideoManager(VIOLENCE_DIR, NON_VIOLENCE_DIR)
    camera_processes = []

    for cam_info in selected:
        cid, curl, crisk = cam_info["camera_id"], cam_info["rtsp_url"], cam_info.get("risk_level", "low")
        sim = CameraSimulator(cid, curl, vm, crisk)
        sim.generate_playlist()
        proc = sim.start()
        camera_processes.append({"camera_id": cid, "rtsp_url": curl, "risk_level": crisk, "process": proc, "simulator": sim})
        print(f"[RTSP] {cid} started.")
        time.sleep(2)
        register_camera(cid, curl, crisk)

    try:
        while True:
            time.sleep(10)
            for i, cam in enumerate(camera_processes):
                if cam["process"].poll() is not None:
                    new_proc = cam["simulator"].start()
                    camera_processes[i]["process"] = new_proc
    except KeyboardInterrupt:
        for cam in camera_processes: cam["process"].kill()

if __name__ == "__main__":
    main()