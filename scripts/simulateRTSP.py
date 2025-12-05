import os
import random
import subprocess
import time
import threading
import glob
from natsort import natsorted 

# ================= CẤU HÌNH HỆ THỐNG =================
# Đường dẫn tới thư mục video (Bạn hãy sửa lại cho đúng máy bạn)
# VIOLENCE_DIR = "../data/raw/RWF-2000/norTrain/Fight" 
# NON_VIOLENCE_DIR = "../data/raw/RWF-2000/norTrain/NonFight"

VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/Fight" 
NON_VIOLENCE_DIR = "/app/data/raw/RWF-2000/norTrain/NonFight"

# Đường dẫn tới thư mục chứa các file playlist được tạo ra (Mới)
PLAYLIST_DIR = "/app/data/playlist" 

# Cấu hình RTSP Server
# RTSP_SERVER_URL = "rtsp://localhost:8554"
RTSP_SERVER_URL = "rtsp://103.78.3.32:8554"



# Số lượng camera muốn giả lập
NUM_CAMERAS = 16

# Thời lượng demo (tạo playlist cho bao nhiêu phút?)
SIMULATION_DURATION_MINUTES = 60 

# ================= XỬ LÝ DỮ LIỆU =================

class VideoManager:
    def __init__(self, v_dir, nv_dir):
        self.v_events = self._load_and_group_videos(v_dir)
        self.nv_events = self._load_and_group_videos(nv_dir)
        print(f"Loaded {len(self.v_events)} violence events and {len(self.nv_events)} non-violence events.")

    def _load_and_group_videos(self, directory):
        """
        Nhóm các video rời rạc thành bộ. 
        Ví dụ: 'abc_0.avi', 'abc_1.avi' -> Key: 'abc', Value: ['path/abc_0.avi', 'path/abc_1.avi']
        """
        groups = {}
        # Lấy tất cả file video (avi, mp4...)
        files = [f for f in os.listdir(directory) if f.endswith(('.avi', '.mp4', '.mkv'))]
        
        for f in files:
            # Giả định tên file là: ID_Index.avi (VD: nVdn1krnTCQ_0.avi)
            try:
                # Tách tên để lấy ID (phần trước dấu _ cuối cùng)
                base_name = "_".join(f.split('_')[:-1])
                
                # --- SỬA LỖI QUAN TRỌNG: Chuyển sang đường dẫn tuyệt đối (Absolute Path) ---
                # Điều này giải quyết xung đột khi FFmpeg đọc file playlist được đặt ở thư mục khác.
                full_path = os.path.abspath(os.path.join(directory, f))
                # -------------------------------------------------------------------------
                
                if base_name not in groups:
                    groups[base_name] = []
                groups[base_name].append(full_path)
            except:
                continue
        
        # Sắp xếp các file trong từng group theo thứ tự 0, 1, 2... (Sử dụng natsort để sort số tự nhiên)
        final_events = []
        for key in groups:
            sorted_files = natsorted(groups[key])
            final_events.append(sorted_files)
            
        return final_events # List các List paths

    def get_random_event(self, is_violence=False):
        source = self.v_events if is_violence else self.nv_events
        if not source: return []
        return random.choice(source)

# ================= GIẢ LẬP CAMERA =================

class CameraSimulator:
    def __init__(self, cam_id, video_manager, risk_level):
        self.cam_id = cam_id
        self.vm = video_manager
        self.risk_level = risk_level # 'high', 'medium', 'low'
        
        # --- SỬA ĐỔI: Định nghĩa đường dẫn file playlist mới ---
        playlist_filename = f"playlist_cam_{cam_id}.txt"
        self.playlist_file = os.path.join(PLAYLIST_DIR, playlist_filename)
        
        self.rtsp_url = f"{RTSP_SERVER_URL}/cam{cam_id:02d}"

    def generate_scenario(self):
        """Tạo kịch bản phát sóng dựa trên mức độ rủi ro"""
        print(f"Generating scenario for Camera {self.cam_id} ({self.risk_level})...")
        
        # Cấu hình xác suất dựa trên Risk Level
        if self.risk_level == 'high':
            # Hotspot: Cứ 3-5 phút lại có biến
            avg_safe_duration = (3 * 60, 5 * 60) 
            violence_prob = 1.0 # Chắc chắn có violence xen kẽ
        elif self.risk_level == 'medium':
            # Public: 10-15 phút mới có biến
            avg_safe_duration = (10 * 60, 15 * 60)
            violence_prob = 0.8
        else: # low
            # Safe zone: Gần như không có, hoặc 30-45p mới có
            avg_safe_duration = (30 * 60, 45 * 60)
            violence_prob = 0.3

        playlist = []
        current_time = 0
        target_time = SIMULATION_DURATION_MINUTES * 60

        while current_time < target_time:
            # 1. Giai đoạn Bình yên (Non-violence)
            safe_time_target = random.randint(*avg_safe_duration)
            segment_time = 0
            while segment_time < safe_time_target:
                event = self.vm.get_random_event(is_violence=False)
                playlist.extend(event)
                segment_time += len(event) * 5 # Giả định mỗi clip 5s
            current_time += segment_time

            # 2. Giai đoạn Bạo lực (Violence) - Nếu trúng xác suất
            if random.random() < violence_prob:
                # Mục tiêu: Tạo ra vụ ẩu đả kéo dài khoảng 20s - 60s
                violence_duration_target = random.randint(20, 60)
                v_time = 0
                while v_time < violence_duration_target:
                    event = self.vm.get_random_event(is_violence=True)
                    playlist.extend(event) # Thêm cả bộ (0, 1, 2...)
                    v_time += len(event) * 5
                current_time += v_time

        # Ghi ra file playlist đúng format FFmpeg
        with open(self.playlist_file, 'w') as f:
            for video_path in playlist:
                # Bây giờ video_path đã là đường dẫn tuyệt đối, rất an toàn
                clean_path = video_path.replace('\\', '/')
                f.write(f"file '{clean_path}'\n")

    def start_streaming(self):
        """Gọi FFmpeg để đẩy stream"""
        # Đã sửa đổi để dùng H.264 và TCP (giúp giải quyết lỗi I/O Timeout trước đó)
        cmd = [
            'ffmpeg',
            '-re', 
            '-f', 'concat',
            '-safe', '0',
            '-i', self.playlist_file,
            
            # Cấu hình Encode H.264 để stream ổn định
            '-c:v', 'libx264',       
            '-preset', 'ultrafast',  
            '-tune', 'zerolatency',  
            '-b:v', '1000k',         
            '-f', 'rtsp',
            '-rtsp_transport', 'tcp', # Ép dùng TCP
            
            self.rtsp_url
        ]
        
        print(f"Starting Stream Cam {self.cam_id} -> {self.rtsp_url}")
        # Chạy subprocess ẩn. None để hiện lỗi nếu FFmpeg crash.
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=None)

# ================= MAIN =================

def main():
    # 0. Đảm bảo thư mục playlist tồn tại (Mới)
    if not os.path.exists(PLAYLIST_DIR):
        print(f"Creating playlist directory: {PLAYLIST_DIR}")
        os.makedirs(PLAYLIST_DIR)
        
    # 1. Load dữ liệu
    if not os.path.exists(VIOLENCE_DIR) or not os.path.exists(NON_VIOLENCE_DIR):
        print("Lỗi: Không tìm thấy thư mục dữ liệu video!")
        return

    vm = VideoManager(VIOLENCE_DIR, NON_VIOLENCE_DIR)
    
    simulators = []
    processes = []

    # 2. Phân bổ Camera (4 Hotspot, 8 Public, 4 Safe)
    risk_distribution = ['high']*4 + ['medium']*8 + ['low']*4
    
    # Tạo playlist cho từng camera
    for i in range(NUM_CAMERAS):
        risk = risk_distribution[i] if i < len(risk_distribution) else 'low'
        sim = CameraSimulator(i+1, vm, risk)
        sim.generate_scenario()
        simulators.append(sim)

    print("--- Đã tạo xong kịch bản, bắt đầu stream ---")

    # 3. Chạy Stream song song
    for sim in simulators:
        p = sim.start_streaming()
        processes.append(p)
        time.sleep(1) # Delay nhẹ để không overload CPU lúc khởi động

    print(f"Hệ thống đang chạy {NUM_CAMERAS} camera giả lập.")
    print("Nhấn Ctrl+C để dừng hệ thống.")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nĐang dừng hệ thống...")
        for p in processes:
            p.kill()
        
        # --- SỬA ĐỔI: Xóa file tạm trong thư mục PLAYLIST_DIR ---
        for f in glob.glob(os.path.join(PLAYLIST_DIR, "playlist_cam_*.txt")):
            try:
                os.remove(f)
            except OSError as e:
                print(f"Error deleting file {f}: {e}")
        # --------------------------------------------------------
        
        print("Đã dọn dẹp xong.")

if __name__ == "__main__":
    main()