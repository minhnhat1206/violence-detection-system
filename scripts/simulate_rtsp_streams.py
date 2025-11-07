# #!/usr/bin/env python3
# """
# simulate_rtsp_streams.py

# Mục đích:
#  - Đọc data/metadata/camera_registry.csv
#  - Tạo file concat list (ffmpeg) cho từng camera
#  - Spawn ffmpeg process cho mỗi camera để stream playlist lên rtsp://localhost:8554/<camera_id>
#  - Lưu logs và PIDs để dễ quản lý / dừng

# Cách dùng (PowerShell / CMD):
#   python .\scripts\simulate_rtsp_streams.py start
#   python .\scripts\simulate_rtsp_streams.py stop
#   python .\scripts\simulate_rtsp_streams.py status

# Lưu ý:
#  - Cần cài ffmpeg và docker (để chạy MediaMTX).
#  - Chạy script từ đâu cũng được — nó tự tìm project root dựa vào vị trí file này.
# """

# import csv
# import subprocess
# import sys
# from pathlib import Path
# import time
# import os
# import signal

# # ====== Determine project base path reliably ======
# # BASE = parent of the directory where this script file is located.
# BASE = Path(__file__).resolve().parent.parent

# # ====== Paths relative to project root ======
# PROCESSED = BASE / "data" / "processed" / "clips_for_streaming"
# METADATA = BASE / "data" / "metadata" / "camera_registry.csv"
# TMP_DIR = BASE / "tmp" / "concat_lists"
# LOG_DIR = BASE / "logs"
# PID_FILE = BASE / "pids" / "simulate_rtsp_streams.pids"

# RTSP_BASE = "rtsp://localhost:8554"

# # ensure dirs
# TMP_DIR.mkdir(parents=True, exist_ok=True)
# LOG_DIR.mkdir(parents=True, exist_ok=True)
# PID_FILE.parent.mkdir(parents=True, exist_ok=True)

# FFMPEG_BIN = "ffmpeg"  # ensure ffmpeg is in PATH

# def create_concat_file(camera_id, clip_names):
#     """
#     Create a concat file for ffmpeg (format: file '/full/path/..' per line)
#     Returns path to concat file.
#     """
#     concat_path = TMP_DIR / f"list_{camera_id}.txt"
#     with open(concat_path, "w", encoding="utf-8") as f:
#         # concat demuxer requires lines like: file '/full/path'
#         for name in clip_names:
#             p = PROCESSED / name
#             f.write(f"file '{p.resolve().as_posix()}'\n")
#     return concat_path

# def start_stream(camera_id, concat_file, log_file):

#     out_url = f"{RTSP_BASE}/{camera_id}"
#     cmd = [
#         FFMPEG_BIN,
#         "-re",
#         "-f", "concat",
#         "-safe", "0",
#         "-stream_loop", "-1",
#         "-i", str(concat_file),

#         # encode video to H264
#         "-c:v", "libx264",
#         "-preset", "veryfast",
#         "-tune", "zerolatency",
#         "-profile:v", "baseline",
#         "-pix_fmt", "yuv420p",
#         "-vf", "scale=640:-2",
#         "-r", "15",
#         "-b:v", "600k",
#         "-maxrate", "600k",
#         "-bufsize", "1200k",

#         # audio
#         "-c:a", "aac",
#         "-b:a", "64k",

#         # force RTSP transport over TCP
#         "-rtsp_transport", "tcp",

#         # output
#         "-f", "rtsp",
#         out_url
#     ]

#     lf = open(log_file, "ab")
#     proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
#     return proc, lf



# def start_all():
#     procs = []
#     files_handles = []
#     if not METADATA.exists():
#         print(f"[ERROR] Metadata file not found: {METADATA}")
#         return

#     with open(METADATA, newline="", encoding="utf-8") as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             cam = row["camera_id"]
#             playlist_field = row.get("playlist", "")
#             if not playlist_field:
#                 print(f"[WARN] camera {cam} has empty playlist, skip.")
#                 continue
#             clip_names = playlist_field.split("|")
#             # verify files exist
#             missing = [n for n in clip_names if not (PROCESSED / n).exists()]
#             if missing:
#                 print(f"[ERROR] missing files for {cam}: {missing}. Skipping this camera.")
#                 continue
#             concat_file = create_concat_file(cam, clip_names)
#             log_file = LOG_DIR / f"{cam}.log"
#             print(f"[INFO] Starting stream {cam} -> {RTSP_BASE}/{cam}, playlist items: {len(clip_names)}")
#             proc, lf = start_stream(cam, concat_file, log_file)
#             procs.append((cam, proc.pid))
#             files_handles.append(lf)
#             # small delay to avoid starting all at once
#             time.sleep(0.25)

#     # write pids
#     with open(PID_FILE, "w", encoding="utf-8") as f:
#         for cam, pid in procs:
#             f.write(f"{cam},{pid}\n")
#     print(f"[OK] Started {len(procs)} ffmpeg processes. PID file: {PID_FILE}")
#     print("Use 'python .\\scripts\\simulate_rtsp_streams.py stop' to stop them.")

#     # Note: file handles remain open to keep logs alive while processes run.
#     # We won't close them here; OS will close when script exits. If you need cleanup, call stop_all().

# def stop_all():
#     if not PID_FILE.exists():
#         print("[WARN] No PID file found. Nothing to stop.")
#         return
#     lines = PID_FILE.read_text(encoding="utf-8").strip().splitlines()
#     stopped = 0
#     for line in lines:
#         if not line.strip():
#             continue
#         cam, pid_s = line.split(",", 1)
#         try:
#             pid = int(pid_s)
#             print(f"[INFO] Stopping {cam} (pid {pid})")
#             # terminate process
#             os.kill(pid, signal.SIGTERM)
#             stopped += 1
#         except ProcessLookupError:
#             print(f"[WARN] Process {pid_s} not found, might have exited already.")
#         except Exception as e:
#             print(f"[ERROR] Failed to stop pid {pid_s}: {e}")

#     # remove pid file
#     try:
#         PID_FILE.unlink()
#     except Exception:
#         pass

#     print(f"[OK] Stopped {stopped} processes.")

# def status():
#     if not PID_FILE.exists():
#         print("[INFO] No PID file found. No running streams started by this script?")
#         return
#     print("[INFO] Current running processes from PID file:")
#     lines = PID_FILE.read_text(encoding="utf-8").strip().splitlines()
#     for line in lines:
#         if not line.strip():
#             continue
#         cam, pid_s = line.split(",", 1)
#         try:
#             pid = int(pid_s)
#             # check if process exists
#             os.kill(pid, 0)
#             print(f" - {cam} -> pid {pid} (running)")
#         except ProcessLookupError:
#             print(f" - {cam} -> pid {pid_s} (not found)")
#         except Exception as e:
#             print(f" - {cam} -> pid {pid_s} (error: {e})")

# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("Usage: python simulate_rtsp_streams.py [start|stop|status]")
#         sys.exit(1)
#     cmd = sys.argv[1].lower()
#     if cmd == "start":
#         start_all()
#     elif cmd == "stop":
#         stop_all()
#     elif cmd == "status":
#         status()
#     else:
#         print("Unknown command. Use start|stop|status")
#!/usr/bin/env python3
"""
simulate_rtsp_streams.py - Dành cho Container Docker

Mục đích: Đọc clips (.avi) từ Volume Mount và stream lên MediaMTX.
"""

import csv
import subprocess
import sys
from pathlib import Path
import time
import os
import signal

# ====== Determine project base path reliably - Dành cho môi trường Docker ======
CONTAINER_ROOT = Path("/app")

# ====== Paths relative to container root (Phải khớp với Volume Mounts) ======
PROCESSED = CONTAINER_ROOT / "data" / "processed" / "clips_for_streaming"
METADATA = CONTAINER_ROOT / "data" / "metadata" / "camera_registry.csv"
TMP_DIR = CONTAINER_ROOT / "tmp" 
LOG_DIR = CONTAINER_ROOT / "logs"
PID_FILE = CONTAINER_ROOT / "pids" / "simulate_rtsp_streams.pids"

# ĐỌC ĐỊA CHỈ RTSP BASE TỪ BIẾN MÔI TRƯỜNG CỦA DOCKER COMPOSE
RTSP_BASE = os.getenv("RTSP_BASE", "rtsp://localhost:8554")

# ensure dirs
TMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
PID_FILE.parent.mkdir(parents=True, exist_ok=True)

FFMPEG_BIN = "ffmpeg" 

def create_concat_file(camera_id, clip_names):
    """
    Create a concat file for ffmpeg (format: file '/full/path/..' per line)
    """
    concat_path = TMP_DIR / f"list_{camera_id}.txt"
    with open(concat_path, "w", encoding="utf-8") as f:
        for name in clip_names:
            p = PROCESSED / name
            # Sử dụng đường dẫn tuyệt đối (resolve().as_posix()) cho FFmpeg
            f.write(f"file '{p.resolve().as_posix()}'\n")
    return concat_path

def start_stream(camera_id, concat_file, log_file):

    out_url = f"{RTSP_BASE}/{camera_id}"
    cmd = [
        FFMPEG_BIN,
        "-re",
        "-f", "concat",
        "-safe", "0",
        "-stream_loop", "-1",
        "-i", str(concat_file), # Input video (playlist)
        
        # Thêm Nguồn Audio Rỗng để tránh lỗi khi clips .avi không có audio
        "-f", "lavfi",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=44100", 
        
        # encode video to H264
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-profile:v", "baseline",
        "-pix_fmt", "yuv420p",
        "-vf", "scale=640:-2",
        "-r", "15",
        "-b:v", "600k",
        "-maxrate", "600k",
        "-bufsize", "1200k",

        # audio
        "-c:a", "aac",
        "-b:a", "64k",
        "-shortest", # Quan trọng: Dùng độ dài video làm giới hạn
        
        # force RTSP transport over TCP
        "-rtsp_transport", "tcp",

        # output
        "-f", "rtsp",
        out_url
    ]

    lf = open(log_file, "ab")
    # Thay đổi stdout/stderr để nó không bị chuyển hướng ngay lập tức (dễ dàng debug hơn)
    # Tuy nhiên, để giữ tiến trình chạy ngầm, chúng ta giữ nguyên:
    proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
    return proc, lf


def start_all():
    procs = []
    files_handles = []
    
    # In thông báo để đảm bảo chúng ta thấy log này
    print("[INFO] Starting RTSP pusher script...")

    if not METADATA.exists():
        print(f"[ERROR] Metadata file not found: {METADATA}. Vui lòng kiểm tra Volume Mount.")
        return

    with open(METADATA, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cam = row["camera_id"]
            playlist_field = row.get("playlist", "")
            if not playlist_field:
                print(f"[WARN] camera {cam} has empty playlist, skip.")
                continue
            clip_names = playlist_field.split("|")
            
            missing = [n for n in clip_names if not (PROCESSED / n).exists()]
            if missing:
                print(f"[ERROR] missing files for {cam}: {missing}. Skipping this camera.")
                continue
            
            concat_file = create_concat_file(cam, clip_names)
            log_file = LOG_DIR / f"{cam}.log"
            print(f"[INFO] Starting stream {cam} -> {RTSP_BASE}/{cam}, playlist items: {len(clip_names)}")
            
            proc, lf = start_stream(cam, concat_file, log_file)
            procs.append((cam, proc.pid))
            files_handles.append(lf)
            time.sleep(0.25)

    # write pids
    with open(PID_FILE, "w", encoding="utf-8") as f:
        for cam, pid in procs:
            f.write(f"{cam},{pid}\n")
    print(f"[OK] Started {len(procs)} ffmpeg processes. PID file: {PID_FILE}")
    print("[INFO] Entering infinite loop to keep container alive...")

    # THAY ĐỔI: Thêm vòng lặp vô hạn để giữ container chạy.
    # Nếu script Python thoát, Container Docker sẽ dừng.
    try:
        while True:
            # Dành cho mục đích kiểm tra, chúng ta có thể kiểm tra trạng thái tiến trình
            time.sleep(5)
    except KeyboardInterrupt:
        pass # Thoát khi người dùng gửi tín hiệu dừng (Ctrl+C)


def stop_all():
    # ... (giữ nguyên phần stop_all)
    if not PID_FILE.exists():
        print("[WARN] No PID file found. Nothing to stop.")
        return
    lines = PID_FILE.read_text(encoding="utf-8").strip().splitlines()
    stopped = 0
    for line in lines:
        if not line.strip():
            continue
        cam, pid_s = line.split(",", 1)
        try:
            pid = int(pid_s)
            print(f"[INFO] Stopping {cam} (pid {pid})")
            # terminate process
            os.kill(pid, signal.SIGTERM)
            stopped += 1
        except ProcessLookupError:
            print(f"[WARN] Process {pid_s} not found, might have exited already.")
        except Exception as e:
            print(f"[ERROR] Failed to stop pid {pid_s}: {e}")

    # remove pid file
    try:
        PID_FILE.unlink()
    except Exception:
        pass

    print(f"[OK] Stopped {stopped} processes.")

def status():
    # ... (giữ nguyên phần status)
    if not PID_FILE.exists():
        print("[INFO] No PID file found. No running streams started by this script?")
        return
    print("[INFO] Current running processes from PID file:")
    lines = PID_FILE.read_text(encoding="utf-8").strip().splitlines()
    for line in lines:
        if not line.strip():
            continue
        cam, pid_s = line.split(",", 1)
        try:
            pid = int(pid_s)
            # check if process exists
            os.kill(pid, 0)
            print(f" - {cam} -> pid {pid} (running)")
        except ProcessLookupError:
            print(f" - {cam} -> pid {pid_s} (not found)")
        except Exception as e:
            print(f" - {cam} -> pid {pid_s} (error: {e})")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python simulate_rtsp_streams.py [start|stop|status]")
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "start":
        start_all()
    elif cmd == "stop":
        stop_all()
    elif cmd == "status":
        status()
    else:
        print("Unknown command. Use start|stop|status")