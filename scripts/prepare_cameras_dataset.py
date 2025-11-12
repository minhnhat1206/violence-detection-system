import os
import random
import shutil
import csv
from pathlib import Path

# ====== CẤU HÌNH ======
BASE_DIR = Path("./data")
RAW_PATH = BASE_DIR / "raw" / "RWF-2000" / "train"
PROCESSED_PATH = BASE_DIR / "processed" / "clips_for_streaming"
METADATA_PATH = BASE_DIR / "metadata"

N_CAMERAS = 15
MAX_CLIPS_PER_CAMERA = 4  # mỗi camera sẽ có 2..MAX_CLIPS_PER_CAMERA clip
RTSP_BASE = "rtsp://mediamtx:8554" # Thay localhost thành mediamtx cho môi trường docker

# Xác định vị trí chi tiết (Quận 1, TP. Hồ Chí Minh)
CITY = "TP. Hồ Chí Minh"
DISTRICT = "Quận 1"
WARDS = [
    "Phường Bến Nghé", "Phường Nguyễn Thái Bình", "Phường Bến Thành",
    "Phường Cầu Ông Lãnh", "Phường Phạm Ngũ Lão", "Phường Tân Định",
    "Phường Đa Kao", "Phường Bến Thành (2)", "Phường Nguyễn Cư Trinh",
    "Phường Cầu Kho", "Phường Tân Định (2)", "Phường Nguyễn Thái Bình (2)",
    "Phường Phạm Ngũ Lão (2)", "Phường Bến Nghé (2)", "Phường Đa Kao (2)"
]
STREETS = [
    "Đường Nguyễn Huệ", "Đường Lê Lợi", "Đường Nguyễn Thái Học",
    "Đường Lê Thánh Tôn", "Đường Pasteur", "Đường Trần Hưng Đạo",
    "Đường Đồng Khởi", "Đường Hai Bà Trưng", "Đường Nguyễn Du",
    "Đường Võ Văn Kiệt", "Đường Nguyễn Công Trứ", "Đường Công Trường Mê Linh",
    "Đường Hàm Nghi", "Đường Nguyễn Bỉnh Khiêm", "Đường Trương Định"
]

# Thêm Kinh độ và Vĩ độ giả định (Quận 1, HCMC)
# Phạm vi: Vĩ độ ~ 10.77 đến 10.78; Kinh độ ~ 106.69 đến 106.71
LATITUDES = [round(random.uniform(10.770, 10.785), 5) for _ in range(N_CAMERAS)]
LONGITUDES = [round(random.uniform(106.690, 106.710), 5) for _ in range(N_CAMERAS)]

# ====== Tạo thư mục đầu ra nếu chưa tồn tại ======
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
METADATA_PATH.mkdir(parents=True, exist_ok=True)

# ====== Đọc video nguồn ======
fight_dir = RAW_PATH / "Fight"
nonfight_dir = RAW_PATH / "NonFight"

fight_videos = sorted(list(fight_dir.glob("*.avi")))
nonfight_videos = sorted(list(nonfight_dir.glob("*.avi")))

if not fight_videos:
    raise FileNotFoundError(f"Không tìm thấy file .avi trong {fight_dir.resolve()}")
if not nonfight_videos:
    raise FileNotFoundError(f"Không tìm thấy file .avi trong {nonfight_dir.resolve()}")

# ====== Helper: chọn playlist cho 1 camera ======
def make_playlist(fight_pool, normal_pool, max_clips=4, prob_include_fight=0.6):
    """
    Trả về list Path của các clip (Path object).
    - Số clip giữa 2 và max_clips
    - 60% xác suất chọn ít nhất 1 clip bạo lực 
    """
    n_clips = random.randint(2, max_clips)
    include_fight = random.random() < prob_include_fight

    if include_fight and len(fight_pool) > 0:
        # lấy 1 clip bạo lực + (n_clips-1) clip thường
        fight_choice = random.sample(fight_pool, k=1)
        remain = max(0, n_clips - len(fight_choice))
        normal_choice = random.sample(normal_pool, k=remain) if remain > 0 else []
        clips = fight_choice + normal_choice
    else:
        # tất cả là clip thường
        clips = random.sample(normal_pool, k=min(n_clips, len(normal_pool)))

    random.shuffle(clips)
    return clips

# ====== Tạo metadata cho N_CAMERAS ======
metadata_rows = []
used_names = set()

for i in range(N_CAMERAS):
    cam_id = f"cam_{i+1:02d}"
    ward = WARDS[i % len(WARDS)]
    street = STREETS[i % len(STREETS)]
    lat = LATITUDES[i] # Lấy tọa độ tương ứng
    lon = LONGITUDES[i]
    
    # Tạo playlist
    playlist_paths = make_playlist(fight_videos, nonfight_videos, max_clips=MAX_CLIPS_PER_CAMERA)
    copied_names = []
    has_violence = False

    for j, clip_path in enumerate(playlist_paths, start=1):
        base_name = clip_path.name
        dest_name = f"{cam_id}_{j:02d}_{base_name}"
        if dest_name in used_names:
            suffix = 1
            name_only = f"{cam_id}_{j:02d}_{base_name}"
            while f"{name_only}_{suffix}" in used_names:
                suffix += 1
            dest_name = f"{name_only}_{suffix}"
        used_names.add(dest_name)
        # copy file sang processed
        dest_path = PROCESSED_PATH / dest_name
        shutil.copy2(clip_path, dest_path)
        copied_names.append(dest_name)
        # check nếu clip gốc là fight (tên nằm trong thư mục fight)
        if clip_path.parent.name.lower() in ("fight", "violence"):
            has_violence = True

    metadata_rows.append({
        "camera_id": cam_id,
        "city": CITY,
        "district": DISTRICT,
        "ward": ward,
        "street": street,
        "latitude": lat,  # <--- Đã thêm
        "longitude": lon, # <--- Đã thêm
        "playlist": "|".join(copied_names),
        "rtsp_url": f"{RTSP_BASE}/{cam_id}",
        "total_clips": len(copied_names),
        "has_violence": str(has_violence)
    })

# ====== Ghi CSV metadata ======
csv_file = METADATA_PATH / "camera_registry.csv"
# Cập nhật fieldnames
fieldnames = [
    "camera_id", "city", "district", "ward", "street", "latitude", "longitude", 
    "playlist", "rtsp_url", "total_clips", "has_violence"
]
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(metadata_rows)

print(f" Đã tạo metadata cho {len(metadata_rows)} camera tại {DISTRICT}, {CITY}")
print(f" Video sao chép vào: {PROCESSED_PATH.resolve()}")
print(f" File metadata: {csv_file.resolve()}")
