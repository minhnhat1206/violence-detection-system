
import random
import csv
from pathlib import Path

# ====== GEOGRAPHIC CONFIG (District 1, Ho Chi Minh City) ======
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
# Range: Latitude ~ 10.77 to 10.78; Longitude ~ 106.69 to 106.71
# Use Seed to ensure consistent coordinates between runs
random.seed(42) 
LAT_BASE = [round(random.uniform(10.770, 10.785), 5) for _ in range(32)]
LON_BASE = [round(random.uniform(106.690, 106.710), 5) for _ in range(32)]

# Risk level distribution
RISK_LEVELS = ['high'] * 4 + ['medium'] * 8 + ['low'] * 4 # 4+8+4 = 16 camera

def generate_and_save_metadata(n_cameras: int, rtsp_base: str, metadata_file: Path):
    """
    Generate geographic metadata for cameras and write to CSV file.
    """
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    metadata_rows = []
    
    # Ensure RISK_LEVELS covers enough cameras
    risk_list = (RISK_LEVELS * (n_cameras // len(RISK_LEVELS) + 1))[:n_cameras]
    
    for i in range(n_cameras):
        # cam_id = f"cam_{i+1:02d}"
        cam_id = f"cam{i+1:02d}"

        risk = risk_list[i]
        
        metadata_rows.append({
            "camera_id": cam_id,
            "city": CITY,
            "district": DISTRICT,
            "ward": WARDS[i % len(WARDS)],
            "street": STREETS[i % len(STREETS)],
            "latitude": LAT_BASE[i], 
            "longitude": LON_BASE[i],
            "rtsp_url": f"{rtsp_base}/{cam_id}",
            "risk_level": risk # Add risk level info
        })

    fieldnames = [
        "camera_id", "city", "district", "ward", "street", "latitude", "longitude", 
        "rtsp_url", "risk_level"
    ]
    
    with open(metadata_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata_rows)
        
    print(f" Generated metadata for {len(metadata_rows)} cameras and wrote to: {metadata_file.resolve()}")
    
    return metadata_rows

def load_camera_registry(metadata_file: Path):
    """Read camera_registry CSV file and return dictionary."""
    registry = {}
    if not metadata_file.exists():
        print(f"ERROR: Metadata file not found at {metadata_file.resolve()}")
        return registry
        
    with open(metadata_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            registry[row['camera_id']] = row
    return registry

if __name__ == '__main__':
    # Test run only
    generate_and_save_metadata(16, "rtsp://mediamtx:8554", Path("../data/metadata/camera_registry.csv"))