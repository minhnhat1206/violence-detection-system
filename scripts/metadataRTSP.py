
import random
import csv
from pathlib import Path

# ====== CẤU HÌNH ĐỊA LÝ (Quận 1, TP. Hồ Chí Minh) ======
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
# Phạm vi: Vĩ độ ~ 10.77 đến 10.78; Kinh độ ~ 106.69 đến 106.71
# Dùng Seed để đảm bảo tọa độ không đổi giữa các lần chạy
random.seed(42) 
LAT_BASE = [round(random.uniform(10.770, 10.785), 5) for _ in range(32)]
LON_BASE = [round(random.uniform(106.690, 106.710), 5) for _ in range(32)]

# Phân bổ mức độ rủi ro
RISK_LEVELS = ['high'] * 4 + ['medium'] * 8 + ['low'] * 4 # 4+8+4 = 16 camera

def generate_and_save_metadata(n_cameras: int, rtsp_base: str, metadata_file: Path):
    """
    Tạo metadata địa lý cho các camera và ghi ra file CSV.
    """
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    metadata_rows = []
    
    # Đảm bảo RISK_LEVELS bao phủ đủ số lượng camera
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
            "risk_level": risk # Thêm thông tin mức độ rủi ro
        })

    fieldnames = [
        "camera_id", "city", "district", "ward", "street", "latitude", "longitude", 
        "rtsp_url", "risk_level"
    ]
    
    with open(metadata_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata_rows)
        
    print(f"✅ Đã tạo metadata cho {len(metadata_rows)} camera và ghi vào: {metadata_file.resolve()}")
    
    return metadata_rows

def load_camera_registry(metadata_file: Path):
    """Đọc file CSV camera_registry và trả về dictionary."""
    registry = {}
    if not metadata_file.exists():
        print(f"LỖI: Không tìm thấy file metadata tại {metadata_file.resolve()}")
        return registry
        
    with open(metadata_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            registry[row['camera_id']] = row
    return registry

if __name__ == '__main__':
    # Chỉ chạy thử nghiệm
    generate_and_save_metadata(16, "rtsp://103.78.3.29:8554", Path("../data/metadata/camera_registry.csv"))