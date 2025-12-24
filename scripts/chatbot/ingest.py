import os, io, uuid
import pyarrow.parquet as pq
import pandas as pd
from minio import Minio

def connect_minio():
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def list_parquet_objects(client: Minio, bucket: str, prefix: str):
    # Thêm in log để debug chính xác Prefix đang quét
    print(f"[ingest] Scanning bucket: {bucket} with prefix: {prefix}")
    
    objects = client.list_objects(
        bucket_name=bucket,
        prefix=prefix,
        recursive=True
    )
    
    parquet_files = []
    for o in objects:
        # Kiểm tra nếu tên đối tượng chứa đuôi .parquet 
        # (Xử lý trường hợp lồng thư mục như trong ảnh của bạn)
        if ".parquet" in o.object_name and not o.is_dir:
            parquet_files.append(o.object_name)
            
    return parquet_files

def read_parquet_df(client: Minio, bucket: str, key: str) -> pd.DataFrame:
    resp = client.get_object(bucket_name=bucket, object_name=key)
    data = resp.read()
    table = pq.read_table(io.BytesIO(data))
    return table.to_pandas()

def build_text_row(row: pd.Series) -> str:
    event_id = row.get('event_id', 'không rõ ID')
    cam_id = row.get('camera_id', 'không rõ vị trí')
    
    # Lấy giá trị thời gian
    time_val = row.get('timestamp_utc')
    
    # Chuyển đổi Timestamp sang chuỗi (String) một cách an toàn
    if pd.isnull(time_val):
        time_str = "không rõ thời gian"
    else:
        # Bạn có thể dùng str(time_val) hoặc định dạng lại theo ý muốn .strftime('%Y-%m-%d %H:%M:%S')
        time_str = str(time_val)
    
    city = row.get('city', 'không rõ thành phố')
    district = row.get('district', 'không rõ quận/huyện')
    ward = row.get('ward', 'không rõ phường/xã')
    street = row.get('street', 'không rõ đường/phố')
    
    if city != 'không rõ thành phố' or district != 'không rõ quận/huyện' or ward != 'không rõ phường/xã' or street != 'không rõ đường/phố':
        location_parts = [part for part in [street, ward, district, city] if part and str(part).lower() != 'nan' and 'không rõ' not in str(part)]
        location_str = ", ".join(location_parts)
        # Sử dụng f-string ở đây là an toàn nhất
        time_str = f"{time_str} tại {location_str}"      

    # Chuẩn hóa nhãn hiển thị
    raw_label = str(row.get('is_violent_window', '')).upper()
    label = "BẠO LỰC" if raw_label == "TRUE" else "bình thường"
    
    score = row.get('score', 0)
    
    text = (
        f"Sự kiện {event_id} được ghi nhận tại camera {cam_id} vào lúc {time_str}. "
        f"Hệ thống phân tích hình ảnh đánh giá đây là hành vi {label} với mức độ bạo lực {score:.2f}."
    )
    
    extra = row.get('extra', None)
    if isinstance(extra, dict) and extra.get('notes'):
        text += f" Ghi chú hệ thống: {extra['notes']}."
        
    return text

def dataframe_to_docs(df: pd.DataFrame):
    docs = []
    for _, row in df.iterrows():
        text = build_text_row(row)
        unique_id = str(uuid.uuid4())
        metadata = {
            "event_id": str(row.get("event_id", "")),
            "camera_id": str(row.get("camera_id", "")),
            "date": str(row.get("date", "")),
            "label": str(row.get("label", "")),
            "score": float(row.get("score")) if pd.notnull(row.get("score")) else 0.0,
            "frame_s3_path": str(row.get("frame_s3_path", ""))
        }
        docs.append({"id": unique_id, "text": text, "metadata": metadata})
    return docs

def run_ingest(existing_ids=None):
    """BƯỚC 2: Hỗ trợ Incremental Ingest (chỉ nạp dữ liệu chưa tồn tại)"""
    bucket = os.getenv("ICEBERG_BUCKET")
    prefix = os.getenv("ICEBERG_PREFIX")
    if not bucket or not prefix:
        raise RuntimeError("Missing ICEBERG_BUCKET or ICEBERG_PREFIX env vars.")

    client = connect_minio()
    keys = list_parquet_objects(client, bucket, prefix)
    
    all_docs = []
    existing_ids = existing_ids or set()

    for k in keys:
        try:
            df = read_parquet_df(client, bucket, k)
            
            # Lọc bỏ các bản ghi đã có trong ChromaDB dựa trên event_id
            if not df.empty and existing_ids:
                df = df[~df['event_id'].astype(str).isin(existing_ids)]
            
            if not df.empty:
                docs = dataframe_to_docs(df)
                all_docs.extend(docs)
                print(f"[ingest] Loaded {k}: Found {len(docs)} new records.")
            else:
                print(f"[ingest] Skipped {k}: No new records.")
                
        except Exception as e:
            print(f"[ingest] Failed to read {k}: {e}")

    return all_docs