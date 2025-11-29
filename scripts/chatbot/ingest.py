import os, io, uuid
import pyarrow.parquet as pq
import pandas as pd
from minio import Minio

def connect_minio():
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # Với minio >=7.0.0 phải truyền endpoint bằng keyword
    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def list_parquet_objects(client: Minio, bucket: str, prefix: str):
    return [
        o.object_name
        for o in client.list_objects(
            bucket_name=bucket,
            prefix=prefix,
            recursive=True
        )
        if o.object_name.endswith(".parquet")
    ]

def read_parquet_df(client: Minio, bucket: str, key: str) -> pd.DataFrame:
    # gọi get_object với keyword arguments
    resp = client.get_object(bucket_name=bucket, object_name=key)
    data = resp.read()
    table = pq.read_table(io.BytesIO(data))
    return table.to_pandas()

def build_text_row(row: pd.Series) -> str:
    parts = []
    parts.append(f"Event {row.get('event_id', '')} on camera {row.get('camera_id', '')}")
    parts.append(f"Time (UTC): {row.get('timestamp_utc', '')} | Date: {row.get('date', '')}")
    parts.append(f"Label: {row.get('label', '')} | Score: {row.get('score', '')}")
    parts.append(f"Model: {row.get('model_name', '')} v{row.get('model_version', '')} | Latency(ms): {row.get('latency_ms', '')}")
    extra = row.get('extra', None)
    if isinstance(extra, dict) and extra.get('notes'):
        parts.append(f"Notes: {extra['notes']}")
    frame = row.get('frame_s3_path', '')
    if frame:
        parts.append(f"Frame: {frame}")
    return " | ".join([str(p) for p in parts if p])

def dataframe_to_docs(df: pd.DataFrame):
    docs = []
    for _, row in df.iterrows():
        text = build_text_row(row)
        unique_id = str(uuid.uuid4())
        metadata = {
            "event_id": row.get("event_id", None),
            "camera_id": row.get("camera_id", None),
            "date": row.get("date", None),
            "label": row.get("label", None),
            "score": float(row.get("score")) if pd.notnull(row.get("score")) else None,
            "frame_s3_path": row.get("frame_s3_path", None)
        }
        docs.append({"id": unique_id, "text": text, "metadata": metadata})
    return docs

def run_ingest():
    bucket = os.getenv("ICEBERG_BUCKET")
    prefix = os.getenv("ICEBERG_PREFIX")
    if not bucket or not prefix:
        raise RuntimeError("Missing ICEBERG_BUCKET or ICEBERG_PREFIX env vars.")

    client = connect_minio()
    keys = list_parquet_objects(client, bucket, prefix)
    frames = []
    for k in keys:
        try:
            df = read_parquet_df(client, bucket, k)
            frames.append(df)
            print(f"[ingest] Loaded {k} rows={len(df)}")
        except Exception as e:
            print(f"[ingest] Failed to read {k}: {e}")

    if not frames:
        print("[ingest] No dataframes loaded.")
        return []

    big_df = pd.concat(frames, ignore_index=True)
    docs = dataframe_to_docs(big_df)
    print(f"[ingest] Built {len(docs)} documents.")
    return docs
