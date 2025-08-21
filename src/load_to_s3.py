# src/load_to_s3.py
import os
import shutil
import boto3
from myconfig import settings

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

def upload_csvs(bucket_name, local_folder, prefix="data3"):
    s3 = get_minio_client()
    
    # Ensure bucket exists
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")

    uploaded_folder = os.path.join(local_folder, "uploaded")
    os.makedirs(uploaded_folder, exist_ok=True)

    for file_name in os.listdir(local_folder):
        local_path = os.path.join(local_folder, file_name)
        if file_name.endswith(".csv") and os.path.isfile(local_path):
            key = f"{prefix}/{file_name}" if prefix else file_name
            s3.upload_file(local_path, bucket_name, key)
            print(f"Uploaded {file_name} to {bucket_name}/{key}")
            shutil.move(local_path, os.path.join(uploaded_folder, file_name))
            print(f"Moved {file_name} to {uploaded_folder}")

def delete_csvs(bucket_name):
    s3 = get_minio_client()
    objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    for obj in objects:
        if obj["Key"].endswith(".csv"):
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            print(f"Deleted {obj['Key']} from {bucket_name}")

def list_bucket(bucket_name):
    s3 = get_minio_client()
    objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    if not objects:
        print(f"Bucket '{bucket_name}' is empty.")
    else:
        print(f"Objects in {bucket_name}:")
        for obj in objects:
            print(" -", obj["Key"])
