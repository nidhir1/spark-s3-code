# scripts/run_load_to_s3.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.load_to_s3 import upload_csvs, list_bucket, delete_csvs

if __name__ == "__main__":
    bucket_name = "mybucket"
    local_folder = "data/"   # Update to your local CSV folder path
    prefix = "data3"

    # Upload all CSVs from local folder
    upload_csvs(bucket_name, local_folder, prefix)

    # List objects in bucket
    list_bucket(bucket_name)

    # Optional: delete uploaded CSVs
    # delete_csvs(bucket_name)
    # list_bucket(bucket_name)
