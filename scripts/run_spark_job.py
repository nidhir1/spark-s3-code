import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.spark_jobs import create_spark_session
from src.s3_utils import list_files

if __name__ == "__main__":
    spark = create_spark_session()
    print("✅ Spark session created with MinIO S3 support!")

    bucket_name = "mybucket"
    files = list_files(bucket_name)

    if files:
        print(f"Files in bucket {bucket_name}:")
        for f in files:
            print(f"- {f}")
    else:
        print(f"No files found in bucket '{bucket_name}'")
