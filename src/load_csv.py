# src/load_csv.py
import os
from src.spark_jobs import create_spark_session  # your fixed session
from src.s3_utils import list_files  # should list objects in bucket

def load_csvs(bucket_name: str, preview_rows: int = 3):
    """
    Load all CSV files from the given MinIO/S3 bucket into Spark DataFrames.
    Returns a dictionary mapping dynamic variable names to DataFrames.
    """
    spark = create_spark_session()
    files = list_files(bucket_name)

    if not files:
        print(f"⚠️ No files found in bucket '{bucket_name}'")
        return {}

    dfs = {}
    for file_key in files:
        if not file_key.lower().endswith(".csv"):
            continue

        path = f"s3a://{bucket_name}/{file_key}"
        print(f"📥 Loading '{file_key}'...")

        try:
            df = spark.read.option("header", True).csv(path)

            # Create safe variable name
            var_name = "df_" + os.path.splitext(os.path.basename(file_key))[0].replace("-", "_")
            dfs[var_name] = df

            # Log loaded DataFrame
            print(f"✅ Loaded '{file_key}' as '{var_name}' with columns: {df.columns}")
            df.show(preview_rows)

        except Exception as e:
            print(f"❌ Failed to load '{file_key}': {e}")

    return dfs
