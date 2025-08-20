# src/load_csv.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.spark_jobs import create_spark_session
from src.s3_utils import list_files

def load_csvs(bucket_name):
    """
    Load all CSV files from the given S3 bucket into separate Spark DataFrames.
    Each DataFrame is stored in a dictionary with key = df_<filename_without_extension>
    """
    spark = create_spark_session()
    files = list_files(bucket_name)
    
    if not files:
        print(f"No files found in bucket '{bucket_name}'")
        return {}

    dfs = {}
    for file_key in files:
        if file_key.endswith(".csv"):
            # Create DataFrame
            df = spark.read.option("header", True).csv(f"s3a://{bucket_name}/{file_key}")
            
            # Generate variable name: df_<filename_without_extension>
            var_name = "df_" + os.path.splitext(os.path.basename(file_key))[0].replace("-", "_")
            dfs[var_name] = df
            
            print(f"✅ Loaded '{file_key}' into '{var_name}' with {df.count()} rows and {len(df.columns)} columns")
    
    return dfs
