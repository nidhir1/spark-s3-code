# src/spark_jobs.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from myconfig import settings
from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Spark-MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.540") \
        .getOrCreate()
    return spark
