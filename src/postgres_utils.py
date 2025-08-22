import os
from src.spark_jobs import create_spark_session
from myconfig import settings

def write_to_postgres(df, table_name: str, mode: str = "overwrite"):
    """
    Write a Spark DataFrame to Postgres table.
    :param df: Spark DataFrame
    :param table_name: Target table name
    :param mode: 'overwrite' | 'append' | 'ignore' | 'error'
    """
    try:
        df.write \
            .format("jdbc") \
            .option("url", settings.POSTGRES_URL) \
            .option("dbtable", table_name) \
            .option("user", settings.POSTGRES_USER) \
            .option("password", settings.POSTGRES_PASSWORD) \
            .option("driver", settings.POSTGRES_DRIVER) \
            .mode(mode) \
            .save()
        print(f"✅ Data written to Postgres table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to write to Postgres: {e}")
