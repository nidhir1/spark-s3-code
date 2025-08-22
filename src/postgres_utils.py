import psycopg2
from psycopg2 import sql
from myconfig import settings

def write_to_postgres(df, table_name: str, mode: str = "overwrite"):
    """
    Create a table in Postgres if it doesn't exist and write a Spark DataFrame to it.
    :param df: Spark DataFrame
    :param table_name: Target table name
    :param mode: 'overwrite' | 'append' | 'ignore' | 'error'
    """
    # 1️⃣ Ensure table exists
    try:
        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            database=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        # All columns as TEXT for simplicity
        columns_sql = ", ".join([f"{col} TEXT" for col in df.columns])
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(columns_sql)
        )
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Table '{table_name}' ensured in Postgres")
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        return

    # 2️⃣ Write DataFrame to Postgres
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
