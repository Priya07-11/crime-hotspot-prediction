from config import spark, RAW_DATA_PATH, PROCESSED_DATA_PATH
from pyspark.sql.functions import col, to_date
import os

def ingest_crime_data():
    print(f"Reading dataset from {RAW_DATA_PATH}")
    df = spark.read.csv(RAW_DATA_PATH, header=True, inferSchema=True)
    print(f"Initial rows: {df.count()} | Columns: {len(df.columns)}")

    # ===== Normalize column names =====
    new_columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.toDF(*new_columns)
    print(f"Normalized columns: {df.columns}")

    # ===== Convert date columns =====
    if "date" in df.columns:
        df = df.withColumn("date", to_date(col("date")))
    if "updated_on" in df.columns:
        df = df.withColumn("updated_on", to_date(col("updated_on")))

    # ===== Drop rows with null latitude/longitude/primary_type =====
    df = df.dropna(subset=["latitude", "longitude", "primary_type"])

    # ===== Convert numeric columns to int =====
    for c in ["year", "ward", "district", "community_area"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("int"))

    print(f"Rows after cleaning: {df.count()}")
    return df

def save_processed_data(df):
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    
    # ===== Save as single CSV to avoid Hadoop/Windows errors =====
    df.coalesce(1).write.csv(PROCESSED_DATA_PATH, header=True, mode="overwrite")
    
    print(f"Processed data saved at {PROCESSED_DATA_PATH}")
