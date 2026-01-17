# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from src.spark_analysis import analyze_patterns
from src.config import RAW_DATA_PATH, PROCESSED_DATA_PATH
import os

def load_data(spark, path):
    """Load raw CSV data and fix date parsing."""
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Convert 'date' column to timestamp safely
    df = df.withColumn("date", to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"))

    # Drop rows with malformed or null dates
    df = df.dropna(subset=["date"])

    # Normalize column names: lowercase + underscores
    df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

    return df

def save_data(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.coalesce(1).write.mode("overwrite").csv(path, header=True)
    print(f"Processed data saved at {path}")

def main():
    print("===== Starting Chicago Crime Hotspot Pipeline =====")

    # ===== Step 1: Initialize Spark =====
    spark = SparkSession.builder \
        .appName("ChicagoCrimeHotspot") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ===== Step 2: Load raw data =====
    print("\n[Step 1] Ingesting data...")
    df = load_data(spark, RAW_DATA_PATH)
    print(f"Initial rows: {df.count()} | Columns: {len(df.columns)}")

    print("→ Analysis reports will still be generated")

    # ===== Step 4: Analyze patterns =====
    print("\n[Step 2] Analyzing patterns...")
    try:
        analyze_patterns(df)
    except Exception as e:
        print(f"Error during analysis: {e}")

    print("\n===== Pipeline finished =====")
    spark.stop()

if __name__ == "__main__":
    main()
