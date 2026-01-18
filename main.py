# main.py
from pyspark.sql import SparkSession
from src.config import RAW_DATA_PATH
from src.spark_analysis import analyze_patterns
from src.feature_engineering import create_features
from src.train_model import train_hotspot_model
from src.python_visualization import generate_heatmap_python
import os


def main():
    print("===== Starting Chicago Crime Hotspot Pipeline =====")

    #  Windows-safe temp directories
    os.makedirs("C:/temp/spark-warehouse", exist_ok=True)
    os.makedirs("C:/temp/spark-local", exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("ChicagoCrimeHotspot")
        .master("local[*]")

        #  CRITICAL WINDOWS FIXES
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse")
        .config("spark.local.dir", "C:/temp/spark-local")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    print("✔ Spark session created")

    # ---------------- LOAD DATA ----------------
    df = spark.read.csv(RAW_DATA_PATH, header=True, inferSchema=True)

    # Normalize column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().replace(" ", "_"))

    df = df.dropna(subset=["latitude", "longitude", "primary_type"])
    print(f"✔ Data loaded: {df.count()} rows")

    # ---------------- ANALYSIS ----------------
    analyze_patterns(df)

    # ---------------- FEATURE ENGINEERING ----------------
    features_df = create_features(df)

    # ---------------- MODEL TRAINING (SPARK) ----------------
    clustered_df = train_hotspot_model(features_df)

    # ---------------- VISUALIZATION (PYTHON) ----------------
    print("✔ Converting Spark DataFrame to Pandas for visualization")

    pdf = (
        clustered_df
        .limit(50000)   #  Memory-safe
        .toPandas()
    )

    generate_heatmap_python(pdf)

    # ---------------- STOP SPARK ----------------
    spark.stop()
    print("===== Pipeline Completed Successfully =====")


if __name__ == "__main__":
    main()
