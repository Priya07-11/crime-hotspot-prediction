from src.config import OUTPUT_REPORT_PATH
from pyspark.sql.functions import col, count, to_timestamp, to_date
import os

def analyze_patterns(df):
    os.makedirs(os.path.dirname(OUTPUT_REPORT_PATH), exist_ok=True)

    # ===== Step 0: Normalize column names =====
    df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

    # ===== Step 1: Safely convert 'date' column to timestamp =====
    df = df.withColumn(
        "timestamp_safe",
        to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a")
    ).withColumn(
        "date_safe",
        to_date(col("timestamp_safe"))
    )

    # ===== Step 2: Top 10 primary crime types =====
    top_crimes = df.groupBy("primary_type") \
                   .agg(count("*").alias("count")) \
                   .orderBy(col("count").desc())

    # ===== Step 3: Top 10 community areas by crime count =====
    top_areas = df.groupBy("community_area") \
                  .agg(count("*").alias("count")) \
                  .orderBy(col("count").desc())

    # ===== Step 4: Top 10 districts by crime count =====
    top_districts = df.groupBy("district") \
                      .agg(count("*").alias("count")) \
                      .orderBy(col("count").desc())

    # ===== Step 5: Save summary report =====
    with open(OUTPUT_REPORT_PATH, "w") as f:
        f.write("Top 10 Crime Types:\n")
        for row in top_crimes.limit(10).collect():
            f.write(f"{row['primary_type']}: {row['count']}\n")

        f.write("\nTop 10 Community Areas:\n")
        for row in top_areas.limit(10).collect():
            f.write(f"{row['community_area']}: {row['count']}\n")

        f.write("\nTop 10 Districts:\n")
        for row in top_districts.limit(10).collect():
            f.write(f"{row['district']}: {row['count']}\n")

    print(f"Analysis summary saved at {OUTPUT_REPORT_PATH}")
    return df
