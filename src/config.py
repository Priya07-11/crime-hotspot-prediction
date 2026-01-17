from pyspark.sql import SparkSession
import os

# Paths
RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), '../data/raw/chicago_crimes.csv')
PROCESSED_DATA_PATH = os.path.join(os.path.dirname(__file__), '../data/processed/cleaned_chicago_crimes.csv')
OUTPUT_REPORT_PATH = os.path.join(os.path.dirname(__file__), '../outputs/reports/analysis_summary.txt')
HEATMAP_OUTPUT = os.path.join(os.path.dirname(__file__), '../outputs/maps/crime_hotspot_heatmap.html')

# Spark Session
spark = SparkSession.builder \
    .appName("Chicago Crime Hotspot Analysis") \
    .getOrCreate()
