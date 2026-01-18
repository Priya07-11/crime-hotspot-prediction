

import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "chicago_crimes.csv")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_chicago_crimes.csv")

OUTPUT_REPORT_PATH = os.path.join(BASE_DIR, "outputs", "reports", "analysis_summary.txt")
HEATMAP_OUTPUT = os.path.join(BASE_DIR, "outputs", "maps", "crime_hotspot_heatmap.html")
