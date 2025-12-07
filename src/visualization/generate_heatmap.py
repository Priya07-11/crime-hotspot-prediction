import folium
from folium.plugins import HeatMap
import pandas as pd

def generate_heatmap(data_path, output_path="output/crime_heatmap.html"):
    print("[INFO] Loading processed data..")
    df = pd.read_csv(data_path)

    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError("Dataset must contain latitude and longitude columns.")

    map_center = [df["latitude"].mean(), df["longitude"].mean()]
    m = folium.Map(location=map_center, zoom_start=12)

    HeatMap(df[["latitude", "longitude"]].values.tolist()).add_to(m)

    print("[INFO] Heatmap generated, saving..")
    m.save(output_path)
