
import folium
from folium.plugins import HeatMap
import os
import numpy as np

def generate_heatmap_python(pdf):
    """
    Enhanced crime hotspot visualization using Python (Folium)
    Spark is used ONLY for processing
    """

    os.makedirs("outputs/maps", exist_ok=True)

    center_lat = pdf.latitude.mean()
    center_lon = pdf.longitude.mean()

    #  Base map
    base_map = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles="OpenStreetMap"
    )

    #  Dark mode tiles (safe)
    folium.TileLayer(
        tiles="CartoDB dark_matter",
        name="Dark Mode"
    ).add_to(base_map)

    #  Terrain tiles (FIXED with attribution)
    folium.TileLayer(
        tiles="https://stamen-tiles.a.ssl.fastly.net/terrain/{z}/{x}/{y}.png",
        name="Terrain",
        attr="Map tiles by Stamen Design, CC BY 3.0 — Map data © OpenStreetMap contributors"
    ).add_to(base_map)

    #  Heatmap layer
    HeatMap(
        data=pdf[["latitude", "longitude"]].values.tolist(),
        radius=14,
        blur=20,
        min_opacity=0.35,
        name="Crime Intensity Heatmap"
    ).add_to(base_map)

    #  Cluster markers
    if "prediction" in pdf.columns:
        cluster_colors = [
            "red", "blue", "green", "purple", "orange",
            "darkred", "cadetblue", "darkgreen", "pink",
            "lightblue", "lightgreen", "gray"
        ]

        for cluster_id in sorted(pdf["prediction"].unique()):
            cluster_df = pdf[pdf["prediction"] == cluster_id]

            cluster_group = folium.FeatureGroup(
                name=f"Cluster {cluster_id} (Crimes: {len(cluster_df)})"
            )

            sample_size = min(120, len(cluster_df))

            for _, row in cluster_df.sample(sample_size, random_state=42).iterrows():
                folium.CircleMarker(
                    location=[row.latitude, row.longitude],
                    radius=3,
                    color=cluster_colors[int(cluster_id) % len(cluster_colors)],
                    fill=True,
                    fill_opacity=0.65,
                    popup=f"Cluster {cluster_id}"
                ).add_to(cluster_group)

            cluster_group.add_to(base_map)

    #  Legend
    legend_html = """
    <div style="
        position: fixed;
        bottom: 40px;
        left: 40px;
        width: 190px;
        background-color: white;
        border: 2px solid grey;
        z-index: 9999;
        font-size: 14px;
        padding: 10px;
    ">
        <b>Crime Intensity</b><br>
        🔴 High Crime<br>
        🟡 Medium Crime<br>
        🔵 Low Crime
    </div>
    """
    base_map.get_root().html.add_child(folium.Element(legend_html))

    # 🎛 Layer control
    folium.LayerControl(collapsed=False).add_to(base_map)

    #  Save
    map_path = "outputs/maps/crime_hotspot_heatmap.html"
    base_map.save(map_path)

    print(f"✔ Enhanced heatmap saved at {map_path}")
