import folium

def create_map(lat, lon, zoom=12):
    return folium.Map(location=[lat, lon], zoom_start=zoom)
