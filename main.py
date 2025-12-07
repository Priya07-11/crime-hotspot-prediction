from src.visualization.generate_heatmap import generate_heatmap

if __name__ == "__main__":
    INPUT_PATH = "data/processed/cleaned_crime_data.csv"
    OUTPUT_PATH = "output/crime_heatmap.html"

    print("=== Crime Heatmap Generator (Person B) ===")
    generate_heatmap(INPUT_PATH, OUTPUT_PATH)
    print("[DONE] Heatmap saved!")
