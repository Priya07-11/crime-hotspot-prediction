#  Chicago Crime Hotspot Prediction

A big-data analytics project that identifies and visualizes crime hotspots in Chicago using **Apache Spark for large-scale processing** and **Python for interactive visualization**.

---

##  Project Overview

This project analyzes historical Chicago crime data to:
- Discover crime patterns
- Identify geographical crime hotspots using clustering
- Visualize hotspots interactively on a map

Apache Spark is used for scalable data processing, while Python is used for visualization.

---

##  Technologies Used

- Apache Spark (PySpark)
- Python
- Spark MLlib (KMeans)
- Pandas
- Folium
- Git & GitHub

##  Project Structure

crime-hotspot-prediction/
│
├── data/
│ └── raw/
│ └── chicago_crimes.csv
│
├── src/
│ ├── config.py
│ ├── spark_analysis.py
│ ├── feature_engineering.py
│ ├── train_model.py
│ └── python_visualization.py
│
├── outputs/
│ ├── models/
│ ├── maps/
│ │ └── crime_hotspot_heatmap.html
│ └── reports/
│
├── main.py
├── README.md
└── requirements.txt


## 🔄 Workflow

1. Load raw crime data using Spark  
2. Clean and engineer features  
3. Apply KMeans clustering for hotspot detection  
4. Generate interactive heatmap using Python

---

 1️ Create virtual environment
```bash
python -m venv .venv
source .venv/Scripts/activate

2️ Install dependencies
pip install -r requirements.txt

3️ Run the pipeline
python main.py

4 Output

Clustered crime data processed using Spark

Interactive crime heatmap generated at:

outputs/maps/crime_hotspot_heatmap.html


Open the HTML file in any browser to view the visualization.

 5 Dataset

Chicago Crime Dataset (publicly available):

🔗 https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2

The dataset contains reported crime incidents in Chicago with location, time, and crime type details.


---

##  Final Step
After updating README:

```bash
git add README.md
git commit -m "Updated README with dataset information"
git push origin main


