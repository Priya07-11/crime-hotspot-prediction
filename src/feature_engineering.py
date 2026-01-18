# src/feature_engineering.py
from pyspark.sql.functions import col

def create_features(df):
    """
    Select only required features for hotspot modeling
    """
    features_df = df.select(
        col("latitude").cast("double"),
        col("longitude").cast("double")
    ).dropna()

    print(f"✔ Feature rows: {features_df.count()}")
    return features_df
