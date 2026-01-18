# src/train_model.py
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

def train_hotspot_model(df):

    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features"
    )

    feature_vector = assembler.transform(df)

    kmeans = KMeans(
        k=15,
        seed=42,
        featuresCol="features"
    )

    model = kmeans.fit(feature_vector)

    clustered_df = model.transform(feature_vector)

    print("✔ Spark clustering completed")

    #  NO FILE WRITE
    return clustered_df.select("latitude", "longitude", "prediction")
