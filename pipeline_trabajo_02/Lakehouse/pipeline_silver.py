# Databricks pipeline source
"""
Archivo: pipeline_silver.py

Capa Silver:
Transforma la información cruda de Bronze en una tabla limpia, tipada y
orientada al análisis climático.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, when

CATALOG = "workspace"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_clima_raw_v2"


@dp.table(
    name="silver_clima",
    comment="Datos climáticos limpios y estructurados a partir de la capa Bronze"
)
def silver_clima():
    df = spark.readStream.table(BRONZE_TABLE)

    base = (
        df.select(
            col("ciudad"),
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
            col("country"),
            to_timestamp(col("ingestion_time")).alias("ingestion_time"),
            to_timestamp(col("payload.current.time")).alias("event_time"),
            col("payload.current.temperature_2m").cast("double").alias("temperature_2m"),
            col("payload.current.relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
            col("payload.current.apparent_temperature").cast("double").alias("apparent_temperature"),
            col("payload.current.precipitation").cast("double").alias("precipitation"),
            col("payload.current.rain").cast("double").alias("rain"),
            col("payload.current.cloud_cover").cast("double").alias("cloud_cover"),
            col("payload.current.pressure_msl").cast("double").alias("pressure_msl"),
            col("payload.current.wind_speed_10m").cast("double").alias("wind_speed_10m"),
            col("payload.current.wind_direction_10m").cast("double").alias("wind_direction_10m"),
            col("source_file"),
            col("bronze_processing_time")
        )
    )

    return (
        base
        .withColumn("alerta_lluvia", when(col("precipitation") >= 5, 1).otherwise(0))
        .withColumn("alerta_viento", when(col("wind_speed_10m") >= 35, 1).otherwise(0))
        .withColumn("alerta_temperatura", when(col("temperature_2m") >= 30, 1).otherwise(0))
    )
