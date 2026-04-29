# Databricks pipeline source
"""
Archivo: pipeline_anomalias.py

Transformación Gold:
Identifica anomalías climáticas mediante reglas simples de negocio.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when

CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_clima"


@dp.table(
    name="anomalias_clima",
    comment="Eventos climáticos anómalos detectados a partir de reglas sobre la tabla Silver"
)
def anomalias_clima():
    df = spark.readStream.table(SILVER_TABLE)

    return (
        df.withColumn(
            "tipo_anomalia",
            when(col("precipitation") >= 10, "lluvia_extrema")
            .when(col("wind_speed_10m") >= 35, "viento_extremo")
            .when(col("temperature_2m") >= 30, "temperatura_alta")
        )
        .filter(col("tipo_anomalia").isNotNull())
        .select(
            "ciudad",
            "event_time",
            "tipo_anomalia",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",
            "wind_speed_10m",
            "cloud_cover",
            "pressure_msl"
        )
    )
