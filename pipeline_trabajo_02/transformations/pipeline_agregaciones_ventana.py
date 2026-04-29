# Databricks pipeline source
"""
Archivo: pipeline_agregaciones_ventana.py

Transformación Gold:
Calcula métricas climáticas por ciudad en ventanas de 10 minutos.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, window, avg, sum, count, round

CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_clima"


@dp.table(
    name="agg_clima_ventana",
    comment="Agregaciones por ventana de 10 minutos sobre datos climáticos"
)
def agg_clima_ventana():
    df = spark.readStream.table(SILVER_TABLE)

    return (
        df.withWatermark("event_time", "20 minutes")
          .groupBy(
              col("ciudad"),
              window(col("event_time"), "10 minutes").alias("window")
          )
          .agg(
              round(avg("temperature_2m"), 2).alias("temperatura_promedio"),
              round(avg("relative_humidity_2m"), 2).alias("humedad_promedio"),
              round(sum("precipitation"), 2).alias("precipitacion_acumulada"),
              round(sum("rain"), 2).alias("lluvia_acumulada"),
              round(avg("wind_speed_10m"), 2).alias("viento_promedio"),
              sum("alerta_lluvia").alias("total_alertas_lluvia"),
              sum("alerta_viento").alias("total_alertas_viento"),
              sum("alerta_temperatura").alias("total_alertas_temperatura"),
              count("*").alias("total_eventos")
          )
    )
