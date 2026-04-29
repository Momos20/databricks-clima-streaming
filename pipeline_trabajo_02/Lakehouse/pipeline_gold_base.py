# Databricks pipeline source
"""
Archivo: pipeline_gold_base.py

Capa Gold base:
Calcula métricas agregadas por ciudad a partir de la tabla Silver.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import avg, sum, count, max, round

CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_clima"


@dp.table(
    name="gold_clima_metricas",
    comment="Métricas climáticas agregadas por ciudad para visualización en dashboard"
)
def gold_clima_metricas():
    df = spark.readStream.table(SILVER_TABLE)

    return (
        df.groupBy("ciudad")
          .agg(
              round(avg("temperature_2m"), 2).alias("temperatura_promedio"),
              round(avg("relative_humidity_2m"), 2).alias("humedad_promedio"),
              round(avg("apparent_temperature"), 2).alias("sensacion_termica_promedio"),
              round(sum("precipitation"), 2).alias("precipitacion_acumulada"),
              round(sum("rain"), 2).alias("lluvia_acumulada"),
              round(avg("cloud_cover"), 2).alias("nubosidad_promedio"),
              round(avg("pressure_msl"), 2).alias("presion_promedio"),
              round(avg("wind_speed_10m"), 2).alias("viento_promedio"),
              sum("alerta_lluvia").alias("total_alertas_lluvia"),
              sum("alerta_viento").alias("total_alertas_viento"),
              sum("alerta_temperatura").alias("total_alertas_temperatura"),
              count("*").alias("total_eventos"),
              max("event_time").alias("ultimo_evento")
          )
    )
