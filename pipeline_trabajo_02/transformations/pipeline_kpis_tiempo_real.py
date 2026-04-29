# Databricks pipeline source
"""
Archivo: pipeline_kpis_tiempo_real.py

Transformación Gold:
Genera indicadores globales para el dashboard en tiempo casi real.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum, max, lit, approx_count_distinct

CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_clima"


@dp.table(
    name="kpis_clima_tiempo_real",
    comment="KPIs globales de monitoreo climático para el dashboard"
)
def kpis_clima_tiempo_real():
    df = spark.readStream.table(SILVER_TABLE)

    return (
        df.agg(
            count("*").alias("total_lecturas"),
            approx_count_distinct("ciudad").alias("ciudades_monitoreadas"),
            sum("alerta_lluvia").alias("total_alertas_lluvia"),
            sum("alerta_viento").alias("total_alertas_viento"),
            sum("alerta_temperatura").alias("total_alertas_temperatura"),
            max("event_time").alias("ultimo_evento")
        )
        .withColumn("nivel", lit("global"))
    )
