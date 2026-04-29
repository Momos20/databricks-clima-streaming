# Databricks pipeline source
"""
Archivo: pipeline_reorder.py

Transformación Gold:
Genera eventos de reorder cuando la precipitación acumulada supera un umbral.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit

CATALOG = "workspace"
SCHEMA = "default"
AGG_TABLE = f"{CATALOG}.{SCHEMA}.agg_clima_ventana"


@dp.table(
    name="reorder_eventos_clima",
    comment="Eventos de reorder generados por condiciones climáticas"
)
def reorder_eventos_clima():
    df = spark.readStream.table(AGG_TABLE)

    return (
        df.filter(col("precipitacion_acumulada") > 15)
          .select(
              col("ciudad"),
              col("window.start").alias("ventana_inicio"),
              col("window.end").alias("ventana_fin"),
              col("precipitacion_acumulada"),
              lit("reordenar_insumos_lluvia").alias("evento_reorder")
          )
    )
