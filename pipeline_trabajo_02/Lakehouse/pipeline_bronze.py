# Databricks pipeline source
"""
Archivo: pipeline_bronze.py

Capa Bronze:
Lee los archivos JSON crudos generados por el notebook fuente_open_meteo.py.
La lectura se realiza mediante Auto Loader usando cloudFiles.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# Ruta de entrada de los archivos JSON crudos.
RAW_PATH = "/Volumes/workspace/default/streaming_clima/raw_json_v2/"

# Esquema principal del JSON.
# Se modela principalmente la sección current, que es la que se usa en Silver.
schema_bronze = StructType([
    StructField("ciudad", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("payload", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("utc_offset_seconds", IntegerType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_abbreviation", StringType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("current", StructType([
            StructField("time", StringType(), True),
            StructField("interval", IntegerType(), True),
            StructField("temperature_2m", DoubleType(), True),
            StructField("relative_humidity_2m", DoubleType(), True),
            StructField("apparent_temperature", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("rain", DoubleType(), True),
            StructField("cloud_cover", DoubleType(), True),
            StructField("pressure_msl", DoubleType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
            StructField("wind_direction_10m", DoubleType(), True)
        ]), True)
    ]), True)
])


@dp.table(
    name="bronze_clima_raw_v2",
    comment="Datos crudos de clima consumidos desde archivos JSON generados por la fuente Open-Meteo"
)
def bronze_clima_raw_v2():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .schema(schema_bronze)
            .load(RAW_PATH)
            .withColumn("source_file", input_file_name())
            .withColumn("bronze_processing_time", current_timestamp())
    )
