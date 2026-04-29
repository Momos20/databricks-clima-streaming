# Databricks notebook source
"""
Notebook: fuente_open_meteo.py

Descripción:
Este notebook consume la API pública de Open-Meteo para obtener información
climática actual, horaria y diaria de diferentes ciudades de Colombia.

El resultado se guarda como archivos JSON incrementales en un volumen de
Databricks. Estos archivos son posteriormente procesados por el pipeline
Lakehouse en las capas Bronze, Silver y Gold.

Importante:
Este notebook NO hace parte del pipeline declarativo. Se ejecuta de forma
independiente, ya sea manualmente o como un Job programado.
"""

import requests
import json
import uuid
from datetime import datetime


# Ruta donde se almacenarán los archivos JSON crudos.
# Esta ruta debe coincidir con la ruta utilizada por la capa Bronze.
ruta_salida = "/Volumes/workspace/default/streaming_clima/raw_json_v2/"

# Creación de la carpeta en caso de que no exista.
dbutils.fs.mkdirs("dbfs:/Volumes/workspace/default/streaming_clima/raw_json_v2/")


# Ciudades de Colombia monitoreadas por el flujo.
ciudades = [
    {"ciudad": "Bogota", "lat": 4.60971, "lon": -74.08175},
    {"ciudad": "Medellin", "lat": 6.25184, "lon": -75.56359},
    {"ciudad": "Cali", "lat": 3.45165, "lon": -76.53198},
    {"ciudad": "Barranquilla", "lat": 10.96389, "lon": -74.79639},
    {"ciudad": "Cartagena", "lat": 10.39105, "lon": -75.47943},
    {"ciudad": "Bucaramanga", "lat": 7.12539, "lon": -73.11980},
    {"ciudad": "Pereira", "lat": 4.81428, "lon": -75.69456},
    {"ciudad": "Manizales", "lat": 5.07028, "lon": -75.51382},
    {"ciudad": "Cucuta", "lat": 7.89391, "lon": -72.50782},
    {"ciudad": "SantaMarta", "lat": 11.24079, "lon": -74.19904},
    {"ciudad": "Ibague", "lat": 4.43889, "lon": -75.23222},
    {"ciudad": "Villavicencio", "lat": 4.14200, "lon": -73.62664},
    {"ciudad": "Pasto", "lat": 1.21361, "lon": -77.28111},
    {"ciudad": "Monteria", "lat": 8.74798, "lon": -75.88143},
    {"ciudad": "Armenia", "lat": 4.53389, "lon": -75.68111},
    {"ciudad": "Sincelejo", "lat": 9.30472, "lon": -75.39778},
    {"ciudad": "Valledupar", "lat": 10.46314, "lon": -73.25322},
    {"ciudad": "Popayan", "lat": 2.44481, "lon": -76.61474},
    {"ciudad": "Neiva", "lat": 2.92730, "lon": -75.28189},
    {"ciudad": "Riohacha", "lat": 11.54444, "lon": -72.90722},
    {"ciudad": "Tunja", "lat": 5.53528, "lon": -73.36778},
    {"ciudad": "Florencia", "lat": 1.61438, "lon": -75.60623},
    {"ciudad": "Yopal", "lat": 5.33775, "lon": -72.39586},
    {"ciudad": "Quibdo", "lat": 5.69472, "lon": -76.66111},
    {"ciudad": "Arauca", "lat": 7.08471, "lon": -70.75908},
    {"ciudad": "Mocoa", "lat": 1.14722, "lon": -76.64778},
    {"ciudad": "SanAndres", "lat": 12.58472, "lon": -81.70056},
    {"ciudad": "Leticia", "lat": -4.21528, "lon": -69.94056},
    {"ciudad": "Inirida", "lat": 3.86528, "lon": -67.92389},
    {"ciudad": "PuertoCarrenio", "lat": 6.18500, "lon": -67.49389}
]

# COMMAND ----------

def construir_url(lat, lon):
    """
    Construye la URL de consulta a la API de Open-Meteo.

    Se solicitan variables actuales, horarias y diarias para simular
    un escenario de ingesta climática en tiempo casi real.
    """
    return (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&current="
        f"temperature_2m,"
        f"relative_humidity_2m,"
        f"apparent_temperature,"
        f"precipitation,"
        f"rain,"
        f"cloud_cover,"
        f"pressure_msl,"
        f"wind_speed_10m,"
        f"wind_direction_10m"
        f"&hourly="
        f"temperature_2m,"
        f"relative_humidity_2m,"
        f"precipitation_probability,"
        f"precipitation,"
        f"rain,"
        f"cloud_cover,"
        f"pressure_msl,"
        f"wind_speed_10m"
        f"&daily="
        f"temperature_2m_max,"
        f"temperature_2m_min,"
        f"precipitation_sum,"
        f"rain_sum,"
        f"wind_speed_10m_max"
        f"&forecast_days=3"
        f"&timezone=auto"
    )

# Se consume la API por cada ciudad y se genera un archivo JSON independiente.
for c in ciudades:
    try:
        url = construir_url(c["lat"], c["lon"])
        respuesta = requests.get(url, timeout=30)
        respuesta.raise_for_status()
        datos_api = respuesta.json()

        evento = {
            "ciudad": c["ciudad"],
            "latitude": c["lat"],
            "longitude": c["lon"],
            "country": "Colombia",
            "ingestion_time": datetime.utcnow().isoformat(),
            "payload": datos_api
        }

        nombre_archivo = (
            f"{ruta_salida}"
            f"{c['ciudad']}_"
            f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex}.json"
        )

        with open(nombre_archivo, "w", encoding="utf-8") as f:
            json.dump(evento, f, ensure_ascii=False)

        print(f"Archivo generado: {nombre_archivo}")

    except Exception as e:
        print(f"Error con {c['ciudad']}: {e}")

print("\nArchivos disponibles:")
display(dbutils.fs.ls("dbfs:/Volumes/workspace/default/streaming_clima/raw_json_v2/"))
