# Arquitectura del flujo climático en Databricks

Este documento describe la arquitectura general del proyecto implementado en Databricks para procesar información climática en tiempo casi real.

## 1. Visión general

El flujo se divide en cuatro bloques principales:

```text
API Open-Meteo
     |
     v
Notebook fuente externa
     |
     v
Archivos JSON en Databricks Volumes
     |
     v
Pipeline Lakehouse
     |
     ├── Bronze
     ├── Silver
     └── Gold
     |
     v
AI/BI Dashboard
```

## 2. Fuente de datos

La fuente corresponde a la API pública de Open-Meteo. El notebook `fuente_open_meteo.py` consulta variables climáticas para 30 ciudades de Colombia y guarda un archivo JSON independiente por ciudad.

Variables principales consultadas:

- Temperatura actual.
- Humedad relativa.
- Sensación térmica.
- Precipitación.
- Lluvia.
- Nubosidad.
- Presión atmosférica.
- Velocidad del viento.
- Dirección del viento.

La ruta de salida usada en Databricks es:

```text
/Volumes/workspace/default/streaming_clima/raw_json_v2/
```

## 3. Capa Bronze

La capa Bronze conserva la información cruda proveniente de los archivos JSON.

Tabla principal:

```text
bronze_clima_raw_v2
```

Responsabilidades:

- Leer archivos JSON incrementales.
- Registrar archivo fuente.
- Registrar timestamp de procesamiento.
- Mantener trazabilidad del evento original.

## 4. Capa Silver

La capa Silver limpia y estructura la información relevante para análisis.

Tabla principal:

```text
silver_clima
```

Campos principales:

```text
ciudad
latitude
longitude
country
ingestion_time
event_time
temperature_2m
relative_humidity_2m
apparent_temperature
precipitation
rain
cloud_cover
pressure_msl
wind_speed_10m
wind_direction_10m
alerta_lluvia
alerta_viento
alerta_temperatura
```

## 5. Capa Gold

La capa Gold contiene tablas orientadas a análisis, dashboard y toma de decisiones.

Tablas principales:

```text
gold_clima_metricas
kpis_clima_tiempo_real
anomalias_clima
agg_clima_ventana
reorder_eventos_clima
```

## 6. Transformaciones Gold

### Métricas por ciudad

La tabla `gold_clima_metricas` calcula métricas agregadas por ciudad:

- Temperatura promedio.
- Humedad promedio.
- Precipitación acumulada.
- Lluvia acumulada.
- Nubosidad promedio.
- Presión promedio.
- Viento promedio.
- Total de eventos.
- Total de alertas.

### KPIs globales

La tabla `kpis_clima_tiempo_real` resume el estado general del flujo:

- Total de lecturas.
- Ciudades monitoreadas.
- Total de alertas por lluvia.
- Total de alertas por viento.
- Total de alertas por temperatura.
- Último evento registrado.

### Anomalías

La tabla `anomalias_clima` identifica eventos mediante reglas simples:

```text
precipitation >= 10      -> lluvia_extrema
wind_speed_10m >= 35     -> viento_extremo
temperature_2m >= 30     -> temperatura_alta
```

### Ventanas de tiempo

La tabla `agg_clima_ventana` agrupa los eventos en ventanas de 10 minutos por ciudad.

### Eventos de reorder

La tabla `reorder_eventos_clima` genera eventos cuando la precipitación acumulada por ventana supera el umbral definido:

```text
precipitacion_acumulada > 15
```

## 7. Justificación de la arquitectura

Esta arquitectura permite separar claramente las responsabilidades del flujo:

- Bronze conserva el dato crudo.
- Silver limpia y normaliza.
- Gold calcula métricas para consumo analítico.
- El dashboard consume únicamente tablas listas para análisis.

Este diseño facilita la trazabilidad, el mantenimiento, la escalabilidad y la explicación del proceso ante un evaluador.
