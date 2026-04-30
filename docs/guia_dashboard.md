# Guía para construir el dashboard en Databricks AI/BI Dashboards

Este documento describe cómo construir el dashboard del proyecto a partir de las tablas generadas por el pipeline.

## 1. Tablas utilizadas

Las visualizaciones se construyen principalmente con estas tablas:

```text
workspace.default.gold_clima_metricas
workspace.default.kpis_clima_tiempo_real
workspace.default.anomalias_clima
workspace.default.agg_clima_ventana
workspace.default.reorder_eventos_clima
workspace.default.silver_clima
```

## 2. Distribución sugerida del dashboard

Se recomienda dividir el dashboard en cuatro secciones:

```text
1. KPIs generales
2. Comportamiento por ciudad
3. Análisis temporal
4. Alertas y eventos de reorder
```

## 3. KPIs generales

### Total de lecturas

Consulta:

```sql
SELECT
  SUM(total_eventos) AS total_lecturas
FROM workspace.default.gold_clima_metricas;
```

Visualización sugerida:

```text
Counter / KPI
```

### Ciudades monitoreadas

Consulta:

```sql
SELECT
  COUNT(DISTINCT ciudad) AS ciudades_monitoreadas
FROM workspace.default.gold_clima_metricas;
```

Visualización sugerida:

```text
Counter / KPI
```

### Total de alertas

Consulta:

```sql
SELECT
  SUM(total_alertas_lluvia) AS total_alertas_lluvia,
  SUM(total_alertas_viento) AS total_alertas_viento,
  SUM(total_alertas_temperatura) AS total_alertas_temperatura,
  SUM(total_alertas_lluvia + total_alertas_viento + total_alertas_temperatura) AS total_alertas
FROM workspace.default.gold_clima_metricas;
```

Visualización sugerida:

```text
Counter / KPI
```

## 4. Comportamiento por ciudad

### Temperatura promedio por ciudad

Consulta:

```sql
SELECT
  ciudad,
  temperatura_promedio
FROM workspace.default.gold_clima_metricas
ORDER BY temperatura_promedio DESC;
```

Visualización sugerida:

```text
Bar chart
```

Configuración:

```text
Eje X: ciudad
Eje Y: temperatura_promedio
```

### Precipitación acumulada por ciudad

Consulta:

```sql
SELECT
  ciudad,
  precipitacion_acumulada
FROM workspace.default.gold_clima_metricas
ORDER BY precipitacion_acumulada DESC;
```

Visualización sugerida:

```text
Bar chart
```

Configuración:

```text
Eje X: ciudad
Eje Y: precipitacion_acumulada
```

### Viento promedio por ciudad

Consulta:

```sql
SELECT
  ciudad,
  viento_promedio
FROM workspace.default.gold_clima_metricas
ORDER BY viento_promedio DESC;
```

Visualización sugerida:

```text
Bar chart
```

Configuración:

```text
Eje X: ciudad
Eje Y: viento_promedio
```

## 5. Análisis temporal

### Evolución por ventanas de tiempo

Consulta:

```sql
SELECT
  ciudad,
  window.start AS ventana_inicio,
  window.end AS ventana_fin,
  temperatura_promedio,
  precipitacion_acumulada,
  viento_promedio
FROM workspace.default.agg_clima_ventana
ORDER BY ventana_inicio DESC;
```

Visualización sugerida:

```text
Line chart
```

Configuración:

```text
Eje X: ventana_inicio
Eje Y: temperatura_promedio
Serie / color: ciudad
```

También se puede construir una segunda línea usando `precipitacion_acumulada` o `viento_promedio` como métrica principal.

## 6. Scatter plots

Los scatter plots ayudan a que el dashboard no dependa únicamente de barras y tablas.

### Temperatura vs humedad

Consulta:

```sql
SELECT
  ciudad,
  event_time,
  temperature_2m,
  relative_humidity_2m,
  precipitation,
  wind_speed_10m
FROM workspace.default.silver_clima;
```

Visualización sugerida:

```text
Scatter plot
```

Configuración:

```text
Eje X: temperature_2m
Eje Y: relative_humidity_2m
Color: ciudad
Tamaño opcional: precipitation
```

Interpretación sugerida:

```text
Esta gráfica permite observar la relación entre temperatura y humedad relativa por ciudad. Los puntos más alejados pueden representar condiciones climáticas particulares o eventos atípicos.
```

### Viento vs precipitación

Consulta:

```sql
SELECT
  ciudad,
  event_time,
  wind_speed_10m,
  precipitation,
  temperature_2m
FROM workspace.default.silver_clima;
```

Visualización sugerida:

```text
Scatter plot
```

Configuración:

```text
Eje X: wind_speed_10m
Eje Y: precipitation
Color: ciudad
Tamaño opcional: temperature_2m
```

Interpretación sugerida:

```text
Esta gráfica permite identificar combinaciones de lluvia y viento que podrían ser relevantes para activar alertas operativas.
```

## 7. Alertas y eventos de reorder

### Tabla de anomalías

Consulta:

```sql
SELECT
  ciudad,
  event_time,
  tipo_anomalia,
  temperature_2m,
  precipitation,
  wind_speed_10m
FROM workspace.default.anomalias_clima
ORDER BY event_time DESC;
```

Visualización sugerida:

```text
Table
```

### Tabla de eventos de reorder

Consulta:

```sql
SELECT
  ciudad,
  ventana_inicio,
  ventana_fin,
  precipitacion_acumulada,
  evento_reorder
FROM workspace.default.reorder_eventos_clima
ORDER BY ventana_inicio DESC;
```

Visualización sugerida:

```text
Table
```

## 8. Filtros recomendados

Se recomienda agregar filtros globales por:

```text
ciudad
event_time
ventana_inicio
tipo_anomalia
```
