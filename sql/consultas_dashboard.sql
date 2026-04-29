-- ============================================================
-- Consultas sugeridas para construir el dashboard en Databricks
-- AI/BI Dashboards
-- ============================================================

-- Ajustar el catálogo y esquema si en el workspace se utilizan otros nombres.
-- En este proyecto se usó: workspace.default

-- 1. KPI: total de lecturas
SELECT
  SUM(total_eventos) AS total_lecturas
FROM workspace.default.gold_clima_metricas;

-- 2. KPI: ciudades monitoreadas
SELECT
  COUNT(DISTINCT ciudad) AS ciudades_monitoreadas
FROM workspace.default.gold_clima_metricas;

-- 3. KPI: total de alertas climáticas
SELECT
  SUM(total_alertas_lluvia) AS total_alertas_lluvia,
  SUM(total_alertas_viento) AS total_alertas_viento,
  SUM(total_alertas_temperatura) AS total_alertas_temperatura,
  SUM(total_alertas_lluvia + total_alertas_viento + total_alertas_temperatura) AS total_alertas
FROM workspace.default.gold_clima_metricas;

-- 4. Temperatura promedio por ciudad
SELECT
  ciudad,
  temperatura_promedio
FROM workspace.default.gold_clima_metricas
ORDER BY temperatura_promedio DESC;

-- 5. Precipitación acumulada por ciudad
SELECT
  ciudad,
  precipitacion_acumulada
FROM workspace.default.gold_clima_metricas
ORDER BY precipitacion_acumulada DESC;

-- 6. Velocidad promedio del viento por ciudad
SELECT
  ciudad,
  viento_promedio
FROM workspace.default.gold_clima_metricas
ORDER BY viento_promedio DESC;

-- 7. Nubosidad promedio por ciudad
SELECT
  ciudad,
  nubosidad_promedio
FROM workspace.default.gold_clima_metricas
ORDER BY nubosidad_promedio DESC;

-- 8. Evolución temporal por ventana
SELECT
  ciudad,
  window.start AS ventana_inicio,
  window.end AS ventana_fin,
  temperatura_promedio,
  humedad_promedio,
  precipitacion_acumulada,
  viento_promedio,
  total_eventos
FROM workspace.default.agg_clima_ventana
ORDER BY ventana_inicio DESC;

-- 9. Scatter plot: temperatura vs humedad
SELECT
  ciudad,
  event_time,
  temperature_2m,
  relative_humidity_2m,
  precipitation,
  wind_speed_10m
FROM workspace.default.silver_clima;

-- 10. Scatter plot: viento vs precipitación
SELECT
  ciudad,
  event_time,
  wind_speed_10m,
  precipitation,
  temperature_2m
FROM workspace.default.silver_clima;

-- 11. Tabla de anomalías climáticas
SELECT
  ciudad,
  event_time,
  tipo_anomalia,
  temperature_2m,
  relative_humidity_2m,
  precipitation,
  rain,
  wind_speed_10m,
  cloud_cover,
  pressure_msl
FROM workspace.default.anomalias_clima
ORDER BY event_time DESC;

-- 12. Eventos de reorder
SELECT
  ciudad,
  ventana_inicio,
  ventana_fin,
  precipitacion_acumulada,
  evento_reorder
FROM workspace.default.reorder_eventos_clima
ORDER BY ventana_inicio DESC;

-- 13. Ranking de ciudades con más eventos
SELECT
  ciudad,
  total_eventos,
  total_alertas_lluvia,
  total_alertas_viento,
  total_alertas_temperatura
FROM workspace.default.gold_clima_metricas
ORDER BY total_eventos DESC;

-- 14. Resumen global desde la tabla de KPIs
SELECT
  nivel,
  total_lecturas,
  ciudades_monitoreadas,
  total_alertas_lluvia,
  total_alertas_viento,
  total_alertas_temperatura,
  ultimo_evento
FROM workspace.default.kpis_clima_tiempo_real;
