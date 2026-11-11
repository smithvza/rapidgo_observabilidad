# Ejecución local del pipeline

Todo el pipeline (Kafka, producer, Spark, Airflow) se levanta con Docker.

## Prerrequisitos

- Docker Desktop abierto y en estado *Running* (en Windows necesita WSL).
- Git y Python 3.12 o superior (para las pruebas y la prueba de humo).

## Pasos

1. Clonar y entrar al repositorio:
   ```
   git clone https://github.com/smithvza/rapidgo_observabilidad.git
   cd rapidgo_observabilidad
   ```
2. Levantar todo:
   ```
   docker compose up -d --build
   ```
   La primera vez tarda varios minutos (construye las imágenes y Spark descarga el
   conector de Kafka). Levanta cinco contenedores: `kafka`, `kafka-init` (crea el topic
   y termina), `producer`, `spark` (ejecuta el job de streaming) y `airflow`.
3. Esperar unos 4 minutos. La capa `curated` tarda en aparecer porque cada ventana
   de 1 minuto se cierra con el watermark de 2 minutos.
4. Verificar de punta a punta:
   ```
   python scripts/smoke_test.py
   ```
   Debe terminar con `RESULTADO: pipeline completo funcionando`.
5. Ver los resultados:
   - Airflow: http://localhost:8080, usuario `admin`. Contraseña:
     ```
     docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
     ```
     El DAG `rapidgo_observability_pipeline` corre solo cada 10 minutos; también se
     puede lanzar con *Trigger DAG*.
   - Reporte de KPIs y alertas: `datalake/reports/latest.md`.
   - Capas del Data Lake: `datalake/raw`, `datalake/processed`, `datalake/curated`.
6. Pruebas automáticas:
   ```
   pip install -r requirements.txt
   python -m pytest -v
   ```
7. Pausar y retomar sin perder datos:
   ```
   docker compose stop
   docker compose start
   ```

## Empezar de cero

```
docker compose down
```

y borrar dentro de `datalake/` las carpetas `raw`, `processed`, `curated`, `reports`
y `_checkpoints` (no borrar `datalake/README.md`). El historial de ejecuciones de
Airflow también se reinicia con `down`.

Importante: Kafka no guarda sus datos al hacer `down`, pero el checkpoint de Spark
sí recuerda las posiciones de lectura anteriores. Si se hace `down` y `up` sin borrar
`_checkpoints`, Spark muestra avisos `offset was changed from X to Y`, puede saltarse
los primeros eventos, y la prueba de humo podría verse con datos viejos.

## Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| `failed to connect to the docker API` | Abrir Docker Desktop y esperar a que diga *Running*. |
| El DAG falla con `Sin archivos Parquet en: ...` | Spark aún no escribió las capas. Esperar unos minutos o revisar `docker compose logs spark`. |
| El puerto 8080 está ocupado | Cerrar el programa que lo usa o cambiar `8080:8080` en `docker-compose.yml`. |
| Spark avisa `offset was changed from X to Y` | Quedó un checkpoint de una corrida anterior. No es un error, pero para una prueba limpia, ver "Empezar de cero". |
| Spark avisa `Current batch is falling behind` | Es un aviso. Si el atraso no deja de crecer, subir `TRIGGER_INTERVAL` en `spark/config.py`. |
| Un contenedor se reinicia solo | Ver sus logs: `docker compose logs <servicio>` (`spark`, `producer`, `airflow`). |
