# rapidgo_observabilidad# Data Lake - RapidGo Observabilidad

Estructura por capas del Data Lake del pipeline:

| Capa | Carpeta | Contenido |
|---|---|---|
| Raw | `datalake/raw/` | Eventos tal como llegan de Kafka, sin transformar (Parquet) |
| Processed | `datalake/processed/` | Eventos validados y con transformaciones |
| Curated | `datalake/curated/` | KPIs por servicio y ventana de 1 min (error rate, p95, disponibilidad) y alertas |

Notas:
- Los datos generados no se suben a Git (están en `.gitignore`).
- Spark escribe en esta carpeta a través del volumen `./datalake:/opt/app/datalake`
  definido en `docker-compose.yml`.
- Los checkpoints de Spark Structured Streaming también se guardan fuera de Git.
