"""Configuracion central del job de Spark (RapidGo - Variante 9).

Los umbrales viven aqui para que los tests de reglas y el job lean
los mismos valores. Cualquier cambio se hace por PR (ver docs/kpis-alertas.md).
"""
import os

# --- Kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo.observability.events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")  # "latest" en la demo

# --- Data Lake (ruta dentro del contenedor spark) -------------------------
DATALAKE_PATH = os.getenv("DATALAKE_PATH", "/opt/app/datalake")
RAW_PATH = f"{DATALAKE_PATH}/raw"
PROCESSED_PATH = f"{DATALAKE_PATH}/processed"
CURATED_PATH = f"{DATALAKE_PATH}/curated"
CHECKPOINT_PATH = f"{DATALAKE_PATH}/_checkpoints"

# --- Contrato del evento (docs/contrato.md) -------------------------------
SERVICES = [
    "orders-api",
    "payments-api",
    "tracking-api",
    "auth-api",
    "catalog-api",
    "notifications-api",
]
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
MIN_STATUS, MAX_STATUS = 100, 599
MIN_LATENCY_MS, MAX_LATENCY_MS = 0, 60000

# --- Ventanas -------------------------------------------------------------
WINDOW_DURATION = "1 minute"
WATERMARK = "2 minutes"
TRIGGER_INTERVAL = "30 seconds"

# --- KPIs y alertas (docs/kpis-alertas.md) --------------------------------
ERROR_STATUS_FROM = 500           # http_status >= 500 cuenta como fallo
ERROR_RATE_THRESHOLD = 0.05       # alerta 1: error rate > 5 %
P95_LATENCY_THRESHOLD_MS = 800    # alerta 2: p95 > 800 ms
MIN_EVENTS_PER_WINDOW = 20        # muestra minima para evitar falsos positivos
