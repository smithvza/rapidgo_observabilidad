"""Producer de eventos - RapidGo Observabilidad (Variante 9).

Genera eventos de peticiones de las APIs de RapidGo, los valida contra el
contrato (docs/contrato.md) y los publica en Kafka usando `service` como key.
Un evento invalido NO se publica: se descarta y se registra en el log
(regla 5 del contrato).

Ejecucion (desde la raiz del repo, con Kafka levantado por docker compose):
  python -m producer.rapidgo_producer
  python -m producer.rapidgo_producer --rate 30 --duration 300 --incident-every 90
  python -m producer.rapidgo_producer --invalid-rate 0.05 --seed 7
"""
import argparse
import json
import logging
import os
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from producer.contract import validate_event

TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo.observability.events")
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
REGION = "co-bogota"
INCIDENT_SECONDS = 90

# servicio -> (peso de trafico, [(metodo, endpoint), ...])
TRAFFIC = {
    "orders-api": (0.40, [("POST", "/v1/orders"), ("GET", "/v1/orders/status")]),
    "payments-api": (0.20, [("POST", "/v1/payments"), ("GET", "/v1/payments/receipt")]),
    "tracking-api": (0.15, [("GET", "/v1/tracking/position"), ("PUT", "/v1/tracking/update")]),
    "auth-api": (0.10, [("POST", "/v1/auth/login"), ("POST", "/v1/auth/refresh")]),
    "catalog-api": (0.10, [("GET", "/v1/catalog/items"), ("GET", "/v1/catalog/search")]),
    "notifications-api": (0.05, [("POST", "/v1/notifications/send")]),
}

log = logging.getLogger("rapidgo.producer")


class Stats:
    """Contadores que se imprimen al final (sirven como evidencia del checkpoint)."""

    def __init__(self) -> None:
        self.sent = 0
        self.invalid = 0
        self.delivery_errors = 0
        self.per_service: Counter = Counter()
        self.per_partition: Counter = Counter()


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event(service: str, degraded: bool = False, rng: random.Random = random) -> dict:
    """Crea un evento valido. Si el servicio esta degradado sube 5xx y latencia."""
    method, endpoint = rng.choice(TRAFFIC[service][1])
    if degraded:
        status = rng.choices([200, 500, 503], weights=[70, 20, 10])[0]
        latency = int(rng.lognormvariate(6.9, 0.4))  # mediana ~1000 ms
    else:
        status = rng.choices([200, 201, 400, 404, 500], weights=[90, 4, 3, 2, 1])[0]
        latency = int(rng.lognormvariate(4.8, 0.5))  # mediana ~120 ms
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": now_utc(),
        "service": service,
        "endpoint": endpoint,
        "http_method": method,
        "http_status": status,
        "latency_ms": max(0, min(latency, 60000)),
        "region": REGION,
    }


def corrupt_event(event: dict, rng: random.Random = random) -> dict:
    """Devuelve una copia del evento rota a proposito (para probar el descarte)."""
    broken = dict(event)
    defect = rng.choice(["status_str", "latency_negative", "unknown_service", "missing_field"])
    if defect == "status_str":
        broken["http_status"] = str(broken["http_status"])
    elif defect == "latency_negative":
        broken["latency_ms"] = -20
    elif defect == "unknown_service":
        broken["service"] = "servicio-inexistente"
    else:
        del broken["endpoint"]
    return broken


def make_delivery_callback(stats: Stats):
    def on_delivery(err, msg) -> None:
        if err is not None:
            stats.delivery_errors += 1
            log.error("entrega fallida: %s", err)
        else:
            stats.per_partition[msg.partition()] += 1

    return on_delivery


def publish_event(producer, event: dict, stats: Stats, topic: str = TOPIC) -> bool:
    """Valida el evento y lo publica con key = service. Devuelve True si se publico."""
    try:
        validate_event(event)
    except ValueError as error:
        stats.invalid += 1
        log.warning("evento invalido descartado: %s", error)
        return False

    service = event["service"]
    producer.produce(
        topic,
        key=service.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=make_delivery_callback(stats),
    )
    stats.sent += 1
    stats.per_service[service] += 1
    return True


def create_kafka_producer(bootstrap: str):
    # Import aqui para que los tests no necesiten confluent_kafka instalado.
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
        }
    )


def run(producer, args: argparse.Namespace, stats: Stats) -> None:
    rng = random.Random(args.seed)
    names = list(TRAFFIC)
    weights = [TRAFFIC[s][0] for s in names]

    start = time.time()
    next_incident = start + args.incident_every if args.incident_every else None
    degraded_service, degraded_until = None, 0.0

    log.info("Publicando en %s via %s a %s ev/s. Ctrl+C para parar.", args.topic, args.bootstrap, args.rate)
    try:
        while args.duration == 0 or time.time() - start < args.duration:
            now = time.time()
            if next_incident and now >= next_incident:
                degraded_service = rng.choice(names)
                degraded_until = now + INCIDENT_SECONDS
                next_incident = now + args.incident_every
                log.info("[incidente] %s degradado por %s s", degraded_service, INCIDENT_SECONDS)
            if degraded_service and now >= degraded_until:
                degraded_service = None

            service = rng.choices(names, weights=weights)[0]
            event = build_event(service, degraded=(service == degraded_service), rng=rng)
            if args.invalid_rate and rng.random() < args.invalid_rate:
                event = corrupt_event(event, rng)
            publish_event(producer, event, stats, args.topic)
            producer.poll(0)
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        log.info("Deteniendo...")
    finally:
        producer.flush(10)


def print_summary(stats: Stats) -> None:
    print(f"\nEnviados: {stats.sent} | invalidos descartados: {stats.invalid} "
          f"| errores de entrega: {stats.delivery_errors}")
    print("Por servicio:", dict(stats.per_service))
    print("Por particion:", dict(sorted(stats.per_partition.items())))


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Producer de eventos RapidGo (observabilidad)")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--rate", type=float, default=20, help="eventos por segundo")
    parser.add_argument("--duration", type=int, default=0, help="segundos (0 = hasta Ctrl+C)")
    parser.add_argument("--incident-every", type=int, default=120,
                        help="cada N s degrada un servicio por 90 s (0 = sin incidentes)")
    parser.add_argument("--invalid-rate", type=float, default=0.0,
                        help="fraccion de eventos rotos a proposito (0 a 1)")
    parser.add_argument("--seed", type=int, default=None, help="semilla para repetir la corrida")
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    stats = Stats()
    run(create_kafka_producer(args.bootstrap), args, stats)
    print_summary(stats)


if __name__ == "__main__":
    main()
