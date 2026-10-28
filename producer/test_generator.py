"""
  python producer/test_generator.py
  python producer/test_generator.py --rate 30 --duration 300 --incident-every 90
"""
import argparse
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "rapidgo.observability.events"


SERVICES = {
    "orders-api": (0.40, [("POST", "/v1/orders"), ("GET", "/v1/orders/status")]),
    "payments-api": (0.20, [("POST", "/v1/payments"), ("GET", "/v1/payments/receipt")]),
    "tracking-api": (0.15, [("GET", "/v1/tracking/position"), ("PUT", "/v1/tracking/update")]),
    "auth-api": (0.10, [("POST", "/v1/auth/login"), ("POST", "/v1/auth/refresh")]),
    "catalog-api": (0.10, [("GET", "/v1/catalog/items"), ("GET", "/v1/catalog/search")]),
    "notifications-api": (0.05, [("POST", "/v1/notifications/send")]),
}
REGION = "co-bogota"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(service: str, degraded: bool) -> dict:
    method, endpoint = random.choice(SERVICES[service][1])
    if degraded:
        # Servicio degradado: muchos 5xx y latencia alta (cruza los umbrales).
        status = random.choices([200, 500, 503], weights=[70, 20, 10])[0]
        latency = int(random.lognormvariate(6.9, 0.4))  # mediana ~1000 ms
    else:
        status = random.choices([200, 201, 400, 404, 500], weights=[90, 4, 3, 2, 1])[0]
        latency = int(random.lognormvariate(4.8, 0.5))  # mediana ~120 ms
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Generador minimo de eventos RapidGo")
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--rate", type=float, default=20, help="eventos por segundo")
    parser.add_argument("--duration", type=int, default=0, help="segundos (0 = hasta Ctrl+C)")
    parser.add_argument("--incident-every", type=int, default=120,
                        help="cada N s degrada un servicio por 90 s (0 = sin incidentes)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
    })

    names = list(SERVICES)
    weights = [SERVICES[s][0] for s in names]
    per_partition, per_service, errors = Counter(), Counter(), Counter()

    def on_delivery(err, msg):
        if err is not None:
            errors["delivery"] += 1
        else:
            per_partition[msg.partition()] += 1

    start = time.time()
    next_incident = start + args.incident_every if args.incident_every else None
    degraded_service, degraded_until = None, 0.0

    print(f"Publicando en {TOPIC} via {args.bootstrap} a {args.rate} ev/s. Ctrl+C para parar.")
    try:
        while args.duration == 0 or time.time() - start < args.duration:
            now = time.time()
            if next_incident and now >= next_incident:
                degraded_service = random.choice(names)
                degraded_until = now + 90
                next_incident = now + args.incident_every
                print(f"[incidente] {degraded_service} degradado por 90 s")
            if degraded_service and now >= degraded_until:
                degraded_service = None

            service = random.choices(names, weights=weights)[0]
            event = make_event(service, degraded=(service == degraded_service))
            producer.produce(
                TOPIC,
                key=service.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=on_delivery,
            )
            per_service[service] += 1
            producer.poll(0)
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nDeteniendo...")
    finally:
        producer.flush(10)
        total = sum(per_service.values())
        print(f"\nEnviados: {total} | errores de entrega: {errors['delivery']}")
        print("Por servicio:", dict(per_service))
        print("Por particion:", dict(sorted(per_partition.items())))


if __name__ == "__main__":
    main()
