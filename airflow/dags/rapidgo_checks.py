"""Logica de negocio del pipeline de control de RapidGo (Variante 9).

Todo lo que decide algo (fuentes, calidad, KPIs, alertas, reporte) vive aqui
en funciones puras sobre listas de dicts, igual que spark/rules.py. Asi se
prueba con pytest sin Airflow ni Spark. El archivo del DAG solo las conecta.
"""
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATALAKE_PATH = os.getenv("DATALAKE_PATH", "/opt/airflow/datalake")
LAYERS = ("raw", "processed", "curated")

# --- Umbrales (docs/kpis-alertas.md) ---------------------------------------
ERROR_RATE_THRESHOLD = 0.05        # alerta: error rate > 5 %
P95_LATENCY_THRESHOLD_MS = 1000    # alerta: p95 > 1000 ms
MIN_EVENTS_PER_WINDOW = 20         # muestra minima para evitar falsos positivos
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "120"))  # 0 = todo el historial
MAX_DUPLICATE_RATIO = 0.01         # calidad: maximo 1 % de event_id repetidos
FRESHNESS_WARN_MINUTES = 10        # aviso si el ultimo evento es mas viejo
PARTITION_SKEW_WARN = 0.5          # aviso si una particion tiene mas del 50 %
MAX_ALERTS_LISTED = 50             # limite de alertas detalladas en el XCom

# --- Contrato del evento (docs/contrato.md) --------------------------------
SERVICES = (
    "orders-api",
    "payments-api",
    "tracking-api",
    "auth-api",
    "catalog-api",
    "notifications-api",
)
HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")
MIN_STATUS, MAX_STATUS = 100, 599
MIN_LATENCY_MS, MAX_LATENCY_MS = 0, 60000


class DataNotReadyError(RuntimeError):
    """El Data Lake todavia no tiene los datos que la tarea necesita."""


class QualityGateError(RuntimeError):
    """Los datos no pasan el control de calidad: no se debe publicar reporte."""


# --- Utilidades ------------------------------------------------------------
def _as_utc(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- Lectura del Data Lake -------------------------------------------------
def count_parquet_files(layer_dir) -> int:
    """Cuenta archivos .parquet de una capa, ignorando _spark_metadata y similares."""
    root = Path(layer_dir)
    if not root.is_dir():
        return 0
    total = 0
    for path in root.rglob("*.parquet"):
        parts = path.relative_to(root).parts
        if not any(part.startswith(("_", ".")) for part in parts):
            total += 1
    return total


def read_parquet_records(layer_dir, columns=None) -> list[dict]:
    """Lee una capa Parquet como lista de dicts (pyarrow se importa aqui)."""
    import pyarrow as pa
    import pyarrow.dataset as ds

    table = ds.dataset(str(layer_dir), format="parquet").to_table(columns=columns)
    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            target = pa.timestamp("us", tz=field.type.tz)
            table = table.set_column(i, field.name, table.column(i).cast(target, safe=False))
    return table.to_pylist()


# --- Tarea 1: fuentes ------------------------------------------------------
def check_sources(datalake_path=None) -> dict:
    base = Path(datalake_path or DATALAKE_PATH)
    counts = {layer: count_parquet_files(base / layer) for layer in LAYERS}
    empty = [layer for layer, total in counts.items() if total == 0]
    if empty:
        raise DataNotReadyError(
            f"Sin archivos Parquet en: {', '.join(empty)} (ruta {base}). "
            "Revisar que el job de Spark este corriendo; curated tarda en "
            "aparecer porque cada ventana se cierra con el watermark de 2 minutos."
        )
    return counts


# --- Tarea 2: calidad ------------------------------------------------------
def _is_valid_event(event: dict) -> bool:
    status, latency = event.get("http_status"), event.get("latency_ms")
    return (
        event.get("event_id") is not None
        and event.get("service") in SERVICES
        and str(event.get("endpoint", "")).startswith("/")
        and event.get("http_method") in HTTP_METHODS
        and isinstance(status, int)
        and MIN_STATUS <= status <= MAX_STATUS
        and isinstance(latency, int)
        and MIN_LATENCY_MS <= latency <= MAX_LATENCY_MS
    )


def check_quality(records: list[dict], now: datetime | None = None) -> dict:
    """Control de calidad sobre processed. Lanza QualityGateError si falla."""
    now = now or _now()
    total = len(records)
    if total == 0:
        raise QualityGateError("processed no tiene filas para validar")

    invalid = sum(not _is_valid_event(event) for event in records)
    ids = Counter(event.get("event_id") for event in records)
    duplicates = sum(count - 1 for count in ids.values() if count > 1)
    duplicate_ratio = duplicates / total

    times = [_as_utc(e["event_time"]) for e in records if e.get("event_time") is not None]
    minutes_since = round((now - max(times)).total_seconds() / 60, 1) if times else None

    warnings = []
    if minutes_since is None:
        warnings.append("ningun evento tiene event_time")
    elif minutes_since > FRESHNESS_WARN_MINUTES:
        warnings.append(
            f"el ultimo evento tiene {minutes_since} min (aviso a partir de {FRESHNESS_WARN_MINUTES})"
        )

    problems = []
    if invalid:
        problems.append(f"{invalid} eventos fuera del contrato")
    if duplicate_ratio > MAX_DUPLICATE_RATIO:
        problems.append(
            f"{duplicates} event_id repetidos ({duplicate_ratio:.2%} > {MAX_DUPLICATE_RATIO:.0%})"
        )
    if problems:
        raise QualityGateError("Control de calidad fallido: " + "; ".join(problems))

    return {
        "rows": total,
        "invalid_rows": invalid,
        "duplicate_events": duplicates,
        "duplicate_ratio": round(duplicate_ratio, 4),
        "minutes_since_last_event": minutes_since,
        "warnings": warnings,
    }


# --- Tarea 3: distribucion por particion (evidencia del ADR-001) ------------
def partition_distribution(records: list[dict]) -> dict:
    counts = Counter(record["kafka_partition"] for record in records)
    total = sum(counts.values())
    if total == 0:
        return {"total": 0, "counts": {}, "shares": {}, "hottest_partition": None,
                "skew_warning": False}
    shares = {str(p): round(c / total, 4) for p, c in sorted(counts.items())}
    hottest = max(counts, key=counts.get)
    return {
        "total": total,
        "counts": {str(p): c for p, c in sorted(counts.items())},
        "shares": shares,
        "hottest_partition": hottest,
        "skew_warning": counts[hottest] / total > PARTITION_SKEW_WARN,
    }


# --- Tarea 4: KPIs ---------------------------------------------------------
def aggregate_windows(
    windows: list[dict], now: datetime | None = None, lookback_minutes: int | None = None
) -> dict:
    """Consolida las ventanas de curated por servicio."""
    now = now or _now()
    lookback = LOOKBACK_MINUTES if lookback_minutes is None else lookback_minutes
    cutoff = now - timedelta(minutes=lookback) if lookback > 0 else None
    selected = [w for w in windows if cutoff is None or _as_utc(w["window_start"]) >= cutoff]
    if not selected:
        raise DataNotReadyError(
            f"curated no tiene ventanas en los ultimos {lookback} minutos "
            "(revisar que Spark y el producer sigan corriendo)"
        )

    per_service = defaultdict(list)
    for window in selected:
        per_service[window["service"]].append(window)

    services = {}
    for service, items in sorted(per_service.items()):
        total = sum(w["total_events"] for w in items)
        errors = sum(w["error_events"] for w in items)
        p95s = [w["p95_latency_ms"] for w in items]
        error_rate = errors / total if total else 0.0
        services[service] = {
            "windows": len(items),
            "total_events": total,
            "error_events": errors,
            "error_rate": round(error_rate, 4),
            "availability": round(1 - error_rate, 4),
            "p95_max_ms": max(p95s),
            "p95_mean_ms": round(sum(p95s) / len(p95s)),
        }

    clean_windows = [
        {
            "window_start": _as_utc(w["window_start"]).isoformat(),
            "service": w["service"],
            "total_events": w["total_events"],
            "error_rate": round(w["error_events"] / w["total_events"], 4)
            if w["total_events"] else 0.0,
            "p95_latency_ms": w["p95_latency_ms"],
        }
        for w in sorted(selected, key=lambda w: _as_utc(w["window_start"]))
    ]
    return {
        "lookback_minutes": lookback,
        "window_count": len(selected),
        "services": services,
        "windows": clean_windows,
    }


# --- Tarea 5: alertas ------------------------------------------------------
def evaluate_alerts(windows: list[dict]) -> dict:
    """Aplica las 2 reglas de docs/kpis-alertas.md a cada ventana."""
    alerts = []
    for window in windows:
        if window["total_events"] < MIN_EVENTS_PER_WINDOW:
            continue
        base = {"service": window["service"], "window_start": window["window_start"]}
        if window["error_rate"] > ERROR_RATE_THRESHOLD:
            alerts.append({**base, "type": "tasa_error_alta", "severity": "alta",
                           "value": window["error_rate"], "threshold": ERROR_RATE_THRESHOLD})
        if window["p95_latency_ms"] > P95_LATENCY_THRESHOLD_MS:
            alerts.append({**base, "type": "latencia_critica", "severity": "critica",
                           "value": window["p95_latency_ms"],
                           "threshold": P95_LATENCY_THRESHOLD_MS})
    return {
        "alert_count": len(alerts),
        "by_type": dict(Counter(a["type"] for a in alerts)),
        "by_service": dict(Counter(a["service"] for a in alerts)),
        "alerts": alerts[:MAX_ALERTS_LISTED],
        "truncated": len(alerts) > MAX_ALERTS_LISTED,
    }


# --- Tarea 6: reporte ------------------------------------------------------
def build_report(sources, quality, partitions, kpis, alerts, generated_at=None) -> dict:
    generated_at = generated_at or _now()
    return {
        "generated_at": generated_at.isoformat(),
        "sources": sources,
        "quality": quality,
        "partitions": partitions,
        "kpis": {k: v for k, v in kpis.items() if k != "windows"},
        "alerts": alerts,
    }


def render_markdown(report: dict) -> str:
    q, p, k, a = report["quality"], report["partitions"], report["kpis"], report["alerts"]
    lines = [
        "# Reporte de observabilidad - RapidGo",
        "",
        f"Generado: {report['generated_at']}",
        f"Archivos Parquet por capa: {report['sources']}",
        "",
        "## Calidad (processed)",
        f"- Filas: {q['rows']} | fuera de contrato: {q['invalid_rows']} | "
        f"event_id repetidos: {q['duplicate_events']}",
        f"- Minutos desde el ultimo evento: {q['minutes_since_last_event']}",
    ]
    lines += [f"- AVISO: {w}" for w in q["warnings"]]
    lines += [
        "",
        f"## KPIs por servicio (ultimos {k['lookback_minutes']} min, {k['window_count']} ventanas)",
        "",
        "| Servicio | Eventos | Error rate | Disponibilidad | p95 max (ms) | Ventanas |",
        "|---|---|---|---|---|---|",
    ]
    for service, s in k["services"].items():
        lines.append(
            f"| {service} | {s['total_events']} | {s['error_rate']:.2%} | "
            f"{s['availability']:.2%} | {s['p95_max_ms']} | {s['windows']} |"
        )
    lines += ["", f"## Alertas: {a['alert_count']}"]
    lines += [f"- {kind}: {n}" for kind, n in a["by_type"].items()]
    for alert in a["alerts"][:10]:
        lines.append(
            f"  - {alert['window_start']} {alert['service']} {alert['type']} "
            f"(valor {alert['value']}, umbral {alert['threshold']})"
        )
    lines += ["", "## Distribucion por particion de Kafka (raw)", "",
              "| Particion | Eventos | Porcentaje |", "|---|---|---|"]
    for part, count in p["counts"].items():
        lines.append(f"| {part} | {count} | {p['shares'][part]:.1%} |")
    if p["skew_warning"]:
        lines.append(
            f"\nAVISO: la particion {p['hottest_partition']} concentra mas del "
            f"{PARTITION_SKEW_WARN:.0%} de los eventos."
        )
    return "\n".join(lines) + "\n"


def write_report(report: dict, output_dir=None, now: datetime | None = None) -> dict:
    """Guarda el reporte en JSON y Markdown dentro de datalake/reports."""
    now = now or _now()
    out = Path(output_dir or Path(DATALAKE_PATH) / "reports")
    out.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    json_path, md_path = out / f"kpi_report_{stamp}.json", out / f"kpi_report_{stamp}.md"
    markdown = render_markdown(report)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    (out / "latest.md").write_text(markdown, encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}
