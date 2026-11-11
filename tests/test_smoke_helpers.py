import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import smoke_test  # noqa: E402


def test_missing_services_lists_only_absent_ones():
    running = "kafka\nairflow\n\nspark\n"
    assert smoke_test.missing_services(running) == ["producer"]


def test_missing_services_empty_when_all_run():
    assert smoke_test.missing_services("kafka\nspark\nairflow\nproducer\n") == []


def test_parse_partition_count_from_kafka_describe():
    text = ("Topic: rapidgo.observability.events\tTopicId: x\tPartitionCount: 3\t"
            "ReplicationFactor: 1\tConfigs: retention.ms=86400000")
    assert smoke_test.parse_partition_count(text) == 3


def test_parse_partition_count_returns_none_when_missing():
    assert smoke_test.parse_partition_count("Topic not found") is None


def test_parse_offsets_ignores_noise_lines():
    text = "rapidgo.observability.events:0:340\nrapidgo.observability.events:1:708\n" \
           "WARN algo raro\nrapidgo.observability.events:2:127\n"
    assert smoke_test.parse_offsets(text) == {0: 340, 1: 708, 2: 127}


def make_parquet(path: Path, minutes_ago: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    old = time.time() - minutes_ago * 60
    os.utime(path, (old, old))


def test_newest_parquet_age_uses_most_recent_file_and_ignores_metadata(tmp_path):
    make_parquet(tmp_path / "a.parquet", minutes_ago=30)
    make_parquet(tmp_path / "b.parquet", minutes_ago=2)
    make_parquet(tmp_path / "_spark_metadata" / "c.parquet", minutes_ago=0)
    age = smoke_test.newest_parquet_age_minutes(tmp_path)
    assert 1.9 < age < 2.2


def test_newest_parquet_age_is_none_without_files(tmp_path):
    assert smoke_test.newest_parquet_age_minutes(tmp_path) is None


def test_layers_ready_needs_files_in_every_layer_and_fresh_data():
    fresh = {"raw": {"files": 3, "age": 0.5}, "processed": {"files": 3, "age": 0.5},
             "curated": {"files": 1, "age": 3.0}}
    assert smoke_test.layers_ready(fresh, max_age_minutes=5) is True

    stale = {**fresh, "curated": {"files": 1, "age": 600.0}}   # datos de una corrida vieja
    assert smoke_test.layers_ready(stale, max_age_minutes=5) is False

    empty = {**fresh, "raw": {"files": 0, "age": None}}
    assert smoke_test.layers_ready(empty, max_age_minutes=5) is False
