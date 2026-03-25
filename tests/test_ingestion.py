"""
tests/test_ingestion.py

Unit + integration tests for the wikit ingestion pipeline:
  - sse_producer.py  (SSE -> Kafka wiki-raw)
  - spark_ingest.py  (Kafka wiki-raw -> Iceberg)

Unit tests mock all external deps (Kafka, SSE, Spark).
Integration tests require docker compose to be running.

Run all:
    pytest tests/test_ingestion.py -v

Run only unit tests (no docker needed):
    pytest tests/test_ingestion.py -v -m unit

Run only integration tests:
    pytest tests/test_ingestion.py -v -m integration
"""

import json
import time
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Marks
# ---------------------------------------------------------------------------
pytestmark = []  # individual tests set their own marks

# ---------------------------------------------------------------------------
# Sample payloads
# ---------------------------------------------------------------------------

SAMPLE_EDIT_EVENT = {
    "id": 123456789,
    "type": "edit",
    "namespace": 0,
    "title": "Python (programming language)",
    "comment": "fix typo",
    "parsedcomment": "fix typo",
    "timestamp": int(time.time()),
    "user": "WikiEditor",
    "bot": False,
    "minor": True,
    "old_len": 5000,
    "new_len": 5005,
    "rev_id": 987654321,
    "wiki": "enwiki",
    "server_name": "en.wikipedia.org",
}

SAMPLE_NON_EDIT_EVENT = {**SAMPLE_EDIT_EVENT, "type": "log", "rev_id": None}


def make_sse_event(data: dict | None, event_id: str = "ev-1") -> SimpleNamespace:
    """Fake SSEClient event object."""
    e = SimpleNamespace()
    e.data = json.dumps(data) if data is not None else ""
    e.id = event_id
    return e


# ===========================================================================
# SSE PRODUCER TESTS
# ===========================================================================


class TestBuildProducer:
    """sse_producer.build_producer() wires KafkaProducer correctly."""

    @pytest.mark.unit
    def test_uses_env_bootstrap(self, monkeypatch):
        # KAFKA_BOOTSTRAP is read at module load time, so we must reload
        # after setting the env var, then patch on the reloaded module.
        import importlib
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
        from ingestion import sse_producer
        importlib.reload(sse_producer)

        with patch("ingestion.sse_producer.KafkaProducer") as mock_kp:
            sse_producer.build_producer()
            mock_kp.assert_called_once()
            kwargs = mock_kp.call_args.kwargs
            assert kwargs["bootstrap_servers"] == "broker:9092"

    @pytest.mark.unit
    def test_default_bootstrap(self, monkeypatch):
        import importlib
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        from ingestion import sse_producer
        importlib.reload(sse_producer)

        with patch("ingestion.sse_producer.KafkaProducer") as mock_kp:
            sse_producer.build_producer()
            kwargs = mock_kp.call_args.kwargs
            assert kwargs["bootstrap_servers"] == "localhost:29092"

    @pytest.mark.unit
    @patch("ingestion.sse_producer.KafkaProducer")
    def test_acks_all_and_lz4(self, mock_kp):
        from ingestion import sse_producer

        sse_producer.build_producer()
        kwargs = mock_kp.call_args.kwargs
        assert kwargs["acks"] == "all"
        assert kwargs["compression_type"] == "lz4"


class TestStreamFunction:
    """sse_producer.stream() filters and forwards events correctly."""

    @pytest.mark.unit
    def _run_stream(self, events: list, last_id: str | None = None):
        """Helper: patch requests + sseclient and run stream()."""
        from ingestion import sse_producer

        mock_producer = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("ingestion.sse_producer.requests.get", return_value=mock_resp), \
             patch("ingestion.sse_producer.sseclient.SSEClient") as mock_sse:
            mock_sse.return_value.events.return_value = iter(events)
            returned_id = sse_producer.stream(mock_producer, last_id)

        return mock_producer, returned_id

    @pytest.mark.unit
    def test_edit_event_is_sent(self):
        events = [make_sse_event(SAMPLE_EDIT_EVENT, "id-1")]
        producer, _ = self._run_stream(events)
        producer.send.assert_called_once()
        args = producer.send.call_args
        assert args.kwargs["key"] == "enwiki"
        assert args.kwargs["value"]["type"] == "edit"

    @pytest.mark.unit
    def test_non_edit_event_is_skipped(self):
        events = [make_sse_event(SAMPLE_NON_EDIT_EVENT)]
        producer, _ = self._run_stream(events)
        producer.send.assert_not_called()

    @pytest.mark.unit
    def test_empty_data_is_skipped(self):
        empty = SimpleNamespace(data="", id="x")
        producer, _ = self._run_stream([empty])
        producer.send.assert_not_called()

    @pytest.mark.unit
    def test_invalid_json_is_skipped(self):
        bad = SimpleNamespace(data="not-json", id="x")
        producer, _ = self._run_stream([bad])
        producer.send.assert_not_called()

    @pytest.mark.unit
    def test_returns_last_event_id(self):
        events = [make_sse_event(SAMPLE_EDIT_EVENT, "final-id")]
        _, returned = self._run_stream(events)
        assert returned == "final-id"

    @pytest.mark.unit
    def test_flush_every_200_events(self):
        events = [make_sse_event({**SAMPLE_EDIT_EVENT, "id": i}, str(i)) for i in range(201)]
        producer, _ = self._run_stream(events)
        assert producer.flush.called

    @pytest.mark.unit
    def test_sends_to_correct_topic(self, monkeypatch):
        import importlib
        monkeypatch.setenv("KAFKA_TOPIC", "wiki-raw")
        from ingestion import sse_producer
        importlib.reload(sse_producer)  # pick up new env

        mock_producer = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("ingestion.sse_producer.requests.get", return_value=mock_resp), \
             patch("ingestion.sse_producer.sseclient.SSEClient") as mock_sse:
            mock_sse.return_value.events.return_value = iter([make_sse_event(SAMPLE_EDIT_EVENT)])
            sse_producer.stream(mock_producer, None)

        send_call = mock_producer.send.call_args
        assert send_call.args[0] == "wiki-raw"

    @pytest.mark.unit
    def test_last_event_id_sent_as_header(self):
        from ingestion import sse_producer

        mock_producer = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("ingestion.sse_producer.requests.get", return_value=mock_resp) as mock_get, \
             patch("ingestion.sse_producer.sseclient.SSEClient") as mock_sse:
            mock_sse.return_value.events.return_value = iter([])
            sse_producer.stream(mock_producer, last_event_id="resume-from-here")

        headers = mock_get.call_args.kwargs["headers"]
        assert headers.get("Last-Event-ID") == "resume-from-here"


# ===========================================================================
# SPARK INGEST TESTS
# ===========================================================================


class TestEnsureTable:
    """spark_ingest.ensure_table() runs the correct DDL."""

    @pytest.mark.unit
    def test_creates_namespace_and_table(self):
        from ingestion import spark_ingest

        mock_spark = MagicMock()
        spark_ingest.ensure_table(mock_spark)

        sql_calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert any("CREATE NAMESPACE" in s for s in sql_calls)
        assert any("CREATE TABLE" in s and "wiki_events_raw" in s for s in sql_calls)

    @pytest.mark.unit
    def test_table_partitioned_by_event_ts_and_wiki(self):
        from ingestion import spark_ingest

        mock_spark = MagicMock()
        spark_ingest.ensure_table(mock_spark)

        ddl = next(
            c.args[0]
            for c in mock_spark.sql.call_args_list
            if "CREATE TABLE" in c.args[0]
        )
        assert "days(event_ts)" in ddl
        assert "wiki" in ddl

    @pytest.mark.unit
    def test_uses_iceberg_format(self):
        from ingestion import spark_ingest

        mock_spark = MagicMock()
        spark_ingest.ensure_table(mock_spark)

        ddl = next(
            c.args[0]
            for c in mock_spark.sql.call_args_list
            if "CREATE TABLE" in c.args[0]
        )
        assert "USING iceberg" in ddl


class TestWikiSchema:
    """WIKI_SCHEMA has all expected fields with correct types."""

    @pytest.mark.unit
    def test_required_fields_present(self):
        from ingestion.spark_ingest import WIKI_SCHEMA

        field_names = {f.name for f in WIKI_SCHEMA.fields}
        required = {"id", "type", "title", "timestamp", "user", "bot", "minor",
                    "wiki", "rev_id", "old_len", "new_len"}
        assert required <= field_names

    @pytest.mark.unit
    def test_bot_is_boolean(self):
        from pyspark.sql.types import BooleanType
        from ingestion.spark_ingest import WIKI_SCHEMA

        bot_field = next(f for f in WIKI_SCHEMA.fields if f.name == "bot")
        assert isinstance(bot_field.dataType, BooleanType)

    @pytest.mark.unit
    def test_id_is_long(self):
        from pyspark.sql.types import LongType
        from ingestion.spark_ingest import WIKI_SCHEMA

        id_field = next(f for f in WIKI_SCHEMA.fields if f.name == "id")
        assert isinstance(id_field.dataType, LongType)


# ===========================================================================
# INTEGRATION TESTS  (require docker compose up)
# ===========================================================================


@pytest.mark.integration
class TestKafkaIntegration:
    """End-to-end: produce to wiki-raw and read back — no mocks."""

    BOOTSTRAP = "localhost:29092"

    @pytest.fixture(autouse=True)
    def _import_kafka(self):
        try:
            from kafka import KafkaConsumer, KafkaProducer
            from kafka.errors import NoBrokersAvailable
        except ImportError:
            pytest.skip("kafka-python not installed")

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all",
                retries=3,
            )
        except Exception:
            pytest.skip(f"Kafka not reachable at {self.BOOTSTRAP}")

        yield
        self._producer.close()

    def _fresh_consumer(self, topic: str):
        from kafka import KafkaConsumer

        return KafkaConsumer(
            topic,
            bootstrap_servers=self.BOOTSTRAP,
            group_id=f"test-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
            consumer_timeout_ms=12_000,
            value_deserializer=lambda v: json.loads(v.decode()),
        )

    def _consume_by_key(self, consumer, key: str) -> dict | None:
        for msg in consumer:
            if msg.key and msg.key.decode() == key:
                return msg.value
        return None

    # --- wiki-raw ---

    def test_produce_raw_event(self):
        key = f"integ-{uuid.uuid4().hex}"
        payload = {**SAMPLE_EDIT_EVENT, "wiki": key}
        self._producer.send("wiki-raw", key=key, value=payload).get(timeout=10)

        consumer = self._fresh_consumer("wiki-raw")
        received = self._consume_by_key(consumer, key)
        consumer.close()

        assert received is not None, "Message not found in wiki-raw"
        assert received["type"] == "edit"
        assert received["rev_id"] == SAMPLE_EDIT_EVENT["rev_id"]

    def test_only_edit_events_format_is_accepted(self):
        """Verify the message schema matches what spark_ingest expects."""
        key = f"schema-{uuid.uuid4().hex}"
        self._producer.send("wiki-raw", key=key, value=SAMPLE_EDIT_EVENT).get(timeout=10)

        consumer = self._fresh_consumer("wiki-raw")
        msg = self._consume_by_key(consumer, key)
        consumer.close()

        assert msg is not None
        for field in ("id", "type", "title", "timestamp", "user", "bot",
                      "minor", "wiki", "rev_id", "old_len", "new_len"):
            assert field in msg, f"Missing field: {field}"

    def test_null_rev_id_is_filtered_by_spark(self):
        """
        spark_ingest filters out rows where rev_id IS NULL.
        Produce such a message and confirm it exists in the topic
        (filtering happens in Spark, not at produce time).
        """
        key = f"null-rev-{uuid.uuid4().hex}"
        bad_payload = {**SAMPLE_EDIT_EVENT, "rev_id": None, "wiki": key}
        self._producer.send("wiki-raw", key=key, value=bad_payload).get(timeout=10)

        consumer = self._fresh_consumer("wiki-raw")
        msg = self._consume_by_key(consumer, key)
        consumer.close()

        assert msg is not None, "Message with null rev_id should still land in topic"
        assert msg["rev_id"] is None

    def test_message_throughput_100_events(self):
        """Produce 100 events without errors."""
        keys = [f"bulk-{uuid.uuid4().hex}" for _ in range(100)]
        for k in keys:
            self._producer.send("wiki-raw", key=k, value={**SAMPLE_EDIT_EVENT, "wiki": k})
        self._producer.flush()

        consumer = self._fresh_consumer("wiki-raw")
        found = set()
        for msg in consumer:
            if msg.key and msg.key.decode() in keys:
                found.add(msg.key.decode())
            if len(found) == len(keys):
                break
        consumer.close()

        assert len(found) == 100, f"Only received {len(found)}/100 messages"