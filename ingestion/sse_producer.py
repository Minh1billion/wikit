"""
sse_producer.py
Subscribes to the Wikimedia RecentChange SSE stream and forwards
'edit' events to Kafka topic 'wiki-raw'.

Run:
    python sse_producer.py

Env vars (optional override):
    KAFKA_BOOTSTRAP_SERVERS  default: localhost:29092
    KAFKA_TOPIC              default: wiki-raw
"""

import json
import logging
import os
import time

import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "wiki-raw")
SSE_URL         = "https://stream.wikimedia.org/v2/stream/recentchange"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        compression_type="lz4",
        linger_ms=10,
    )


def stream(producer: KafkaProducer, last_event_id: str | None) -> str | None:
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "wikit-stream/1.0",
    }
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    resp = requests.get(SSE_URL, stream=True, headers=headers, timeout=30)
    resp.raise_for_status()
    log.info("Connected (HTTP %d)", resp.status_code)

    count = 0
    for event in sseclient.SSEClient(resp).events():
        if not event.data:
            continue
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            continue

        if data.get("type") != "edit":
            continue

        producer.send(
            KAFKA_TOPIC,
            key=data.get("wiki", "unknown"),
            value=data,
        )

        count += 1
        if count % 200 == 0:
            producer.flush()
            log.info(
                "Sent %d | wiki=%-10s | %s",
                count,
                data.get("wiki", "?"),
                data.get("title", "")[:60],
            )

        last_event_id = event.id or last_event_id

    return last_event_id


def main() -> None:
    log.info("Connecting to Kafka at %s …", KAFKA_BOOTSTRAP)
    producer = build_producer()

    delay    = 5
    last_id  = None

    while True:
        try:
            last_id = stream(producer, last_id)
            delay = 5
        except requests.exceptions.RequestException as exc:
            log.error("SSE error: %s — retry in %ds", exc, delay)
        except KafkaError as exc:
            log.error("Kafka error: %s — retry in %ds", exc, delay)
        except KeyboardInterrupt:
            log.info("Stopping …")
            break
        except Exception as exc:
            log.exception("Unexpected: %s — retry in %ds", exc, delay)
        finally:
            producer.flush()

        time.sleep(delay)
    delay = min(delay * 2, 60)


if __name__ == "__main__":
    main()