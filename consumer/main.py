import json
import logging
import os
import time
from datetime import datetime
import pandas as pd
from google.cloud import storage
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "opensky_flights_raw")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
FLUSH_INTERVAL_SECONDS = 60


def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="gcs_parquet_ingestor_group",
            )
            logging.info("Consumer connected to Kafka Broker at %s", KAFKA_BOOTSTRAP_SERVERS)
            return consumer
        except Exception as e:
            logging.warning("Kafka broker unavailable for consumer (%s). Retrying in 5 seconds...", str(e))
            time.sleep(5)


def upload_parquet_to_gcs(records, gcs_client, bucket_name):
    if not records:
        return

    df = pd.DataFrame(records)

    dtype_spec = {
        "snapshot_timestamp": "Int64",
        "icao24": "string",
        "callsign": "string",
        "origin_country": "string",
        "time_position": "Int64",
        "last_contact": "Int64",
        "longitude": "float64",
        "latitude": "float64",
        "baro_altitude": "float64",
        "on_ground": "boolean",
        "velocity": "float64",
        "true_track": "float64",
        "vertical_rate": "float64",
        "geo_altitude": "float64",
        "squawk": "string",
        "spi": "boolean",
        "position_source": "Int64",
        "ingested_at": "Int64",
    }

    for col, dtype in dtype_spec.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as ex:
                logging.debug("Could not convert column %s to %s: %s", col, dtype, str(ex))

    now = datetime.utcnow()
    filename = f"flights_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    local_path = f"/tmp/{filename}"
    gcs_blob_path = f"raw/flights/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/{filename}"

    df.to_parquet(local_path, index=False, engine="pyarrow")
    logging.info("Converted %d records to Parquet file: %s", len(records), local_path)

    if bucket_name and gcs_client:
        try:
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(gcs_blob_path)
            blob.upload_from_filename(local_path)
            logging.info("Successfully uploaded Parquet to gs://%s/%s", bucket_name, gcs_blob_path)
        except Exception as e:
            logging.error("Failed to upload Parquet to GCS bucket %s: %s", bucket_name, str(e))
    else:
        logging.warning("GCS_BUCKET_NAME not configured or client null. Parquet stored at %s", local_path)

    if os.path.exists(local_path):
        os.remove(local_path)


def consume_and_batch():
    consumer = get_kafka_consumer()
    gcs_client = None
    if GCS_BUCKET_NAME:
        try:
            gcs_client = storage.Client()
        except Exception as e:
            logging.warning("Could not initialize GCS client: %s", str(e))

    buffer = []
    last_flush_time = time.time()

    logging.info("Starting Kafka to GCS Consumer Loop (60s batch interval)...")

    while True:
        try:
            poll_records = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in poll_records.items():
                for message in messages:
                    buffer.append(message.value)

            current_time = time.time()
            if (current_time - last_flush_time) >= FLUSH_INTERVAL_SECONDS:
                if buffer:
                    logging.info("Flushing %d records to GCS...", len(buffer))
                    upload_parquet_to_gcs(buffer, gcs_client, GCS_BUCKET_NAME)
                    buffer.clear()
                else:
                    logging.info("No records in buffer during flush cycle.")
                last_flush_time = current_time

        except Exception as e:
            logging.error("Error in consumer loop: %s", str(e))
            time.sleep(2)


if __name__ == "__main__":
    consume_and_batch()
