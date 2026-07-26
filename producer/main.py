import json
import logging
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "opensky_flights_raw")
OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME", "").strip()
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "").strip()
POLL_INTERVAL = int(os.getenv("OPENSKY_POLL_INTERVAL", "15"))
OPENSKY_URL = "https://opensky-network.org/api/states/all"


def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            logging.info("Connected to Kafka Broker at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except Exception as e:
            logging.warning("Kafka broker unavailable (%s). Retrying in 5 seconds...", str(e))
            time.sleep(5)


def safe_get(lst, idx, default=None):
    return lst[idx] if (lst and idx < len(lst)) else default


def parse_state_vector(record_time, state):
    callsign_raw = safe_get(state, 1)
    callsign = callsign_raw.strip() if isinstance(callsign_raw, str) else None

    return {
        "snapshot_timestamp": record_time,
        "icao24": safe_get(state, 0),
        "callsign": callsign,
        "origin_country": safe_get(state, 2),
        "time_position": safe_get(state, 3),
        "last_contact": safe_get(state, 4),
        "longitude": safe_get(state, 5),
        "latitude": safe_get(state, 6),
        "baro_altitude": safe_get(state, 7),
        "on_ground": safe_get(state, 8),
        "velocity": safe_get(state, 9),
        "true_track": safe_get(state, 10),
        "vertical_rate": safe_get(state, 11),
        "geo_altitude": safe_get(state, 13),
        "squawk": safe_get(state, 14),
        "spi": safe_get(state, 15),
        "position_source": safe_get(state, 16),
        "ingested_at": int(time.time()),
    }


def fetch_and_publish():
    producer = get_kafka_producer()
    
    auth = None
    if OPENSKY_USERNAME and OPENSKY_PASSWORD:
        auth = HTTPBasicAuth(OPENSKY_USERNAME, OPENSKY_PASSWORD)
        logging.info("OpenSky Basic Authentication ENABLED for user '%s'", OPENSKY_USERNAME)
    else:
        logging.info("OpenSky Basic Authentication DISABLED (Running in Anonymous Mode)")

    headers = {"User-Agent": "OpenSky-ELT-Producer/1.0"}

    logging.info("Starting OpenSky API Kafka Producer stream (15s poll interval)...")

    while True:
        try:
            response = requests.get(OPENSKY_URL, auth=auth, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                record_time = data.get("time")
                states = data.get("states") or []
                
                count = 0
                for state in states:
                    record = parse_state_vector(record_time, state)
                    producer.send(KAFKA_TOPIC, record)
                    count += 1
                
                producer.flush()
                logging.info("Published %d flight state records to topic '%s' at time %s", count, KAFKA_TOPIC, record_time)
            elif response.status_code == 401:
                logging.error("OpenSky Authentication failed (401 Invalid Credentials). Check OPENSKY_USERNAME & OPENSKY_PASSWORD.")
                time.sleep(30)
            elif response.status_code == 429:
                logging.warning("OpenSky API rate limit reached (429). Sleeping for 60 seconds...")
                time.sleep(60)
            else:
                logging.error("Failed to fetch OpenSky states. HTTP %d: %s", response.status_code, response.text)

        except Exception as e:
            logging.error("Error during poll cycle: %s", str(e))

        # Mandatory 15-second delay between API calls to prevent 429 rate limit errors
        time.sleep(max(15, POLL_INTERVAL))


if __name__ == "__main__":
    fetch_and_publish()
