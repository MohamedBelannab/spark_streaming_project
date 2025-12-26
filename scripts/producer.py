#!/usr/bin/env python3
"""
Kafka Producer - Clickstream (Version robuste & production-ready)
"""

import json
import time
import csv
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ==============================
# CONFIGURATION
# ==============================
KAFKA_BROKER = "localhost:9092"   # ⚠️ kafka:9092 si exécuté depuis Docker
TOPIC_NAME = "clickstream"
CSV_PATH = "data/clickstream_events.csv"

# ==============================
# UTILS
# ==============================
def clean_user_id(value):
    """Convertir user_092 / u92 → 92"""
    if not value:
        return None
    value = value.replace("user_", "").replace("u", "")
    return int(value) if value.isdigit() else None


def safe_int(value):
    return int(value) if value and value.isdigit() else None


def safe_float(value):
    try:
        return float(value) if value else None
    except ValueError:
        return None


# ==============================
# PRODUCER
# ==============================
def send_csv_to_kafka():
    print("=" * 50)
    print("🚀 PRODUCER KAFKA - CLICKSTREAM")
    print("=" * 50)
    print(f"📖 Lecture du fichier : {CSV_PATH}")

    # Connexion Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=10
        )
        print("✅ Connexion Kafka établie")
    except KafkaError as e:
        print(f"❌ Impossible de se connecter à Kafka : {e}")
        sys.exit(1)

    sent, errors = 0, 0
    start_time = time.time()

    try:
        with open(CSV_PATH, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for line_num, row in enumerate(reader, start=1):

                # 🚨 Ignorer lignes invalides
                if not row or "user_id" not in row:
                    errors += 1
                    continue

                try:
                    event = {
                        "user_id": clean_user_id(row.get("user_id")),
                        "timestamp": row.get("timestamp"),
                        "page": row.get("page"),
                        "action": row.get("action"),
                        "product_id": safe_int(row.get("product_id")),
                        "product_category": row.get("product_category"),
                        "session_id": row.get("session_id"),
                        "ip_address": row.get("ip_address"),
                        "user_agent": row.get("user_agent"),
                        "duration_seconds": safe_int(row.get("duration_seconds")),
                        "referrer": row.get("referrer"),
                        "location": row.get("location"),
                        "device_type": row.get("device_type"),
                        "purchase_amount": safe_float(row.get("purchase_amount"))
                    }

                    # Envoi Kafka
                    producer.send(
                        topic=TOPIC_NAME,
                        key=event["user_id"],
                        value=event
                    )

                    sent += 1

                    # Progress log
                    if sent % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = sent / elapsed if elapsed > 0 else 0
                        print(f"📊 {sent} événements envoyés | {rate:.1f}/sec")

                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"⚠️ Ligne {line_num} ignorée → {e}")

    except FileNotFoundError:
        print(f"❌ Fichier introuvable : {CSV_PATH}")
        sys.exit(1)

    # Finalisation
    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    print("\n" + "=" * 50)
    print("📈 RÉSUMÉ FINAL")
    print(f"   ✔️ Événements envoyés : {sent}")
    print(f"   ❌ Lignes ignorées    : {errors}")
    print(f"   ⏱️ Durée              : {elapsed:.2f} sec")
    if elapsed > 0:
        print(f"   ⚡ Débit moyen        : {sent/elapsed:.1f} evt/sec")
    print("=" * 50)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    send_csv_to_kafka()
