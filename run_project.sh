#!/bin/bash

# ==============================================================================
# Script: run_project.sh (Version 2025.6 - CI/CD Optimized)
# ==============================================================================

set -e 

LOG_DIR="logs"
PID_PRODUCER="$LOG_DIR/producer.pid"
PID_SPARK="$LOG_DIR/spark.pid"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${BLUE}ℹ️  $1${NC}"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; }

# --- Environment Management ---
setup_env() {
    # If running in GitHub Actions (CI=true), dependencies are handled by the YAML
    if [ "$CI" == "true" ]; then
        info "CI Mode detected: Using GitHub managed environment."
    else
        # Local Mode: Use or create venv
        if [ ! -d "venv" ]; then
            info "Local Mode: Creating virtual environment..."
            python3 -m venv venv
            source venv/bin/activate
            pip install -r requirements.txt
        else
            source venv/bin/activate || {
                error "Failed to activate venv. Recreating..."
                rm -rf venv
                python3 -m venv venv
                source venv/bin/activate
                pip install -r requirements.txt
            }
        fi
    fi
}

stop() {
    info "Cleaning up processes and containers..."
    [ -f "$PID_PRODUCER" ] && kill $(cat "$PID_PRODUCER") 2>/dev/null && rm "$PID_PRODUCER" || true
    [ -f "$PID_SPARK" ] && kill $(cat "$PID_SPARK") 2>/dev/null && rm "$PID_SPARK" || true
    docker compose down --remove-orphans 2>/dev/null || true

    # Force remove any lingering containers by name
    docker rm -f hive-metastore-postgresql redis spark-master zookeeper namenode \
        datanode kafka hive-metastore hive-server spark-worker airflow grafana kafka-ui 2>/dev/null || true
}

start() {
    mkdir -p "$LOG_DIR"
    stop
    setup_env

    info "Launching Docker Infrastructure..."
    docker compose up -d

    info "Waiting 45s for services (Kafka/HDFS/Spark)..."
    sleep 45

    info "Initializing Kafka and HDFS..."
    python3 scripts/init_kafka.py

    info "Starting Producer..."
    nohup python3 -u scripts/producer.py --no-interactive < /dev/null > "$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$PID_PRODUCER"

    info "Installing Python dependencies in Spark container..."
    docker exec spark-master pip install redis 2>/dev/null || docker exec spark-master pip3 install redis 2>/dev/null || true

    info "Submitting Spark Job..."
    nohup docker exec spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
        /opt/spark-apps/stream_processor.py > "$LOG_DIR/spark_streaming.log" 2>&1 &
    echo $! > "$PID_SPARK"

    success "Project is running!"
}

case "$1" in
    start) start ;;
    stop) stop ;;
    status) docker compose ps ;;
    *) echo "Usage: $0 {start|stop|status}"; exit 1 ;;
esac
