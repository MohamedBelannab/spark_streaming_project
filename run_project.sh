#!/bin/bash

# ==============================================================================
# Script: run_project.sh (Version 2025.5 - Auto-Venv Support)
# ==============================================================================

set -e 

LOG_DIR="logs"
PID_PRODUCER="$LOG_DIR/producer.pid"
PID_SPARK="$LOG_DIR/spark.pid"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m'

info()    { echo -e "${BLUE}ℹ️  $1${NC}"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }

setup_python_env() {
    if [ ! -d "venv" ]; then
        info "Creating virtual environment and installing requirements..."
        python3 -m venv venv
        source venv/bin/activate
        pip install --upgrade pip
        if [ -f "requirements.txt" ]; then
            pip install -r requirements.txt
        else
            echo "requirements.txt not found! Installing defaults..."
            pip install kafka-python confluent-kafka faker pandas redis pyspark
        fi
    else
        source venv/bin/activate
    fi
}

stop() {
    info "Cleaning up..."
    [ -f "$PID_PRODUCER" ] && kill $(cat "$PID_PRODUCER") 2>/dev/null && rm "$PID_PRODUCER" || true
    [ -f "$PID_SPARK" ] && kill $(cat "$PID_SPARK") 2>/dev/null && rm "$PID_SPARK" || true
    docker compose down --remove-orphans 2>/dev/null || true
    # Force clean conflicts
    docker rm -f zookeeper kafka namenode datanode spark-master spark-worker redis 2>/dev/null || true
}

start() {
    mkdir -p "$LOG_DIR"
    stop
    
    setup_python_env

    info "Launching Docker..."
    docker compose up -d

    info "Waiting 45s for services..."
    sleep 45

    info "Initializing Kafka..."
    python3 scripts/init_kafka.py

    info "Starting Producer..."
    nohup python3 scripts/producer.py < /dev/null > "$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$PID_PRODUCER"

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