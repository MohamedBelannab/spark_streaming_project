#!/bin/bash

# ==============================================================================
# Script: run_project.sh (Version 2025.6 - CI/CD Optimized) - MODIFIÉ
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
    # Si GitHub Actions, les dépendances sont gérées par le YAML
    if [ "$CI" == "true" ]; then
        info "CI Mode détecté: Utilisation de l'environnement GitHub."
    else
        # Mode Local: Utiliser ou créer venv
        if [ ! -d "venv" ]; then
            info "Mode Local: Création de l'environnement virtuel..."
            python3 -m venv venv
            source venv/bin/activate
            pip install -r requirements.txt
        else
            source venv/bin/activate || {
                error "Échec de l'activation de venv. Recréation..."
                rm -rf venv
                python3 -m venv venv
                source venv/bin/activate
                pip install -r requirements.txt
            }
        fi
    fi
}

stop_hard() {
    info "ARRÊT COMPLET - Suppression de tous les conteneurs et données..."
    [ -f "$PID_PRODUCER" ] && kill $(cat "$PID_PRODUCER") 2>/dev/null && rm "$PID_PRODUCER" || true
    [ -f "$PID_SPARK" ] && kill $(cat "$PID_SPARK") 2>/dev/null && rm "$PID_SPARK" || true
    
    # Avec suppression des volumes (-v)
    docker compose down -v --remove-orphans 2>/dev/null || true

    # Force remove any lingering containers by name
    docker rm -f hive-metastore-postgresql redis spark-master zookeeper namenode \
        datanode redis-ui kafka hive-metastore hive-server spark-worker airflow grafana kafka-ui 2>/dev/null || true
}

stop_soft() {
    info "ARRÊT DOUX - Garde les données..."
    [ -f "$PID_PRODUCER" ] && kill $(cat "$PID_PRODUCER") 2>/dev/null && rm "$PID_PRODUCER" || true
    [ -f "$PID_SPARK" ] && kill $(cat "$PID_SPARK") 2>/dev/null && rm "$PID_SPARK" || true
    
    # SANS suppression des volumes
    docker compose down --remove-orphans 2>/dev/null || true
    success "Conteneurs arrêtés, données préservées."
}

start() {
    local MODE=${1:-soft}  # soft par défaut
    
    mkdir -p "$LOG_DIR"
    
    if [ "$MODE" = "hard" ]; then
        stop_hard
    else
        stop_soft
    fi
    
    setup_env

    info "Lancement de l'infrastructure Docker..."
    docker compose up -d

    info "Attente de 45s pour les services (Kafka/HDFS/Spark)..."
    sleep 45

    info "Initialisation de Kafka et HDFS..."
    python3 scripts/init_kafka.py

    info "Démarrage du Producteur..."
    nohup python3 -u scripts/producer.py --no-interactive < /dev/null > "$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$PID_PRODUCER"

    info "Installation des dépendances Python dans le conteneur Spark..."
    docker exec spark-master pip install redis 2>/dev/null || docker exec spark-master pip3 install redis 2>/dev/null || true

    info "Soumission du Job Spark..."
    nohup docker exec spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
        /opt/spark-apps/stream_processor.py > "$LOG_DIR/spark_streaming.log" 2>&1 &
    echo $! > "$PID_SPARK"

    success "Projet en cours d'exécution !"
}

# Nouvelle commande pour conserver les données
restart() {
    info "Redémarrage en conservant les données..."
    stop_soft
    start "soft"
}

case "$1" in
    start) 
        if [ "$2" = "hard" ]; then
            start "hard"
        else
            start "soft"
        fi
        ;;
    stop)
        if [ "$2" = "hard" ]; then
            stop_hard
        else
            stop_soft
        fi
        ;;
    restart) restart ;;
    status) docker compose ps ;;
    *) 
        echo "Usage: $0 {start|stop|restart|status} [hard|soft]"
        echo ""
        echo "Options:"
        echo "  start [hard]    : Démarrer (hard supprime tout)"
        echo "  stop [hard]     : Arrêter (hard supprime les données)"
        echo "  restart         : Redémarrer en conservant les données"
        echo "  status          : Voir l'état des conteneurs"
        echo ""
        echo "Exemples:"
        echo "  $0 start        # Démarre en conservant les données"
        echo "  $0 start hard   # Démarre avec un nettoyage complet"
        echo "  $0 restart      # Redémarre rapidement avec données"
        exit 1 
        ;;
esac