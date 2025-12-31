#!/bin/bash

# Script de démarrage complet du projet Real Time Clickstream
# Usage: ./run_project.sh [start|stop|restart|status|logs]

set -e  # Arrêter en cas d'erreur

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonction pour afficher les messages
info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Fonction pour vérifier les prérequis
check_prerequisites() {
    info "Vérification des prérequis..."

    # Vérifier Docker
    if ! command -v docker &> /dev/null; then
        error "Docker n'est pas installé"
        exit 1
    fi
    success "Docker: $(docker --version)"

    # Vérifier Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose n'est pas installé"
        exit 1
    fi
    success "Docker Compose: $(docker-compose --version)"

    # Vérifier Python
    if ! command -v python3 &> /dev/null; then
        error "Python3 n'est pas installé"
        exit 1
    fi
    success "Python: $(python3 --version)"
}

# Fonction pour démarrer l'infrastructure Docker
start_docker() {
    info "Démarrage de l'infrastructure Docker..."

    # Arrêter les conteneurs existants si nécessaire
    docker-compose down 2>/dev/null || true

    # Démarrer tous les services
    docker-compose up -d

    success "Infrastructure Docker démarrée"

    # Attendre que les services soient prêts
    info "Attente du démarrage des services (60 secondes)..."
    sleep 60

    # Vérifier l'état des conteneurs
    docker-compose ps
}

# Fonction pour initialiser Kafka et HDFS
init_services() {
    info "Initialisation de Kafka et HDFS..."

    # Activer l'environnement virtuel si existe
    if [ -d "venv" ]; then
        source venv/bin/activate
    else
        warning "Environnement virtuel non trouvé. Utilisation de Python global."
    fi

    # Exécuter le script d'initialisation
    python3 scripts/init_kafka.py

    success "Kafka et HDFS initialisés"
}

# Fonction pour démarrer le producer en arrière-plan
start_producer() {
    info "Démarrage du Producer Kafka..."

    # Activer l'environnement virtuel si existe
    if [ -d "venv" ]; then
        source venv/bin/activate
    fi

    # Démarrer le producer en arrière-plan avec auto-confirmation
    nohup python3 scripts/producer.py <<EOF > logs/producer.log 2>&1 &
n
EOF

    PRODUCER_PID=$!
    echo $PRODUCER_PID > logs/producer.pid

    success "Producer démarré (PID: $PRODUCER_PID)"
    info "Logs: tail -f logs/producer.log"
}

# Fonction pour démarrer Spark Streaming
start_spark() {
    info "Démarrage de Spark Streaming..."

    # Attendre que Spark soit prêt
    sleep 10

    # Démarrer Spark Streaming en arrière-plan
    nohup docker exec spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        /opt/spark-apps/stream_processor.py > logs/spark_streaming.log 2>&1 &

    SPARK_PID=$!
    echo $SPARK_PID > logs/spark.pid

    success "Spark Streaming démarré (PID: $SPARK_PID)"
    info "Logs: tail -f logs/spark_streaming.log"
}

# Fonction pour afficher l'état du système
show_status() {
    echo ""
    info "=========================================="
    info "     ÉTAT DU SYSTÈME CLICKSTREAM"
    info "=========================================="
    echo ""

    # État Docker
    info "📦 Conteneurs Docker:"
    docker-compose ps
    echo ""

    # État Producer
    if [ -f "logs/producer.pid" ]; then
        PRODUCER_PID=$(cat logs/producer.pid)
        if ps -p $PRODUCER_PID > /dev/null 2>&1; then
            success "Producer: En cours (PID: $PRODUCER_PID)"
        else
            warning "Producer: Arrêté"
        fi
    else
        warning "Producer: Non démarré"
    fi

    # État Spark
    if [ -f "logs/spark.pid" ]; then
        SPARK_PID=$(cat logs/spark.pid)
        if ps -p $SPARK_PID > /dev/null 2>&1; then
            success "Spark Streaming: En cours (PID: $SPARK_PID)"
        else
            warning "Spark Streaming: Arrêté"
        fi
    else
        warning "Spark Streaming: Non démarré"
    fi

    echo ""
    info "🌐 Interfaces Web:"
    echo "  • Kafka UI:        http://localhost:8080"
    echo "  • Spark Master UI: http://localhost:8081"
    echo "  • Spark Worker UI: http://localhost:8083"
    echo "  • HDFS NameNode:   http://localhost:9870"
    echo "  • Grafana:         http://localhost:3000"
    echo "  • Airflow:         http://localhost:8085"
    echo ""
}

# Fonction pour arrêter tous les services
stop_services() {
    info "Arrêt de tous les services..."

    # Arrêter le Producer
    if [ -f "logs/producer.pid" ]; then
        PRODUCER_PID=$(cat logs/producer.pid)
        if ps -p $PRODUCER_PID > /dev/null 2>&1; then
            kill $PRODUCER_PID 2>/dev/null || true
            success "Producer arrêté"
        fi
        rm -f logs/producer.pid
    fi

    # Arrêter Spark
    if [ -f "logs/spark.pid" ]; then
        SPARK_PID=$(cat logs/spark.pid)
        if ps -p $SPARK_PID > /dev/null 2>&1; then
            kill $SPARK_PID 2>/dev/null || true
            success "Spark Streaming arrêté"
        fi
        rm -f logs/spark.pid
    fi

    # Arrêter Docker
    docker-compose down
    success "Infrastructure Docker arrêtée"
}

# Fonction pour afficher les logs
show_logs() {
    info "Logs disponibles:"
    echo "1. Logs Producer"
    echo "2. Logs Spark Streaming"
    echo "3. Logs Docker (tous)"
    echo "4. Logs Kafka"
    echo "5. Logs Redis"
    read -p "Choisir (1-5): " choice

    case $choice in
        1) tail -f logs/producer.log ;;
        2) tail -f logs/spark_streaming.log ;;
        3) docker-compose logs -f ;;
        4) docker-compose logs -f kafka ;;
        5) docker-compose logs -f redis ;;
        *) error "Choix invalide" ;;
    esac
}

# Fonction pour démarrer tout le projet
start_all() {
    echo ""
    info "=========================================="
    info "  DÉMARRAGE DU PROJET CLICKSTREAM"
    info "=========================================="
    echo ""

    # Créer le dossier logs s'il n'existe pas
    mkdir -p logs

    # Vérifier les prérequis
    check_prerequisites

    # Démarrer Docker
    start_docker

    # Initialiser les services
    init_services

    # Démarrer le Producer
    start_producer

    # Attendre un peu avant Spark
    sleep 5

    # Démarrer Spark
    start_spark

    # Attendre que tout démarre
    sleep 10

    # Afficher l'état
    show_status

    echo ""
    success "=========================================="
    success "  ✅ PROJET DÉMARRÉ AVEC SUCCÈS!"
    success "=========================================="
    echo ""

    info "Commandes utiles:"
    echo "  • Voir les logs Producer:  tail -f logs/producer.log"
    echo "  • Voir les logs Spark:     tail -f logs/spark_streaming.log"
    echo "  • Voir l'état:             ./run_project.sh status"
    echo "  • Arrêter le projet:       ./run_project.sh stop"
    echo ""
}

# Menu principal
case "${1:-start}" in
    start)
        start_all
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 5
        start_all
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "Commandes:"
        echo "  start   - Démarrer tout le projet"
        echo "  stop    - Arrêter tous les services"
        echo "  restart - Redémarrer le projet"
        echo "  status  - Afficher l'état du système"
        echo "  logs    - Afficher les logs"
        exit 1
        ;;
esac
