# Projet Real Time Clickstream Complet

## Architecture
- **Kafka**: Ingestion des événements en temps réel
- **Spark Streaming**: Traitement temps réel
- **Redis**: Stockage rapide pour dashboard
- **HDFS**: Stockage long terme
- **Airflow**: Orchestration des jobs batch
- **Grafana**: Visualisation des données

## Installation

### 1. Prérequis
```bash
# Installer Docker et Docker Compose
sudo apt-get update
sudo apt-get install docker docker-compose

# Installer Python et dépendances
python3 -m venv venv
source venv/bin/activate
pip install kafka-python faker pyspark redis pandas