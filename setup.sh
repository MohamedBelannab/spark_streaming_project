#!/bin/bash

# Script de configuration initiale du projet
# À exécuter UNE SEULE FOIS avant le premier démarrage

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  CONFIGURATION INITIALE DU PROJET${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# 1. Créer les dossiers nécessaires
echo -e "${BLUE}📁 Création des dossiers...${NC}"
mkdir -p logs
mkdir -p data
echo -e "${GREEN}✅ Dossiers créés${NC}"

# 2. Créer l'environnement virtuel Python
if [ ! -d "venv" ]; then
    echo -e "${BLUE}🐍 Création de l'environnement virtuel Python...${NC}"
    python3 -m venv venv
    echo -e "${GREEN}✅ Environnement virtuel créé${NC}"
else
    echo -e "${GREEN}✅ Environnement virtuel déjà existant${NC}"
fi

# 3. Activer l'environnement et installer les dépendances
echo -e "${BLUE}📦 Installation des dépendances Python...${NC}"
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo -e "${GREEN}✅ Dépendances installées${NC}"

# 4. Rendre les scripts exécutables
echo -e "${BLUE}🔧 Configuration des permissions...${NC}"
chmod +x run_project.sh
chmod +x scripts/*.py
echo -e "${GREEN}✅ Permissions configurées${NC}"

# 5. Vérifier le fichier .env
if [ ! -s ".env" ]; then
    echo -e "${BLUE}📝 Création du fichier .env...${NC}"
    cat > .env <<EOL
# Kafka Configuration
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=clickstream

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379

# HDFS Configuration
HDFS_HOST=localhost
HDFS_PORT=9000

# Spark Configuration
SPARK_MASTER=spark://localhost:7077
EOL
    echo -e "${GREEN}✅ Fichier .env créé${NC}"
else
    echo -e "${GREEN}✅ Fichier .env existant${NC}"
fi

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}  ✅ CONFIGURATION TERMINÉE!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo "Prochaines étapes:"
echo "  1. Démarrer le projet:  ./run_project.sh start"
echo "  2. Voir l'état:         ./run_project.sh status"
echo "  3. Voir les logs:       ./run_project.sh logs"
echo ""
