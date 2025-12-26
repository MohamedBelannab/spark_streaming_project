Projet Real Time Clickstream CompletArchitectureKafka: Ingestion des événements en temps réel (Broker & Zookeeper).Spark Streaming: Traitement, analyse et double écriture (HDFS/Redis).Redis: Couche "Hot" pour le stockage des agrégats en temps réel.HDFS: Couche "Cold" (Data Lake) pour le stockage long terme au format Parquet.Airflow: Orchestration des futurs jobs batch et maintenance.Grafana: Dashboard interactif pour la visualisation des données live.Installation1. PrérequisBash# Mise à jour du système
sudo apt-get update

# Installation de Docker et Docker Compose (si non installé)
sudo apt-get install docker.io docker-compose

# Préparation de l'environnement Python
python3 -m venv venv
source venv/bin/activate
pip install kafka-python pyspark==3.0.0 redis pandas confluent-kafka
2. Démarrage de l'InfrastructureBash# Lancement de tous les services Docker
sudo docker compose up -d

# Vérification de l'état des conteneurs
sudo docker ps
Guide d'Exécution1. Configuration de HDFSAvant de lancer Spark, il faut s'assurer que les dossiers de réception existent dans le Data Lake.Bash# Création des répertoires HDFS
sudo docker exec namenode hdfs dfs -mkdir -p /user/clickstream/output
sudo docker exec namenode hdfs dfs -mkdir -p /user/clickstream/checkpoint_hdfs
sudo docker exec namenode hdfs dfs -mkdir -p /user/clickstream/checkpoint_redis

# Attribution des droits d'écriture
sudo docker exec namenode hdfs dfs -chmod -R 777 /user/clickstream
2. Lancement du Traitement Spark (Terminal 1)Ce script traite le flux Kafka et écrit simultanément dans HDFS et Redis.Bash# Copier le script dans le conteneur
sudo docker cp scripts/spark_stream.py spark-master:/tmp/spark_stream.py

# Soumettre le job Spark
sudo docker exec -it spark-master /spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  --master spark://spark-master:7077 \
  /tmp/spark_stream.py
3. Lancement de l'Ingestion (Terminal 2)Simulation de l'activité utilisateur via le producteur Python.Bashsource venv/bin/activate
python3 scripts/producer.py
Visualisation et MonitoringMonitoring TechniqueComposantCommande de vérificationHDFSsudo docker exec namenode hdfs dfs -ls -R /user/clickstream/outputRedissudo docker exec -it redis redis-cli GET "page:/home"Kafkasudo docker exec kafka kafka-topics --list --bootstrap-server localhost:9092Accès aux Interfaces WebServiceURLIdentifiantsGrafanahttp://<VM_IP>:3000admin / adminKafka UIhttp://<VM_IP>:8080-Spark Masterhttp://<VM_IP>:8081-HDFS UIhttp://<VM_IP>:9870-