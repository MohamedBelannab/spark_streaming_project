#!/usr/bin/env python3
"""
Script d'initialisation pour créer le topic Kafka
"""

import subprocess
import time

def create_kafka_topic():
    """Créer le topic clickstream dans Kafka"""
    print("🔧 Initialisation de Kafka...")
    
    commands = [
        # Attendre que Kafka soit prêt
        ["docker", "exec", "kafka", "bash", "-c", 
         "for i in {1..30}; do kafka-topics --bootstrap-server localhost:9092 --list && break || sleep 2; done"],
        
        # Créer le topic
        ["docker", "exec", "kafka", "kafka-topics",
         "--create",
         "--topic", "clickstream",
         "--bootstrap-server", "localhost:9092",
         "--partitions", "3",
         "--replication-factor", "1",
         "--config", "retention.ms=604800000",  # 7 jours
         "--config", "cleanup.policy=delete"]
    ]
    
    for cmd in commands:
        print(f"📝 Exécution: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Succès: {result.stdout}")
            else:
                print(f"⚠️  Note: {result.stderr}")
        except Exception as e:
            print(f"❌ Erreur: {e}")
    
    # Vérifier le topic
    print("\n🔍 Vérification du topic...")
    check_cmd = ["docker", "exec", "kafka", "kafka-topics",
                 "--describe",
                 "--topic", "clickstream",
                 "--bootstrap-server", "localhost:9092"]
    
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Topic créé avec succès:")
        print(result.stdout)
    else:
        print("❌ Erreur vérification:")
        print(result.stderr)

def init_hdfs():
    """Initialiser les répertoires HDFS"""
    print("\n🗄️  Initialisation HDFS...")
    
    commands = [
        # Créer les répertoires clickstream
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream"],
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/raw"],
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/aggregations"],
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/sessions"],
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/reports"],
        
        # Vérifier les permissions
        ["docker", "exec", "namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/clickstream"]
    ]
    
    for cmd in commands:
        print(f"📁 Exécution: {' '.join(cmd[3:])}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Succès")
            else:
                print(f"⚠️  Note: {result.stderr}")
        except Exception as e:
            print(f"❌ Erreur: {e}")
    
    # Lister les répertoires créés
    print("\n📂 Contenu HDFS:")
    list_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "-R", "/clickstream"]
    result = subprocess.run(list_cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)

def main():
    print("=" * 60)
    print("🚀 INITIALISATION DU PROJET CLICKSTREAM")
    print("=" * 60)
    
    # Initialiser Kafka
    create_kafka_topic()
    
    # Initialiser HDFS
    init_hdfs()
    
    print("\n" + "=" * 60)
    print("✅ INITIALISATION TERMINÉE")
    print("=" * 60)
    print("\nProchaines étapes:")
    print("1. 📤 Lancer le producer: python scripts/producer.py")
    print("2. ⚡ Lancer Spark Streaming: python spark-jobs/stream_processor.py")
    print("3. 📊 Accéder aux interfaces:")
    print("   - Grafana: http://localhost:3000")
    print("   - Kafka UI: http://localhost:8080")
    print("   - Airflow: http://localhost:8085")
    print("   - Spark UI: http://localhost:8081")

if __name__ == "__main__":
    main()