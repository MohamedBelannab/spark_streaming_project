# #!/usr/bin/env python3
# """
# Script d'initialisation pour créer le topic Kafka
# """

# import subprocess
# import time

# def create_kafka_topic():
#     """Créer le topic clickstream dans Kafka"""
#     print("🔧 Initialisation de Kafka...")
    
#     commands = [
#         # Attendre que Kafka soit prêt
#         ["docker", "exec", "kafka", "bash", "-c", 
#          "for i in {1..30}; do kafka-topics --bootstrap-server localhost:9092 --list && break || sleep 2; done"],
        
#         # Créer le topic
#         ["docker", "exec", "kafka", "kafka-topics",
#          "--create",
#          "--topic", "clickstream",
#          "--bootstrap-server", "localhost:9092",
#          "--partitions", "3",
#          "--replication-factor", "1",
#          "--config", "retention.ms=604800000",  # 7 jours
#          "--config", "cleanup.policy=delete"]
#     ]
    
#     for cmd in commands:
#         print(f"📝 Exécution: {' '.join(cmd)}")
#         try:
#             result = subprocess.run(cmd, capture_output=True, text=True)
#             if result.returncode == 0:
#                 print(f"✅ Succès: {result.stdout}")
#             else:
#                 print(f"⚠️  Note: {result.stderr}")
#         except Exception as e:
#             print(f"❌ Erreur: {e}")
    
#     # Vérifier le topic
#     print("\n🔍 Vérification du topic...")
#     check_cmd = ["docker", "exec", "kafka", "kafka-topics",
#                  "--describe",
#                  "--topic", "clickstream",
#                  "--bootstrap-server", "localhost:9092"]
    
#     result = subprocess.run(check_cmd, capture_output=True, text=True)
#     if result.returncode == 0:
#         print("✅ Topic créé avec succès:")
#         print(result.stdout)
#     else:
#         print("❌ Erreur vérification:")
#         print(result.stderr)

# def init_hdfs():
#     """Initialiser les répertoires HDFS"""
#     print("\n🗄️  Initialisation HDFS...")
    
#     commands = [
#         # Créer les répertoires clickstream
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream"],
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/raw"],
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/aggregations"],
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/sessions"],
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/clickstream/reports"],
        
#         # Vérifier les permissions
#         ["docker", "exec", "namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/clickstream"]
#     ]
    
#     for cmd in commands:
#         print(f"📁 Exécution: {' '.join(cmd[3:])}")
#         try:
#             result = subprocess.run(cmd, capture_output=True, text=True)
#             if result.returncode == 0:
#                 print("✅ Succès")
#             else:
#                 print(f"⚠️  Note: {result.stderr}")
#         except Exception as e:
#             print(f"❌ Erreur: {e}")
    
#     # Lister les répertoires créés
#     print("\n📂 Contenu HDFS:")
#     list_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "-R", "/clickstream"]
#     result = subprocess.run(list_cmd, capture_output=True, text=True)
#     if result.stdout:
#         print(result.stdout)

# def main():
#     print("=" * 60)
#     print("🚀 INITIALISATION DU PROJET CLICKSTREAM")
#     print("=" * 60)
    
#     # Initialiser Kafka
#     create_kafka_topic()
    
#     # Initialiser HDFS
#     init_hdfs()
    
#     print("\n" + "=" * 60)
#     print("✅ INITIALISATION TERMINÉE")
#     print("=" * 60)
#     print("\nProchaines étapes:")
#     print("1. 📤 Lancer le producer: python scripts/producer.py")
#     print("2. ⚡ Lancer Spark Streaming: python spark-jobs/stream_processor.py")
#     print("3. 📊 Accéder aux interfaces:")
#     print("   - Grafana: http://localhost:3000")
#     print("   - Kafka UI: http://localhost:8080")
#     print("   - Airflow: http://localhost:8085")
#     print("   - Spark UI: http://localhost:8081")

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Structured Streaming pour clickstream - Schéma du dataset
Version corrigée
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import approx_count_distinct
import redis
import json
from datetime import datetime
import os
import time
import socket

# Configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "clickstream"
REDIS_HOST = "redis"
REDIS_PORT = 6379
HDFS_PATH = "hdfs://namenode:9000/clickstream"
CHECKPOINT_PATH = "/tmp/spark-checkpoints"

def test_connections():
    """Tester les connexions aux services externes"""
    print("🔍 Test des connexions...")
    
    # Test Kafka
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('kafka', 9092))
        if result == 0:
            print("✅ Kafka: Connecté sur kafka:9092")
        else:
            print(f"❌ Kafka: Erreur (code: {result}) - Vérifiez 'docker start kafka'")
        sock.close()
    except Exception as e:
        print(f"❌ Kafka: {e}")
    
    # Test Redis
    try:
        r = redis.Redis(host='redis', port=6379, socket_connect_timeout=5)
        r.ping()
        print("✅ Redis: Connecté")
    except Exception as e:
        print(f"❌ Redis: {e}")

def init_spark_session():
    """Initialiser la session Spark"""
    return SparkSession.builder \
        .appName("ClickstreamRealTimeProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "500") \
        .config("spark.kafka.bootstrap.servers", "kafka:9092") \
        .config("spark.kafka.producer.bootstrap.servers", "kafka:9092") \
        .getOrCreate()

def get_clickstream_schema():
    """Définir le schéma selon le dataset"""
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("page", StringType(), True),
        StructField("action", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_category", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("referrer", StringType(), True),
        StructField("location", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("purchase_amount", DoubleType(), True)
    ])

def write_to_redis(batch_df, batch_id):
    """Écrire les métriques temps réel dans Redis"""
    try:
        r = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            decode_responses=True, 
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        
        batch_time = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # 1. Statistiques par page
        page_stats = batch_df.groupBy("page").agg(
            count("*").alias("views"),
            approx_count_distinct("user_id").alias("unique_users"),
            avg("duration_seconds").alias("avg_duration"),
            sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
            sum(col("purchase_amount")).alias("revenue")
        ).collect()

        for row in page_stats:
            page_key = f"page:stats:{row['page']}"
            r.hset(page_key, mapping={
                "views": int(row['views'] or 0),
                "unique_users": int(row['unique_users'] or 0),
                "avg_duration": float(row['avg_duration'] or 0),
                "purchases": int(row['purchases'] or 0),
                "revenue": float(row['revenue'] or 0),
                "last_updated": batch_time
            })
            r.expire(page_key, 7200)  # 2 heures

        # 2. Top pages (leaderboard)
        for row in page_stats:
            r.zincrby("leaderboard:pages:views", row['views'] or 0, row['page'])
            r.zincrby("leaderboard:pages:revenue", row['revenue'] or 0, row['page'])
        
        # 3. Utilisateurs actifs
        active_users = batch_df.select("user_id").distinct().collect()
        for row in active_users:
            user_id = str(row['user_id'])
            r.sadd("active:users:current", user_id)
            r.zadd("active:users:history", {user_id: int(time.time())})
        
        r.expire("active:users:current", 300)  # 5 minutes
        r.zremrangebyscore("active:users:history", 0, int(time.time()) - 3600)
        
        # 4. Statistiques par action
        action_stats = batch_df.groupBy("action").agg(
            count("*").alias("count")
        ).collect()
        
        for row in action_stats:
            action_key = f"action:stats:{row['action']}"
            r.hincrby(action_key, "count", int(row['count'] or 0))
            r.expire(action_key, 3600)
        
        # 5. Derniers événements
        recent_events = batch_df.select(
            "user_id", "page", "action", "timestamp", "purchase_amount"
        ).orderBy(desc("timestamp")).limit(10).collect()
        
        for event in recent_events:
            event_data = {
                "user_id": event['user_id'],
                "page": event['page'],
                "action": event['action'],
                "timestamp": event['timestamp'].isoformat() if event['timestamp'] else "",
                "purchase_amount": event['purchase_amount'] or 0
            }
            r.lpush("recent:events", json.dumps(event_data))
        
        r.ltrim("recent:events", 0, 99)
        
        # 6. Métriques globales
        total_events = batch_df.count()
        total_revenue = batch_df.select(sum("purchase_amount")).collect()[0][0] or 0
        total_users = len(active_users)
        
        r.incrby("global:events:total", total_events)
        r.incrbyfloat("global:revenue:total", float(total_revenue))
        r.set("global:last_batch_time", batch_time)
        r.set("global:last_batch_id", batch_id)
        
        # 7. Statistiques géographiques
        geo_stats = batch_df.groupBy("location").agg(
            count("*").alias("events")
        ).collect()
        
        for row in geo_stats:
            if row['location']:
                r.zincrby("geo:activity", row['events'] or 0, row['location'])
        
        # 8. Statistiques appareils
        device_stats = batch_df.groupBy("device_type").agg(
            count("*").alias("events")
        ).collect()
        
        for row in device_stats:
            if row['device_type']:
                r.zincrby("device:usage", row['events'] or 0, row['device_type'])
        
        print(f"✅ Batch {batch_id} -> Redis: {total_events} events, ${total_revenue:.2f} revenue")
        
    except Exception as e:
        print(f"⚠️  Erreur Redis batch {batch_id}: {str(e)[:100]}")

def process_real_time_metrics(df):
    """Traiter les métriques en temps réel"""
    realtime_metrics = df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("page"),
            col("action"),
            col("location"),
            col("device_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("user_id").alias("unique_users"),
            sum(when(col("action") == "purchase", col("purchase_amount")).otherwise(0)).alias("revenue"),
            avg("duration_seconds").alias("avg_duration")
        ) \
        .withColumn("revenue_per_user", 
                   col("revenue") / when(col("unique_users") == 0, 1).otherwise(col("unique_users"))) \
        .withColumn("conversion_rate",
                   when(col("event_count") > 0, 
                        (col("revenue") / col("event_count")) * 100).otherwise(0))
    
    return realtime_metrics

def process_session_analytics(df):
    """Analyser les sessions utilisateur"""
    # Watermark de 30 minutes pour les sessions
    df_with_watermark = df.withWatermark("timestamp", "30 minutes")
    
    window_spec = Window.partitionBy("user_id", "session_id").orderBy("timestamp")
    
    sessions = df_with_watermark \
        .withColumn("prev_timestamp", lag("timestamp", 1).over(window_spec)) \
        .withColumn("time_diff", 
                   unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp"))) \
        .withColumn("is_new_session", 
                   (col("prev_timestamp").isNull()) | (col("time_diff") > 1800)) \
        .withColumn("session_start", 
                   when(col("is_new_session"), col("timestamp")).otherwise(lit(None))) \
        .withColumn("session_id_final", 
                   concat(col("user_id"), lit("_"), 
                          sum(col("is_new_session").cast("int")).over(
                              Window.partitionBy("user_id").orderBy("timestamp")))) \
        .groupBy("user_id", "session_id_final") \
        .agg(
            min("timestamp").alias("session_start"),
            max("timestamp").alias("session_end"),
            count("*").alias("events_per_session"),
            collect_list("page").alias("page_sequence"),
            sum(when(col("action") == "purchase", col("purchase_amount")).otherwise(0)).alias("session_revenue"),
            sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
        ) \
        .withColumn("session_duration",
                   unix_timestamp(col("session_end")) - unix_timestamp(col("session_start")))
    
    return sessions

def main():
    print("=" * 60)
    print("🚀 SPARK STREAMING - CLICKSTREAM PROCESSOR")
    print("=" * 60)
    print(f"📡 Kafka: {KAFKA_BROKER}")
    print(f"📝 Topic: {TOPIC_NAME}")
    print(f"💾 HDFS: {HDFS_PATH}")
    print(f"🔴 Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 60)
    
    # Tester les connexions
    test_connections()
    
    # Initialiser Spark
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        # 1. Lire depuis Kafka
        print("\n📥 Lecture du stream Kafka...")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # 2. Parser le JSON
        schema = get_clickstream_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # 3. Ajouter des colonnes de temps
        enriched_df = parsed_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
            .withWatermark("timestamp", "10 minutes")
        
        # 4. Traiter les métriques en temps réel
        realtime_metrics = process_real_time_metrics(enriched_df)
        
        # 5. Stream 1: Écrire les métriques temps réel dans Redis
        print("\n🔥 Démarrage du stream Redis...")
        redis_stream = realtime_metrics \
            .select("window", "page", "action", "location", "device_type",
                    "event_count", "unique_users", "revenue", 
                    "avg_duration", "revenue_per_user", "conversion_rate") \
            .writeStream \
            .foreachBatch(write_to_redis) \
            .outputMode("append") \
            .trigger(processingTime="1 minute") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/redis") \
            .start()
        
        # 6. Stream 2: Écrire les données brutes dans HDFS
        print("💾 Démarrage du stream HDFS (raw data)...")
        hdfs_raw_stream = enriched_df \
            .writeStream \
            .format("parquet") \
            .option("path", f"{HDFS_PATH}/raw") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/hdfs_raw") \
            .partitionBy("date", "hour") \
            .outputMode("append") \
            .trigger(processingTime="5 minutes") \
            .start()
        
        # 7. Stream 3: Agrégations quotidiennes
        print("📊 Démarrage du stream HDFS (aggregations)...")
        daily_aggregations = enriched_df \
            .groupBy(
                window(col("timestamp"), "1 day"),
                col("page"),
                col("action"),
                col("product_category"),
                col("location"),
                col("device_type")
            ) \
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("user_id").alias("unique_users"),
                avg("duration_seconds").alias("avg_duration"),
                sum(col("purchase_amount")).alias("total_revenue"),
                count(when(col("action") == "purchase", 1)).alias("purchase_count")
            ) \
            .withColumn("date", col("window.start")) \
            .writeStream \
            .format("parquet") \
            .option("path", f"{HDFS_PATH}/aggregations/daily") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily_agg") \
            .outputMode("append") \
            .trigger(processingTime="15 minutes") \
            .start()
        
        # 8. Stream 4: Analyse des sessions
        print("👥 Démarrage du stream HDFS (sessions)...")
        sessions_df = process_session_analytics(enriched_df)
        
        sessions_stream = sessions_df \
            .withColumn("date", to_date(col("session_start"))) \
            .writeStream \
            .format("parquet") \
            .option("path", f"{HDFS_PATH}/sessions") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/sessions") \
            .partitionBy("date") \
            .outputMode("append") \
            .trigger(processingTime="10 minutes") \
            .start()
        
        # Afficher l'état des streams
        streams = [redis_stream, hdfs_raw_stream, daily_aggregations, sessions_stream]
        stream_names = ["Redis Metrics", "HDFS Raw", "Daily Aggregations", "Sessions"]
        
        print("\n" + "=" * 60)
        print("📈 STREAMS ACTIFS:")
        for name, stream in zip(stream_names, streams):
            status = "✅ ACTIF" if stream.isActive else "❌ INACTIF"
            print(f"  {name}: {status}")
        print("=" * 60 + "\n")
        
        print("🎯 Streaming en cours... Appuyez sur Ctrl+C pour arrêter\n")
        
        # Attendre la terminaison
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt demandé par l'utilisateur...")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Arrêter tous les streams
        print("\n" + "=" * 60)
        print("🛑 Arrêt des streams...")
        for stream in spark.streams.active:
            print(f"  Arrêt de {stream.name}...")
            stream.stop()
        
        spark.stop()
        print("\n✅ Session Spark arrêtée proprement")
        print("=" * 60)

if __name__ == "__main__":
    main()