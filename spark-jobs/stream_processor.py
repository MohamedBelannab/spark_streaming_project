# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Spark Structured Streaming pour clickstream - Python 3.5 compatible
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import redis
import json
from datetime import datetime
import time

# Configuration
KAFKA_BROKER = "kafka:29092"
TOPIC_NAME = "clickstream"
REDIS_HOST = "redis"
REDIS_PORT = 6379
HDFS_PATH = "hdfs://namenode:9000/clickstream"
CHECKPOINT_PATH = "/tmp/spark-checkpoints"

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
        .getOrCreate()

def get_clickstream_schema():
    """Définir le schéma selon le dataset"""
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),  # String first, convert later
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
    """Écrire les métriques temps réel dans Redis - Python 3.5 compatible"""
    try:
        r = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            decode_responses=True, 
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        
        batch_time = datetime.now().strftime("%Y%m%d%H%M%S")

        # Collecter toutes les donnees UNE SEULE FOIS pour eviter les erreurs
        all_rows = batch_df.collect()

        # 1. Statistiques par page (aggreger les donnees deja agregees par fenetre)
        page_data = {}
        for row in all_rows:
            page = row['page']
            if page not in page_data:
                page_data[page] = {
                    'views': 0,
                    'unique_users': 0,
                    'avg_duration': [],
                    'purchases': 0,
                    'revenue': 0
                }
            page_data[page]['views'] += int(row['event_count'] or 0)
            page_data[page]['unique_users'] += int(row['unique_users'] or 0)
            page_data[page]['avg_duration'].append(float(row['avg_duration'] or 0))
            page_data[page]['revenue'] += float(row['revenue'] or 0)

        page_stats = []
        for page, stats in page_data.items():
            avg_dur = sum(stats['avg_duration']) / len(stats['avg_duration']) if stats['avg_duration'] else 0
            page_stats.append({
                'page': page,
                'views': stats['views'],
                'unique_users': stats['unique_users'],
                'avg_duration': avg_dur,
                'purchases': 0,  # Non disponible dans les donnees agregees
                'revenue': stats['revenue']
            })
        
        for row in page_stats:
            page_key = "page:stats:{}".format(row['page'])
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

        # 3. Statistiques globales (utiliser unique_users agrege au lieu de user_id)
        total_unique_users = sum([int(row['unique_users'] or 0) for row in page_stats])
        r.set("global:unique_users:current", total_unique_users)
        r.expire("global:unique_users:current", 300)  # 5 minutes
        
        # 4. Statistiques par action (depuis les donnees deja agregees)
        action_stats = {}
        for row in all_rows:
            action = row['action']
            if action not in action_stats:
                action_stats[action] = 0
            action_stats[action] += int(row['event_count'] or 0)

        for action, count in action_stats.items():
            action_key = "action:stats:{}".format(action)
            r.hincrby(action_key, "count", count)
            r.expire(action_key, 3600)
        
        # 5. Derniers événements (pour monitoring) - utiliser colonnes disponibles
        try:
            recent_events = all_rows[:10]  # Utiliser les données déjà collectées
        except:
            recent_events = []
        
        for event in recent_events:
            event_data = {
                "page": event['page'],
                "action": event['action'],
                "window": str(event['window']),
                "event_count": int(event['event_count'] or 0),
                "revenue": float(event['revenue'] or 0)
            }
            r.lpush("recent:events", json.dumps(event_data))
        
        r.ltrim("recent:events", 0, 99)  # Garder 100 derniers
        
        # 6. Métriques globales
        total_events = len(all_rows)
        total_revenue = sum([float(row['revenue'] or 0) for row in all_rows])
        
        r.incrby("global:events:total", total_events)
        r.incrbyfloat("global:revenue:total", float(total_revenue))
        r.set("global:last_batch_time", batch_time)
        r.set("global:last_batch_id", batch_id)
        
        # 7. Statistiques géographiques (depuis les donnees deja agregees)
        geo_stats = {}
        for row in all_rows:
            location = row['location']
            if location:
                if location not in geo_stats:
                    geo_stats[location] = 0
                geo_stats[location] += int(row['event_count'] or 0)

        for location, events in geo_stats.items():
            r.zincrby("geo:activity", events, location)

        # 8. Statistiques appareils (depuis les donnees deja agregees)
        device_stats = {}
        for row in all_rows:
            device = row['device_type']
            if device:
                if device not in device_stats:
                    device_stats[device] = 0
                device_stats[device] += int(row['event_count'] or 0)

        for device, events in device_stats.items():
            r.zincrby("device:usage", events, device)
        
        print("✅ Batch {} -> Redis: {} events, ${:.2f} revenue".format(batch_id, total_events, total_revenue))

    except Exception as e:
        print("❌ Erreur Redis batch {}: {}".format(batch_id, str(e)[:100]))

def process_real_time_metrics(df):
    """Traiter les métriques en temps réel"""
    # Métriques en temps réel - utiliser approx_count_distinct pour les streams
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
    # Identifier les sessions (30 minutes d'inactivité)
    window_spec = Window.partitionBy("user_id", "session_id").orderBy("timestamp")
    
    sessions = df \
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
    print("📡 Kafka: {}".format(KAFKA_BROKER))
    print("📝 Topic: {}".format(TOPIC_NAME))
    print("🗄️  HDFS: {}".format(HDFS_PATH))
    print("⚡ Redis: {}:{}".format(REDIS_HOST, REDIS_PORT))
    print("=" * 60)
    
    # Initialiser Spark
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        # 1. Lire depuis Kafka
        print("📡 Lecture du stream Kafka...")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # 2. Parser le JSON avec le bon schéma
        schema = get_clickstream_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # 3. Convertir timestamp string to timestamp
        parsed_df = parsed_df.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        
        # 4. Ajouter des colonnes de temps
        enriched_df = parsed_df \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0)) \
            .withWatermark("timestamp", "10 minutes")
        
        # 5. Traiter les métriques en temps réel
        print("📊 Traitement des métriques en temps réel...")
        realtime_metrics = process_real_time_metrics(enriched_df)
        
        # 6. Stream 1: Écrire les métriques temps réel dans Redis
        print("⚡ Démarrage du stream Redis...")
        redis_stream = realtime_metrics \
            .select("window", "page", "action", "location", "device_type",
                    "event_count", "unique_users", "revenue",
                    "avg_duration", "revenue_per_user", "conversion_rate") \
            .writeStream \
            .foreachBatch(write_to_redis) \
            .outputMode("update") \
            .trigger(processingTime="1 minute") \
            .option("checkpointLocation", "{}/redis".format(CHECKPOINT_PATH)) \
            .start()

        # 7. Stream 2: Écrire les données brutes dans HDFS
        print("🗄️  Démarrage du stream HDFS (raw data)...")
        hdfs_raw_stream = enriched_df \
            .writeStream \
            .format("parquet") \
            .option("path", "{}/raw".format(HDFS_PATH)) \
            .option("checkpointLocation", "{}/hdfs_raw".format(CHECKPOINT_PATH)) \
            .partitionBy("date", "hour") \
            .outputMode("append") \
            .trigger(processingTime="5 minutes") \
            .start()

        # 8. Stream 3: Agrégations horaires (append mode)
        print("📊 Démarrage du stream HDFS (aggregations)...")
        daily_aggregations = enriched_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "1 hour"),
                col("date"),
                col("page"),
                col("action"),
                col("device_type")
            ) \
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("user_id").alias("unique_users"),
                avg("duration_seconds").alias("avg_duration")
            ) \
            .writeStream \
            .format("parquet") \
            .option("path", "{}/aggregations/hourly".format(HDFS_PATH)) \
            .option("checkpointLocation", "{}/hourly_agg".format(CHECKPOINT_PATH)) \
            .outputMode("append") \
            .trigger(processingTime="10 minutes") \
            .start()
        
        # Afficher l'état des streams
        streams = [redis_stream, hdfs_raw_stream, daily_aggregations]
        stream_names = ["Redis Metrics", "HDFS Raw", "Daily Aggregations"]
        
        print("\n" + "=" * 60)
        print("✅ STREAMS ACTIFS:")
        for name, stream in zip(stream_names, streams):
            status = "🟢 ACTIF" if stream.isActive else "🔴 INACTIF"
            print("  • {}: {}".format(name, status))
        print("=" * 60)
        
        # Attendre la terminaison
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur...")
    except Exception as e:
        print("\n❌ Erreur: {}".format(e))
        import traceback
        traceback.print_exc()
    finally:
        # Arrêter tous les streams
        for stream in spark.streams.active:
            print("🛑 Arrêt de {}...".format(stream.name))
            stream.stop()

        spark.stop()
        print("\n✅ Session Spark arrêtée")

if __name__ == "__main__":
    main()

"""
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  /opt/spark-apps/stream_processor.py

"""