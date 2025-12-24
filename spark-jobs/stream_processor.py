#!/usr/bin/env python3
"""
Spark Structured Streaming pour clickstream - Schéma du dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import redis
import json
from datetime import datetime
import os

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
            countDistinct("user_id").alias("unique_users"),
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
        r.zremrangebyscore("active:users:history", 0, int(time.time()) - 3600)  # Garder 1h
        
        # 4. Statistiques par action
        action_stats = batch_df.groupBy("action").agg(
            count("*").alias("count")
        ).collect()
        
        for row in action_stats:
            action_key = f"action:stats:{row['action']}"
            r.hincrby(action_key, "count", int(row['count'] or 0))
            r.expire(action_key, 3600)
        
        # 5. Derniers événements (pour monitoring)
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
        
        r.ltrim("recent:events", 0, 99)  # Garder 100 derniers
        
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
    # Fenêtre glissante de 5 minutes, mise à jour chaque minute
    window_spec = Window().orderBy("timestamp").rangeBetween(-300, 0)
    
    # Métriques en temps réel
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
            countDistinct("user_id").alias("unique_users"),
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
    print(f"📡 Kafka: {KAFKA_BROKER}")
    print(f"📝 Topic: {TOPIC_NAME}")
    print(f"💾 HDFS: {HDFS_PATH}")
    print(f"🔴 Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 60)
    
    # Initialiser Spark
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        # 1. Lire depuis Kafka
        print("📥 Lecture du stream Kafka...")
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
        print("🔥 Démarrage du stream Redis...")
        redis_stream = realtime_metrics \
            .select("window", "page", "action", "location", "device_type",
                    "event_count", "unique_users", "revenue", 
                    "avg_duration", "revenue_per_user", "conversion_rate") \
            .writeStream \
            .foreachBatch(write_to_redis) \
            .outputMode("update") \
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
                countDistinct("user_id").alias("unique_users"),
                avg("duration_seconds").alias("avg_duration"),
                sum(col("purchase_amount")).alias("total_revenue"),
                count(when(col("action") == "purchase", 1)).alias("purchase_count")
            ) \
            .withColumn("date", col("window.start")) \
            .writeStream \
            .format("parquet") \
            .option("path", f"{HDFS_PATH}/aggregations/daily") \
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily_agg") \
            .outputMode("complete") \
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
        print("=" * 60)
        
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
        for stream in spark.streams.active:
            print(f"Arrêt de {stream.name}...")
            stream.stop()
        
        spark.stop()
        print("\n✅ Session Spark arrêtée proprement")

if __name__ == "__main__":
    main()