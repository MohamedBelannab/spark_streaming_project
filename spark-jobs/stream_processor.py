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
CHECKPOINT_PATH = "hdfs://namenode:9000/clickstream/checkpoints"

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

def write_raw_to_redis(batch_df, batch_id):
    """Écrire les données brutes directement dans Redis - Sans agrégation"""
    try:
        row_count = batch_df.count()
        if row_count == 0:
            print("⚠️ Batch {} vide, ignoré".format(batch_id))
            return

        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5
        )

        batch_time = datetime.now().strftime("%Y%m%d%H%M%S")
        all_rows = batch_df.collect()

        total_events = len(all_rows)
        total_revenue = 0.0

        for row in all_rows:
            try:
                page = str(row['page']) if row['page'] else 'unknown'
                action = str(row['action']) if row['action'] else 'unknown'
                location = str(row['location']) if row['location'] else 'unknown'
                device = str(row['device_type']) if row['device_type'] else 'unknown'
                purchase = float(row['purchase_amount']) if row['purchase_amount'] else 0.0

                # Stats par page
                r.hincrby("page:{}".format(page), "views", 1)

                # Stats par action
                r.hincrby("action:{}".format(action), "count", 1)

                # Stats géo
                r.zincrby("geo:activity", 1, location)

                # Stats device
                r.zincrby("device:usage", 1, device)

                # Revenue si achat
                if action == "purchase" and purchase > 0:
                    r.hincrbyfloat("page:{}".format(page), "revenue", purchase)
                    total_revenue += purchase

            except Exception as row_error:
                continue

        # Métriques globales
        r.incrby("global:events:total", total_events)
        r.incrbyfloat("global:revenue:total", total_revenue)
        r.set("global:last_batch_time", batch_time)
        r.set("global:last_batch_id", str(batch_id))

        print("✅ Batch {} -> Redis: {} events, ${:.2f} revenue".format(batch_id, total_events, total_revenue))

    except Exception as e:
        print("❌ Erreur Redis batch {}: {}".format(batch_id, str(e)[:200]))

def process_real_time_metrics(df):
    """Traiter les métriques en temps réel"""
    # Métriques en temps réel - utiliser approx_count_distinct pour les streams
    # Watermark de 7 jours pour accepter les données avec timestamps décalés
    realtime_metrics = df \
        .withWatermark("timestamp", "7 days") \
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

    # Vérifier la connexion Redis avant de démarrer
    print("🔍 Vérification de la connexion Redis...")
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=10
        )
        r.ping()
        print("✅ Redis connecté et opérationnel")
    except Exception as e:
        print("❌ ERREUR: Redis non disponible - {}".format(str(e)))
        print("⚠️  Les métriques temps réel ne seront pas disponibles")
        print("💡 Vérifiez que le conteneur Redis est démarré")
        import sys
        sys.exit(1)

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
        
        # 3. Convertir timestamp string to timestamp (format ISO 8601 avec T et microsecondes)
        parsed_df = parsed_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )
        
        # 4. Ajouter des colonnes de temps
        enriched_df = parsed_df \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_weekend", when((col("day_of_week") == 1) | (col("day_of_week") == 7), 1).otherwise(0)) \
            .withWatermark("timestamp", "7 days")
        
        # 5. Traiter les métriques en temps réel
        print("📊 Traitement des métriques en temps réel...")
        realtime_metrics = process_real_time_metrics(enriched_df)
        
        # 6. Stream 1: Écrire les données brutes dans Redis (sans watermark pour avoir des données immédiates)
        print("⚡ Démarrage du stream Redis...")
        redis_stream = parsed_df \
            .writeStream \
            .foreachBatch(write_raw_to_redis) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .option("checkpointLocation", "{}/redis_raw".format(CHECKPOINT_PATH)) \
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
            .withWatermark("timestamp", "7 days") \
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
