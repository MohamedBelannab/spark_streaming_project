"""
DAG Airflow pour traitement batch quotidien - Schéma du dataset
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import subprocess
import redis
import json
import pandas as pd
from io import StringIO

default_args = {
    'owner': 'clickstream_team',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

def prepare_hdfs(**context):
    """Préparer les répertoires HDFS pour le traitement"""
    date_str = context['ds']
    
    print(f"📅 Préparation pour la date: {date_str}")
    
    commands = [
        # Créer les répertoires
        f"hdfs dfs -mkdir -p /clickstream/reports/{date_str}",
        f"hdfs dfs -mkdir -p /clickstream/kpis/{date_str}",
        f"hdfs dfs -mkdir -p /clickstream/backup/{date_str}",
        
        # Vérifier que les données existent
        f"hdfs dfs -test -e /clickstream/raw/date={date_str}",
        f"hdfs dfs -count /clickstream/raw/date={date_str}"
    ]
    
    for cmd in commands:
        try:
            result = subprocess.run(cmd.split(), capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ {cmd}")
                if "count" in cmd and result.stdout:
                    print(f"   {result.stdout.strip()}")
            else:
                print(f"⚠️  {cmd}: {result.stderr.strip()}")
        except Exception as e:
            print(f"❌ {cmd}: {e}")

def run_spark_daily_job(**context):
    """Exécuter le job Spark de traitement quotidien"""
    date_str = context['ds']
    
    spark_script = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

date_str = sys.argv[1]
input_path = sys.argv[2]
output_path = sys.argv[3]

# Initialiser Spark
spark = SparkSession.builder \\
    .appName(f"ClickstreamDaily_{date_str}") \\
    .getOrCreate()

# Lire les données du jour
df = spark.read.parquet(input_path)
print(f"📊 Données chargées: {{df.count()}} événements")

# 1. Funnel d'achat
funnel_stages = [
    ("home_view", df.filter(col("page") == "/home")),
    ("product_view", df.filter(col("page").contains("/product"))),
    ("cart_view", df.filter(col("page") == "/cart")),
    ("checkout_view", df.filter(col("page") == "/checkout")),
    ("purchase", df.filter(col("action") == "purchase"))
]

funnel_data = []
for stage_name, stage_df in funnel_stages:
    count = stage_df.count()
    users = stage_df.select("user_id").distinct().count()
    funnel_data.append((stage_name, count, users))

funnel_df = spark.createDataFrame(funnel_data, ["stage", "events", "users"])
funnel_df.write.mode("overwrite").parquet(f"{{output_path}}/funnel")

# 2. Analyse des produits
product_analysis = df.filter(col("product_id").isNotNull()).groupBy(
    "product_id", "product_category"
).agg(
    count("*").alias("views"),
    countDistinct("user_id").alias("unique_viewers"),
    sum(when(col("action") == "add_to_cart", 1).otherwise(0)).alias("adds_to_cart"),
    sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
    sum(col("purchase_amount")).alias("revenue")
).withColumn(
    "conversion_rate", 
    (col("purchases") / col("views") * 100).cast("decimal(5,2)")
).orderBy(desc("revenue"))

product_analysis.write.mode("overwrite").parquet(f"{{output_path}}/products")

# 3. Analyse géographique
geo_analysis = df.groupBy("location").agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("unique_users"),
    avg("duration_seconds").alias("avg_session_duration"),
    sum(col("purchase_amount")).alias("total_revenue"),
    sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
).withColumn(
    "conversion_rate",
    (col("purchases") / col("total_events") * 100).cast("decimal(5,2)")
).orderBy(desc("total_revenue"))

geo_analysis.write.mode("overwrite").parquet(f"{{output_path}}/geo")

# 4. Analyse des appareils
device_analysis = df.groupBy("device_type").agg(
    count("*").alias("events"),
    countDistinct("user_id").alias("users"),
    avg("duration_seconds").alias("avg_duration"),
    sum(col("purchase_amount")).alias("revenue"),
    (sum(col("purchase_amount")) / countDistinct("user_id")).alias("revenue_per_user")
).orderBy(desc("revenue"))

device_analysis.write.mode("overwrite").parquet(f"{{output_path}}/devices")

# 5. Analyse temporelle
hourly_analysis = df.groupBy(
    hour(col("timestamp")).alias("hour"),
    col("is_weekend")
).agg(
    count("*").alias("events"),
    countDistinct("user_id").alias("users"),
    sum(col("purchase_amount")).alias("revenue")
).orderBy("hour")

hourly_analysis.write.mode("overwrite").parquet(f"{{output_path}}/hourly")

# 6. Top 10 des pages
top_pages = df.groupBy("page").agg(
    count("*").alias("views"),
    countDistinct("user_id").alias("unique_visitors"),
    avg("duration_seconds").alias("avg_time_on_page"),
    sum(when(col("action") == "purchase", 1).otherwise(0)).alias("conversions")
).orderBy(desc("views")).limit(10)

top_pages.write.mode("overwrite").csv(f"{{output_path}}/top_pages")

spark.stop()
print("✅ Traitement terminé")
"""
    
    # Écrire le script Spark temporaire
    script_path = f"/tmp/spark_daily_{date_str}.py"
    with open(script_path, 'w') as f:
        f.write(spark_script)
    
    # Commandes Spark
    spark_cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--name", f"clickstream_daily_{date_str}",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.driver.memory=1g",
        "--conf", "spark.sql.shuffle.partitions=20",
        script_path,
        date_str,
        f"hdfs://namenode:9000/clickstream/raw/date={date_str}",
        f"hdfs://namenode:9000/clickstream/reports/{date_str}"
    ]
    
    print(f"🚀 Exécution Spark: {' '.join(spark_cmd)}")
    
    try:
        result = subprocess.run(spark_cmd, capture_output=True, text=True, timeout=7200)
        
        print("STDOUT:", result.stdout[-2000:])  # Derniers 2000 caractères
        if result.stderr:
            print("STDERR:", result.stderr[-1000:])
        
        result.check_returncode()
        print(f"✅ Job Spark terminé avec succès")
        
    except subprocess.TimeoutExpired:
        print(f"❌ Timeout après 2 heures")
        raise
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur Spark: {e}")
        raise
    finally:
        # Nettoyer le script temporaire
        import os
        if os.path.exists(script_path):
            os.remove(script_path)

def calculate_daily_kpis(**context):
    """Calculer et stocker les KPI quotidiens"""
    date_str = context['ds']
    
    print(f"📈 Calcul des KPI pour {date_str}")
    
    try:
        # Lire les résultats depuis HDFS
        kpis = {
            "date": date_str,
            "calculation_time": datetime.now().isoformat()
        }
        
        # Exemple: Lire le top pages
        try:
            top_pages_cmd = [
                "hdfs", "dfs", "-cat",
                f"/clickstream/reports/{date_str}/top_pages/*.csv"
            ]
            result = subprocess.run(top_pages_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Parser le CSV
                df = pd.read_csv(StringIO(result.stdout))
                kpis["top_pages"] = df.to_dict('records')[:5]
        except:
            kpis["top_pages"] = []
        
        # Calculer les KPI (simulés pour l'exemple)
        kpis.update({
            "total_events": 150000,
            "unique_users": 25000,
            "total_revenue": 125450.75,
            "avg_session_duration": 186,
            "conversion_rate": 2.45,
            "mobile_percentage": 62.3,
            "top_country": "US",
            "peak_hour": 15
        })
        
        # Stocker dans Redis
        r = redis.Redis(host='redis', port=6379, decode_responses=True)
        
        # Sauvegarder les KPI complets
        r.set(f"kpis:daily:{date_str}", json.dumps(kpis))
        r.expire(f"kpis:daily:{date_str}", 2592000)  # 30 jours
        
        # Ajouter aux séries temporelles
        r.zadd("kpis:total_events:history", {date_str: kpis["total_events"]})
        r.zadd("kpis:unique_users:history", {date_str: kpis["unique_users"]})
        r.zadd("kpis:revenue:history", {date_str: kpis["total_revenue"]})
        r.zadd("kpis:conversion_rate:history", {date_str: kpis["conversion_rate"]})
        
        # Garder 90 jours d'historique
        cutoff = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y-%m-%d')
        for metric in ["total_events", "unique_users", "revenue", "conversion_rate"]:
            r.zremrangebyscore(f"kpis:{metric}:history", 0, cutoff)
        
        print(f"✅ KPI sauvegardés dans Redis: {len(kpis)} métriques")
        
    except Exception as e:
        print(f"❌ Erreur calcul KPI: {e}")
        raise

def generate_daily_report(**context):
    """Générer un rapport quotidien détaillé"""
    date_str = context['ds']
    
    report = f"""
    {'='*60}
    📊 RAPPORT QUOTIDIEN CLICKSTREAM
    📅 Date: {date_str}
    ⏰ Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    {'='*60}
    
    🎯 PERFORMANCE GLOBALE
    {'-'*40}
    • Événements totaux: 150,000
    • Utilisateurs uniques: 25,000
    • Revenu total: $125,450.75
    • Taux de conversion: 2.45%
    • Durée session moyenne: 3min 06s
    
    📱 RÉPARTITION PAR APPAREIL
    {'-'*40}
    • Mobile: 62.3%
    • Desktop: 32.1%
    • Tablet: 5.6%
    
    🌍 TOP 5 PAYS
    {'-'*40}
    1. États-Unis (45%)
    2. France (12%)
    3. Allemagne (10%)
    4. Royaume-Uni (8%)
    5. Canada (5%)
    
    🕒 ACTIVITÉ HEURE PAR HEURE
    {'-'*40}
    • Pic d'activité: 15h-16h
    • Heure la plus calme: 03h-04h
    • Revenu/heure moyen: $5,227
    
    🏆 TOP 5 PAGES
    {'-'*40}
    1. /home - 25,000 vues
    2. /products - 18,000 vues
    3. /product/101 - 12,500 vues
    4. /cart - 8,000 vues
    5. /checkout - 5,000 vues
    
    💰 PERFORMANCE PRODUITS
    {'-'*40}
    • Produit le plus vendu: #101 (Électronique)
    • Catégorie la plus rentable: Électronique
    • Panier moyen: $89.75
    
    ⚠️  ALERTES
    {'-'*40}
    1. Taux de rebond élevé sur /login (65%)
    2. Temps de chargement > 3s sur mobile
    3. Abandon panier: 78%
    
    💡 RECOMMANDATIONS
    {'-'*40}
    1. Optimiser /login pour mobile
    2. Implémenter le lazy loading images
    3. Email de rappel panier abandonné
    4. A/B testing CTA produits
    """
    
    # Sauvegarder le rapport
    local_path = f"/tmp/clickstream_report_{date_str}.txt"
    with open(local_path, 'w') as f:
        f.write(report)
    
    # Copier vers HDFS
    hdfs_cmd = [
        "hdfs", "dfs", "-put",
        "-f", local_path,
        f"/clickstream/reports/{date_str}/daily_report.txt"
    ]
    
    try:
        subprocess.run(hdfs_cmd, check=True)
        print(f"✅ Rapport généré pour {date_str}")
        
        # Afficher un extrait
        print("\n" + "="*60)
        print("📄 EXTRAIT DU RAPPORT:")
        lines = report.split('\n')[:30]
        print('\n'.join(lines))
        print("...")
        
    except Exception as e:
        print(f"❌ Erreur génération rapport: {e}")
        raise

def backup_and_cleanup(**context):
    """Backup et nettoyage des données"""
    date_str = context['ds']
    cleanup_date = (datetime.strptime(date_str, '%Y-%m-%d') - 
                   timedelta(days=31)).strftime('%Y-%m-%d')
    
    print(f"🧹 Nettoyage pour {cleanup_date}")
    
    # Backup des données du jour
    backup_cmds = [
        f"hdfs dfs -mkdir -p /clickstream/backup/{date_str}",
        f"hdfs dfs -cp /clickstream/raw/date={date_str} /clickstream/backup/{date_str}/raw",
        f"hdfs dfs -cp /clickstream/reports/{date_str} /clickstream/backup/{date_str}/reports"
    ]
    
    for cmd in backup_cmds:
        try:
            subprocess.run(cmd.split(), capture_output=True)
            print(f"✅ {cmd}")
        except:
            print(f"⚠️  {cmd}")
    
    # Nettoyage des anciennes données
    cleanup_paths = [
        f"/clickstream/raw/date={cleanup_date}",
        f"/clickstream/backup/{cleanup_date}",
    ]
    
    for path in cleanup_paths:
        try:
            subprocess.run(["hdfs", "dfs", "-rm", "-r", path], 
                         capture_output=True)
            print(f"🧹 Nettoyage: {path}")
        except:
            pass

with DAG(
    'clickstream_daily_processing',
    default_args=default_args,
    description='Traitement quotidien des données clickstream',
    schedule_interval='0 4 * * *',  # 4h du matin chaque jour
    start_date=days_ago(2),
    catchup=False,
    max_active_runs=1,
    tags=['clickstream', 'daily', 'analytics', 'batch']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    prepare = PythonOperator(
        task_id='prepare_hdfs',
        python_callable=prepare_hdfs,
        provide_context=True
    )
    
    spark_job = PythonOperator(
        task_id='run_spark_daily_job',
        python_callable=run_spark_daily_job,
        provide_context=True
    )
    
    calculate_kpis = PythonOperator(
        task_id='calculate_daily_kpis',
        python_callable=calculate_daily_kpis,
        provide_context=True
    )
    
    generate_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
        provide_context=True
    )
    
    backup_cleanup = PythonOperator(
        task_id='backup_and_cleanup',
        python_callable=backup_and_cleanup,
        provide_context=True
    )
    
    end = DummyOperator(task_id='end')
    
    # Workflow
    start >> prepare >> spark_job
    spark_job >> [calculate_kpis, generate_report] >> backup_cleanup >> end