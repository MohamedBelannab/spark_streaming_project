# import json
# import time
# import pandas as pd
# from kafka import KafkaProducer

# # Configuration
# KAFKA_TOPIC = 'clickstream'
# KAFKA_SERVER = 'localhost:9092'

# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_SERVER],
#     value_serializer=lambda x: json.dumps(x).encode('utf-8')
# )

# def run_producer():
#     print(f"📡 Producer started. Streaming to Kafka...")
#     try:
#         df = pd.read_csv('data/events.csv')
#         for _, row in df.iterrows():
#             message = row.to_dict()
#             producer.send(KAFKA_TOPIC, value=message)
#             print(f"✅ Sent: {message}")
#             time.sleep(1) # Send one event per second
#     except Exception as e:
#         print(f"❌ Error: {e}")

# if __name__ == "__main__":
#     run_producer()



#!/usr/bin/env python3
"""
Producer Kafka pour événements clickstream - Schéma du dataset
"""

import json
import time
import random
import csv
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'clickstream'
fake = Faker()

# Données de référence basées sur le schéma
PAGES = ['/', '/home', '/products', '/product', '/cart', '/checkout', 
         '/profile', '/login', '/logout', '/search', '/category', '/deals']
ACTIONS = ['view', 'click', 'add_to_cart', 'remove_from_cart', 'purchase',
           'scroll', 'search', 'filter', 'sort', 'login', 'logout']
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 
                      'Sports', 'Beauty', 'Toys', 'Food', 'Automotive', None]
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
LOCATIONS = ['US', 'FR', 'DE', 'UK', 'CA', 'AU', 'JP', 'BR', 'IN', 'CN']

# Charger les product_ids existants si le CSV existe
def load_existing_products():
    """Charger les product_ids existants du CSV"""
    products = []
    try:
        with open('../data/clickstream_events.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['product_id'] and row['product_id'] != '':
                    products.append(int(row['product_id']))
    except FileNotFoundError:
        pass
    
    # Ajouter des IDs par défaut si le fichier n'existe pas
    if not products:
        products = list(range(1001, 1101))
    
    return list(set(products))

# Charger les product_ids
existing_products = load_existing_products()

def create_clickstream_event():
    """Crée un événement clickstream selon le schéma du dataset"""
    user_id = random.randint(1, 1000)
    session_id = fake.uuid4()
    current_time = datetime.now() - timedelta(days=random.randint(0, 7))
    
    # Déterminer si c'est un achat
    is_purchase = random.random() < 0.05  # 5% de chance d'achat
    action = 'purchase' if is_purchase else random.choice(ACTIONS)
    
    # Sélectionner une page en fonction de l'action
    if action == 'purchase':
        page = '/checkout'
        purchase_amount = round(random.uniform(10, 500), 2)
    elif action == 'add_to_cart':
        page = '/cart'
        purchase_amount = None
    elif action in ['view', 'click']:
        page = random.choice(['/product', '/products', '/category'])
        purchase_amount = None
    else:
        page = random.choice(PAGES)
        purchase_amount = None
    
    # Sélectionner un produit pour les pages produit
    product_id = None
    product_category = None
    if page in ['/product', '/products', '/category'] and existing_products:
        product_id = random.choice(existing_products)
        product_category = random.choice(PRODUCT_CATEGORIES)
    
    event = {
        "user_id": user_id,
        "timestamp": current_time.isoformat(),
        "page": page,
        "action": action,
        "product_id": product_id,
        "product_category": product_category,
        "session_id": session_id,
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
        "duration_seconds": random.randint(1, 300),
        "referrer": random.choice(['google.com', 'facebook.com', 'direct', 
                                 'twitter.com', 'newsletter', 'affiliate', '']),
        "location": random.choice(LOCATIONS),
        "device_type": random.choice(DEVICE_TYPES),
        "purchase_amount": purchase_amount
    }
    
    return event

def read_from_csv_and_send(producer, csv_path='../data/clickstream_events.csv'):
    """Lire les données existantes du CSV et les envoyer à Kafka"""
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            count = 0
            
            for row in reader:
                # Convertir les types
                event = {
                    "user_id": int(row['user_id']),
                    "timestamp": row['timestamp'],
                    "page": row['page'],
                    "action": row['action'],
                    "product_id": int(row['product_id']) if row['product_id'] else None,
                    "product_category": row['product_category'] if row['product_category'] else None,
                    "session_id": row['session_id'],
                    "ip_address": row['ip_address'],
                    "user_agent": row['user_agent'],
                    "duration_seconds": int(row['duration_seconds']) if row['duration_seconds'] else 0,
                    "referrer": row['referrer'],
                    "location": row['location'],
                    "device_type": row['device_type'],
                    "purchase_amount": float(row['purchase_amount']) if row['purchase_amount'] else None
                }
                
                # Envoyer à Kafka
                producer.send(
                    topic=TOPIC_NAME,
                    key=str(event['user_id']),
                    value=event
                )
                
                count += 1
                if count % 1000 == 0:
                    print(f"📥 {count} lignes envoyées depuis CSV...")
                
                # Pause pour simuler le temps réel
                time.sleep(random.uniform(0.01, 0.05))
            
            print(f"✅ Total envoyé depuis CSV: {count} lignes")
            return count
            
    except Exception as e:
        print(f"❌ Erreur lecture CSV: {e}")
        return 0

def main():
    print("🚀 Producer Kafka - Clickstream Events")
    print(f"📡 Broker: {KAFKA_BROKER}")
    print(f"📝 Topic: {TOPIC_NAME}")
    print("=" * 50)
    
    try:
        # Configuration du producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            linger_ms=10,
            batch_size=32768,
            compression_type='snappy'
        )
        
        print("✅ Connecté à Kafka")
        
        # Option: Lire depuis CSV ou générer aléatoirement
        use_csv = input("📂 Utiliser le fichier CSV existant? (o/n): ").lower() == 'o'
        
        count = 0
        start_time = time.time()
        
        if use_csv:
            print("📖 Lecture depuis clickstream_events.csv...")
            count = read_from_csv_and_send(producer)
        else:
            print("🎲 Génération d'événements aléatoires...")
            print("🎯 Envoi en cours (Ctrl+C pour arrêter)")
            print("-" * 50)
            
            try:
                while True:
                    event = create_clickstream_event()
                    user_id = event['user_id']
                    
                    producer.send(
                        topic=TOPIC_NAME,
                        key=user_id,
                        value=event
                    )
                    
                    count += 1
                    
                    if count % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = count / elapsed if elapsed > 0 else 0
                        
                        print(f"📊 Événements: {count:,} | "
                              f"Taux: {rate:.1f}/sec | "
                              f"User: {user_id} | "
                              f"Page: {event['page']} | "
                              f"Action: {event['action']}")
                    
                    time.sleep(random.uniform(0.05, 0.2))
                    
            except KeyboardInterrupt:
                print("\n\n🛑 Arrêt demandé...")
        
        # Finaliser
        producer.flush()
        producer.close()
        
        elapsed = time.time() - start_time
        print("\n" + "=" * 50)
        print("📈 RÉSUMÉ:")
        print(f"   Total événements: {count:,}")
        print(f"   Durée: {elapsed:.1f} secondes")
        if elapsed > 0:
            print(f"   Taux moyen: {count/elapsed:.1f} événements/sec")
        print("\n✅ Producer arrêté proprement")
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()