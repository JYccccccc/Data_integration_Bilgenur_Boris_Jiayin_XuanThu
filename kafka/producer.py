from confluent_kafka import Producer
import time
import json
import pandas as pd

# configure de kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'acidified-waters-producer',
    'group.id': 'acidified-waters-group'
}

# Creer Kafka Producer
producer = Producer(conf)
topic_name = 'acidified-water-topic'

# Lire le fichier
data_file = '/mnt/d/Code/exeserce/data_integration/data_integration_projet_final/database/LTM_Data_2022_8_1.xlsx'
df = pd.read_excel(data_file)

# Rapport de livraison
def delivery_report(err, msg):
    """Verify message delivery or raise an exception."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Lire le fichier et envoyer les données au topic
def produce_data():
    """Definit la taille de batch et envoi les données au topic"""
    batch_size = 10 # Taille du batch 10

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size].to_dict(orient='records')
        for record in batch:
        # Envoi des données au topic
            producer.produce(topic_name, value=str(record), callback=delivery_report)
        
        producer.flush()  # Attendre que tous les messages soient envoyés
        time.sleep(10) # Attendre 10 secondes avant d'envoyer le prochain batch
if __name__ == "__main__":
    produce_data()
