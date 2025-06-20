import pandas as pd
from confluent_kafka import Producer
import json
import time

conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OHXSQCLECYXHUMZ7',
    'sasl.password': 'tcyhnpeINm1OVQRm7mA64Na4933wSEkKfBg3aA/XlCilj0uTEMyZS1ER3xL5L9b8'
}

producer = Producer(conf)

# Read the CSV file
df = pd.read_csv('support_tickets.csv')

# Send each row as a JSON message to Kafka
topic = 'incoming_support'

while(True):
    for index, row in df.iterrows():
        msg = {
            'ticket_id': row['ticket_id'],
            'message': row['message']
        }
        producer.produce(topic, value=json.dumps(msg))
        time.sleep(2)
        print(f"✅ Pushed: {msg}")

producer.flush()
print("🚀 All messages pushed to Kafka topic.")
