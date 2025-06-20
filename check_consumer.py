from confluent_kafka import Consumer, Producer
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import json
import time
import json
import requests
import json
import torch
from transformers import pipeline
from confluent_kafka import Consumer, Producer

# Kafka setup
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OHXSQCLECYXHUMZ7',
    'sasl.password': 'tcyhnpeINm1OVQRm7mA64Na4933wSEkKfBg3aA/XlCilj0uTEMyZS1ER3xL5L9b8',
    'group.id': 'llm-router',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['auto_resolve', 'alerts', 'escalated'])

# Main loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        ticket = json.loads(msg.value().decode('utf-8'))
        print(ticket)

except KeyboardInterrupt:
    print("Stopping...")
finally:
    consumer.close()

