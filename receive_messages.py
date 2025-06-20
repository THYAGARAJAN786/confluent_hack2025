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

# Initialize intent classifier with small model
classifier = pipeline("text-classification", model="bhadresh-savani/distilbert-base-uncased-emotion")  # small & usable on CPU

def classify_message(text):
    result = classifier(text)[0]
    label = result['label'].lower()

    # Map labels to our routing categories
    if "anger" in label or "disgust" in label:
        return "alert", "Forwarded to moderation team."
    elif "sadness" in label or "fear" in label:
        return "escalation", "Escalated to a human support agent."
    else:
        return "refund", "Refund or account-related issue handled automatically."

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
producer = Producer(conf)
consumer.subscribe(['incoming_support'])

def route(ticket):
    intent, reply = classify_message(ticket['message'])
    ticket['intent'] = intent
    ticket['reply'] = reply

    # Route to topic based on intent
    topic = {
        "refund": "auto_resolve",
        "alert": "alerts",
        "escalation": "escalated"
    }
    topic = topic[intent]

    producer.produce(topic, value=json.dumps(ticket).encode('utf-8'))
    print(f"✅ Routed to {topic}: {ticket}")

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
        route(ticket)

except KeyboardInterrupt:
    print("Stopping...")
finally:
    consumer.close()
    producer.flush()


