import streamlit as st
import json
import pandas as pd
from confluent_kafka import Consumer
from datetime import datetime
import time

refresh_rate = 5  # seconds
last_refresh = time.time()

if time.time() - last_refresh > refresh_rate:
    st.experimental_rerun()

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

# Topics to read from
consumer = Consumer(conf)
consumer.subscribe(['auto_resolve', 'alerts', 'escalated'])

st.set_page_config(page_title="📊 LLM Support Ticket Dashboard", layout="wide")
st.title("📨 Real-Time Support Ticket Dashboard")

@st.cache_data(ttl=10)
def fetch_messages(n=50):
    records = []
    count = 0
    while count < n:
        print(count, n)
        msg = consumer.poll(10.0)
        print(msg)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            ticket = json.loads(msg.value().decode('utf-8'))
            print(ticket)
            ticket['kafka_topic'] = msg.topic()
            ticket['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            records.append(ticket)
            count += 1
        except:
            continue
    return pd.DataFrame(records)

# UI filters
st.sidebar.header("🔍 Filter Tickets")
intent_filter = st.sidebar.selectbox("Filter by Intent", ["All", "refund", "alert", "escalation"])
search_term = st.sidebar.text_input("Search in Message")

while True:
    # Fetch and filter data
    df = fetch_messages(5)

    if not df.empty:
        if intent_filter != "All":
            df = df[df["intent"] == intent_filter]
        if search_term:
            df = df[df["message"].str.contains(search_term, case=False)]

        # Reorder and rename
        df_display = df[["ticket_id", "message", "intent", "reply", "kafka_topic", "timestamp"]]
        df_display.columns = ["Ticket ID", "Message", "Intent", "Reply", "Kafka Topic", "Timestamp"]

        st.dataframe(df_display, use_container_width=True)

        with st.expander("📥 Raw Data", expanded=False):
            st.json(df.to_dict(orient="records"))
        time.sleep(5)
    else:
        st.info("Waiting for new messages from Kafka topics...")

