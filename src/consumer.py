import json
from kafka import KafkaConsumer

def start_consumer():
    print("Starting Kafka Consumer...")
    print("Listening for messages on topic: 'ingestion-raw'\n")
    print("-" * 50)
    consumer = KafkaConsumer(
        'ingestion-raw',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest', 
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        chunk = message.value
        print(f"Received from {chunk['source'].upper()}")
        print(f"Chunk ID: {chunk['chunk_id']}")
        print(f"Metadata: {json.dumps(chunk['metadata'])}")
        print(f"Text Snippet: {chunk['text'][:100]}...")
        print("-" * 50)

if __name__ == "__main__":
    start_consumer()
