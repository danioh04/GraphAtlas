import json
import pandas as pd
from kafka import KafkaProducer
from unstructured.partition.md import partition_md
from unstructured.partition.text import partition_text
import hashlib
import glob
import os
import time

def generate_chunk_id(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def process_jira(filepath: str) -> list:
    df = pd.read_csv(filepath)
    standardized_chunks = []
    for _, row in df.iterrows():
        raw_text = f"Title: {row['Summary']}\nDescription: {row['Description']}"
        elements = partition_text(text=raw_text)
        clean_text = "\n".join([str(el) for el in elements])
        chunk = {
            "chunk_id": f"jira-{row['Issue Key']}",
            "source": "jira",
            "text": clean_text,
            "metadata": {
                "ticket_id": row['Issue Key'],
                "status": row['Status'],
                "assignee": row['Assignee'],
                "linked_issues": str(row['Linked Issues']) if pd.notna(row['Linked Issues']) else "None"
            }
        }
        standardized_chunks.append(chunk)
    print(f"Processed {len(standardized_chunks)} Jira tickets.")
    return standardized_chunks

def process_slack(filepath: str) -> list:
    with open(filepath, 'r') as f:
        slack_data = json.load(f)
    threads = {}
    for msg in slack_data:
        thread_id = msg.get('thread_ts', msg['ts'])
        if thread_id not in threads:
            threads[thread_id] = []
        threads[thread_id].append(msg)
    standardized_chunks = []
    for thread_id, messages in threads.items():
        thread_text = "\n".join([f"User {m['user']} said: {m['text']}" for m in messages])
        elements = partition_text(text=thread_text)
        clean_text = "\n".join([str(el) for el in elements])
        chunk = {
            "chunk_id": f"slack-{thread_id}",
            "source": "slack",
            "text": clean_text,
            "metadata": {
                "thread_id": thread_id,
                "participants": list(set([m['user'] for m in messages])),
                "message_count": len(messages)
            }
        }
        standardized_chunks.append(chunk)
    print(f"Processed {len(standardized_chunks)} Slack threads.")
    return standardized_chunks

def process_confluence(filepath: str) -> list:
    elements = partition_md(filename=filepath)
    clean_text = "\n".join([str(el) for el in elements])
    base_name = os.path.basename(filepath)
    page_id = base_name.replace('confluence_page_', '').replace('.md', '')
    chunk = {
        "chunk_id": f"confluence-{page_id}",
        "source": "confluence",
        "text": clean_text,
        "metadata": {
            "page_id": page_id,
            "document_type": "wiki"
        }
    }
    print(f"Processed Confluence page {page_id}.")
    return [chunk]

if __name__ == "__main__":
    print("Starting data ingestion and standardization...\n")
    all_standardized_data = []
    jira_chunks = process_jira("data/jira_export.csv")
    all_standardized_data.extend(jira_chunks)
    slack_chunks = process_slack("data/slack_export.json")
    all_standardized_data.extend(slack_chunks)
    confluence_files = glob.glob("data/confluence_page_*.md")
    for filepath in confluence_files:
        confluence_chunks = process_confluence(filepath)
        all_standardized_data.extend(confluence_chunks)
    print(f"\nTotal standardized chunks ready for Kafka: {len(all_standardized_data)}")
    print("\nConnecting to local Kafka broker...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = "ingestion-raw"
    print(f"Streaming chunks to topic: '{topic_name}'...\n")
    for chunk in all_standardized_data:
        producer.send(topic_name, value=chunk)
        print(f"Sent {chunk['source'].upper()} chunk: {chunk['chunk_id']}")
    producer.flush()
    print("\nSuccessfully streamed all data to Kafka.")
