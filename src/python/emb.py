import os
import sys
import yaml
import pickle
import duckdb
import json
import numpy as np
import pandas as pd
from openai import OpenAI
from datetime import datetime

with open('../../config.yaml.local', 'r') as f:
    local_config = yaml.safe_load(f)
with open('../../config.yaml', 'r') as f:
    config = yaml.safe_load(f)

LOCAL_PATH = local_config['LOCAL_PATH']
DATA_PATH = local_config['DATA_PATH']

EMBEDDING_STORE_PATH = os.path.join(LOCAL_PATH, 'db', 'embeddings.db')
BATCH_PATH = os.path.join(DATA_PATH, 'batch')

sys.path.append(os.path.join(LOCAL_PATH, 'src', 'python'))
from utils import get_hash, token_length, split_to_max_length

EMBEDDING_MODEL = config['EMBEDDING_MODEL']
EMBEDDING_DIMENSION = config['EMBEDDING_DIMENSION']
EMBEDDING_MAX_TOKENS = config['EMBEDDING_MAX_TOKENS']

OPENAI_API_KEY = local_config['OPENAI_API_KEY']
openai_client = OpenAI(api_key=OPENAI_API_KEY)


# Embedding store setup
embedding_store = duckdb.connect(EMBEDDING_STORE_PATH)
embedding_store.sql(
    f"""
    CREATE TABLE IF NOT EXISTS embeddings (
        text_hash VARCHAR(64) PRIMARY KEY,
        embedding FLOAT[{EMBEDDING_DIMENSION}],
        last_updated_at TIMESTAMP
    );
    """
)

# Batch processing setup 
batch_jobs = duckdb.connect(os.path.join(BATCH_PATH, 'batch_jobs.db'))
batch_jobs.sql(
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        input_file TEXT,
        output_file TEXT,
        status TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
)

# Function to close all connections
def close_connections():
    if embedding_store:
        embedding_store.close()
    if batch_jobs:
        batch_jobs.close()


# Helper functions for interacting with embedding store
def check_cache(text_hash):
    result = embedding_store.execute(
        "SELECT embedding FROM embeddings WHERE text_hash = ?",
        (text_hash,)
    ).fetchone()
    return result if result else None

def store_embedding(text_hash, embedding):
    timestamp = datetime.now()
    embedding_store.execute(
        "INSERT INTO embeddings (text_hash, embedding, last_updated_at) VALUES (?, ?, ?)",
        (text_hash, embedding, timestamp)
    )
    embedding_store.commit()

# Helper function for interacting with batch jobs
def get_batch_jobs_df():
    return batch_jobs.execute("SELECT * FROM jobs").df()

def store_batch_job(id, input_file):
    timestamp = datetime.now()
    batch_jobs.execute(
        "INSERT INTO jobs (id, input_file, status, created_at, updated_at) VALUES (?, ?, 'created', ?, ?)",
        (id, input_file, timestamp, timestamp)
    )
    batch_jobs.commit()

def update_batch_job(id, status, output_file=None):
    timestamp = datetime.now()
    if output_file:
        batch_jobs.execute(
            "UPDATE jobs SET status = ?, output_file = ?, updated_at = ? WHERE id = ?",
            (status, output_file, timestamp, id)
        )
    else:
        batch_jobs.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (status, timestamp, id)
        )
    batch_jobs.commit()

# Function embeddings from OpenAI
def get_embedding_openai(text):
    client_response = openai_client.embeddings.create(input=text, model=EMBEDDING_MODEL)
    embedding = client_response.data[0].embedding
    return embedding 

# Main function to handle embedding requests
def get_embedding(text, overwrite=False):
    text_hash = get_hash(text)
    if not overwrite:
        cached_response = check_cache(text_hash)
        if cached_response:
            return list(cached_response[0]) # stored as tuple in duckdb
    embedding = get_embedding_openai(text)
    store_embedding(text_hash, embedding)
    return embedding

# Function to handle embedding requests including for long texts
def get_embedding_robust(text, overwrite=False):
    chunks = split_to_max_length(text)
    chunk_embeddings = []
    for chunk in chunks:
        emb = get_embedding(chunk, overwrite=overwrite)
        chunk_embeddings.append(emb)
    chunk_lengths = np.array([token_length(chunk) for chunk in chunks])
    weights = chunk_lengths / chunk_lengths.sum()
    chunk_embeddings = np.array(chunk_embeddings)
    embedding = np.average(chunk_embeddings, axis=0, weights=weights)
    return embedding.tolist()

# Batch jobs
def create_batch_job(texts, input_filename, overwrite=False):
    full_input_path = os.path.join(BATCH_PATH, input_filename)
    if os.path.exists(full_input_path):
        print(f"Batch input file {input_filename} already exists.")
        return

    text_hashes = []
    texts_to_submit = []
    for text in texts:
        text_hash = get_hash(text)
        if not overwrite:
            cached_response = check_cache(text_hash)
            if cached_response:
                continue
        if text_hash in text_hashes:
            continue
        text_hashes.append(text_hash)
        texts_to_submit.append(text)

    if not texts_to_submit:
        print("No new texts to process.")
        return
    
    with open(full_input_path, 'w', encoding='utf-8') as f:
        for text in texts_to_submit:
            text_hash = get_hash(text)
            task = {
                'custom_id': text_hash,
                'method': 'POST',
                'url': '/v1/embeddings',
                'body': {
                    'model': EMBEDDING_MODEL,
                    'input': text
                }
            }
            f.write(json.dumps(task) + '\n')
    print(f"Batch input file created: {full_input_path} ({len(texts_to_submit)} requests)")

    # Upload file for batch
    file_upload = openai_client.files.create(
        file = open(full_input_path, 'rb'),
        purpose = 'batch'
    )
    input_file_id = file_upload.id
    batch = openai_client.batches.create(
        input_file_id = input_file_id,
        endpoint = '/v1/embeddings',
        completion_window = '24h'
    )
    batch_id = batch.id
    print(f"Batch job created with ID: {batch_id}")

    # Store batch job in database
    store_batch_job(batch_id, input_filename)

    return batch

def update_batch_status(batch_id):
    batch = openai_client.batches.retrieve(batch_id)
    if batch:
        if batch.status == 'completed':
            output_filename = f"{batch_id}_output.jsonl"
            output_file_id = batch.output_file_id
            try:
                output_file_content = openai_client.files.content(output_file_id)
                output_file_path = os.path.join(BATCH_PATH, output_filename)
                with open(output_file_path, 'w') as f:
                    f.write(output_file_content.text)
                update_batch_job(batch_id, 'completed', output_filename)
                return batch
            except Exception as e:
                print(f"Error retrieving output file for batch {batch_id}: {e}")
                update_batch_job(batch_id, 'failed')
                batch.status = 'failed'
                return batch
        else:
            update_batch_job(batch_id, batch.status)
            return batch
    print(f"Batch job with ID {batch_id} not found.")
    return None

def fail_all_jobs():
    # use to mark all current jobs as failed (clean restart)
    batch_jobs.execute(
        "UPDATE jobs SET status = 'failed', updated_at = ? WHERE status NOT IN ('written')",
        (datetime.now(),)
    )
    return None

def write_batch_to_embedding_store(batch_id, overwrite=False):
    batch_jobs_df = get_batch_jobs_df()
    batch_row = batch_jobs_df[batch_jobs_df['id'] == batch_id]
    if batch_row.empty:
        print(f"No batch job found with ID: {batch_id}")
        return 1
    output_file = batch_row.iloc[0]['output_file']
    if not output_file:
        print(f"No output file for batch job ID: {batch_id}")
        return 1
    if not os.path.exists(os.path.join(BATCH_PATH, output_file)):
        print(f"Output file {output_file} does not exist for batch job ID: {batch_id}")
        return 1
    output_file_path = os.path.join(BATCH_PATH, output_file)

    n_requests = 0
    n_written = 0
    n_error = 0
    n_existing = 0
    with open(output_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            n_requests += 1
            j = json.loads(line)
            if not j['error']:
                text_hash = j['custom_id']
                if not overwrite:
                    cached_response = check_cache(text_hash)
                    if cached_response:
                        n_existing += 1
                        continue
                response = j['response']
                embedding = response['body']['data'][0]['embedding']
                store_embedding(text_hash, embedding)
                n_written += 1
            else:
                n_error += 1
    print(f"Batch job {batch_id} processed:")
    print(f"    Total requests: {n_requests}")
    print(f"    Total written: {n_written}")
    print(f"    Total errors: {n_error}")
    print(f"    Total already existing: {n_existing}")

    return n_error