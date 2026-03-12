import re
import unicodedata
import hashlib
import numpy as np
import uuid
import yaml
import tiktoken

with open('../../config.yaml', 'r') as f:
    config = yaml.safe_load(f)

EMBEDDING_MODEL = config['EMBEDDING_MODEL']
EMBEDDING_DIMENSION = config['EMBEDDING_DIMENSION']
EMBEDDING_MAX_TOKENS = config['EMBEDDING_MAX_TOKENS']
TOKENIZER = tiktoken.encoding_for_model(EMBEDDING_MODEL)

def sanitize(x):
    normalized = unicodedata.normalize('NFKD', x)
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"[^A-Za-z0-9_]", "", without_accents)

def get_hash(text):
    digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
    return digest

def get_uuid(text):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, text))

def token_length(text):
    return len(TOKENIZER.encode(text))

def split_to_max_length(text):
    tokens = TOKENIZER.encode(text)
    chunks = []
    for i in range(0, len(tokens), EMBEDDING_MAX_TOKENS):
        chunk_tokens = tokens[i:i + EMBEDDING_MAX_TOKENS]
        chunks.append(TOKENIZER.decode(chunk_tokens))
    return chunks

def signed_log(x):
    return np.sign(x)*np.log1p(np.abs(x))

def stars(coef, serr):
    if serr == 0:
        return '***'  # Avoid division by zero; treat as highly significant
    t_stat = np.abs(coef / serr)
    if t_stat > 2.576:
        return '***'  # p < 0.01
    elif t_stat > 1.96:
        return '**'   # p < 0.05
    elif t_stat > 1.645:
        return '*'    # p < 0.10
    else:
        return ''