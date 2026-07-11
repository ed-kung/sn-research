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

def weighted_kreg(x, y, w, bw, grid, nboot=200, rng=np.random.default_rng(42)):
    # kernel matrix
    d = (grid[:,None] - x[None,:]) / bw
    K = np.exp(-0.5 * d * d)
    Kw = K * w[None, :]
    n = len(x)

    y_pred = (Kw @ y) / (Kw.sum(axis=1))

    ci_lower = None
    ci_upper = None

    # efficient bootstrap
    if nboot > 0:
        # resampling rows = reweighting by multinomial counts
        counts = rng.multinomial(n, np.full(n, 1/n), size=nboot)
        Weff = w[None, :] * counts
        numerator = (Weff * y[None, :]) @ K.T 
        denominator = (Weff @ K.T)
        y_boot = numerator / denominator
        ci_lower = np.percentile(y_boot, 2.5, axis=0)
        ci_upper = np.percentile(y_boot, 97.5, axis=0)

    return y_pred, ci_lower, ci_upper
