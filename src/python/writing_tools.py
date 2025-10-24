import os
import sys
import yaml
import numpy as np
import pandas as pd
import json

with open('../../config.yaml.local', 'r') as f:
    local_config = yaml.safe_load(f)

LOCAL_PATH = local_config['LOCAL_PATH']

RESULTS_JSON = os.path.join(LOCAL_PATH, 'results', 'results.json')
RESULTS_TEX = os.path.join(LOCAL_PATH, 'results', 'results.tex')

def update_results(x):
    if os.path.exists(RESULTS_JSON):
        with open(RESULTS_JSON, 'r') as f:
            results = json.load(f)
    else:
        results = {}

    for k, v in x.items():
        results[k] = v

    with open(RESULTS_JSON, 'w') as f:
        json.dump(results, f)
    with open(RESULTS_TEX, 'w') as f:
        for k, v in results.items():
            f.write(f"%<*{k}>\n")
            f.write(f"{v}\n")
            f.write(f"%</{k}>\n")
    
    return results

# Make tbl (a list of lists of strings) into a latex table
def latex_table(tbl, header='', footer=''):
    n_rows = len(tbl)
    n_cols = 0
    for row in tbl:
        if len(row) > n_cols:
            n_cols = len(row)
    out = ''
    out += header + '\n'
    for row in tbl:
        out += ' & '.join(row) + ' \\\\ [1ex] \n'
    out += footer + '\n'
    return out