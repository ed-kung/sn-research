import os
import sys
import yaml
import pandas as pd

with open("../../config.yaml.local", "r") as f:
    LOCAL_CONFIG = yaml.safe_load(f)

LOCAL_PATH = LOCAL_CONFIG['LOCAL_PATH']
RAW_DATA_PATH = LOCAL_CONFIG['RAW_DATA_PATH']

sys.path.append(os.path.join(LOCAL_PATH, 'src/python'))

import globals

def get_posts():
    df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'item.parquet'))
    df = df.loc[df['parentId'].isnull()].reset_index(drop=True)
    df['subName'] = df['subName'].fillna('')
    cols = ['id', 'userId', 'created_at', 'title', 'text', 'subName', 'freebie', 'bio', 'cost', 'pollCost', 'invoiceActionState', 'paidImgLink']
    df = df[cols]

    # attach n_uploads
    itemupload_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'itemupload.parquet'))
    n_uploads_df = itemupload_df.groupby('itemId').agg(n_uploads=('uploadId', 'count')).reset_index()
    n_uploads_df = n_uploads_df.rename(columns={'itemId': 'id'})
    df = df.merge(n_uploads_df, on='id', how='left')
    df['n_uploads'] = df['n_uploads'].fillna(0).astype(int)

    # infer cost modifiers
    df['cost_modifier'] = 0
    df = df.sort_values(by=['userId', 'created_at'], ascending=True).reset_index(drop=True)
    curr_user = 0
    curr_time = pd.to_datetime("2000-01-01", utc=True)
    curr_modifier = 0
    for idx, row in df.iterrows():
        userId = row['userId']
        created_at = row['created_at']
        if row['bio'] or row['freebie']:
            continue
        if userId == globals.anon_id:
            curr_user = userId
            curr_time = created_at
            curr_modifier = 0
            df.loc[idx, 'cost_modifier'] = 1
        if (userId != curr_user) or (created_at - curr_time > pd.Timedelta(minutes=10)):
            curr_user = userId
            curr_time = created_at
            curr_modifier = 0
            continue
        curr_time = created_at
        curr_modifier += 1
        df.loc[idx, 'cost_modifier'] = curr_modifier

    return df


def get_items():
    item_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'item.parquet'))

    # attach root subname
    root_df = item_df.loc[item_df['parentId'].isnull()].reset_index(drop=True).copy()
    root_df = root_df[['id', 'subName']]
    root_df = root_df.rename(columns={'id': 'rootId', 'subName': 'root_subName'})
    item_df = item_df.merge(root_df, on='rootId', how='left')
    mask = item_df['subName'].isnull() & (~item_df['rootId'].isnull())
    item_df.loc[mask, 'subName'] = item_df.loc[mask, 'root_subName']

    # attach n_uploads
    itemupload_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'itemupload.parquet'))
    n_uploads_df = itemupload_df.groupby('itemId').agg(n_uploads=('uploadId', 'count')).reset_index()
    n_uploads_df = n_uploads_df.rename(columns={'itemId': 'id'})
    item_df = item_df.merge(n_uploads_df, on='id', how='left')
    item_df['n_uploads'] = item_df['n_uploads'].fillna(0).astype(int)

    return item_df

def get_post_fees():
    item_df = get_items()
    itemAct_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'itemact.parquet'))
    item_df = item_df.rename(columns={
        'id': 'itemId',
        'userId': 'item_userId',
        'parentId': 'item_parentId',
        'rootId': 'item_rootId',
        'freebie': 'item_is_freebie',
        'bio': 'item_is_bio',
        'subName': 'item_subName',
        'n_uploads': 'item_n_uploads'
    })
    item_df = item_df[['itemId', 'item_userId', 'item_parentId', 'item_rootId', 'item_is_freebie', 'item_is_bio', 'item_subName', 'item_n_uploads']]
    itemAct_df = itemAct_df.loc[itemAct_df['act']=='FEE'].reset_index(drop=True)
    my_df = itemAct_df.merge(item_df, on='itemId', how='left')
    my_df = my_df.loc[my_df['userId'] == my_df['item_userId']].reset_index(drop=True)
    return my_df


