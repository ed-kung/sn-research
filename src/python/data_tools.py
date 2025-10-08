import os
import sys
import yaml
import pandas as pd

with open("../../config.yaml.local", "r") as f:
    LOCAL_CONFIG = yaml.safe_load(f)

LOCAL_PATH = LOCAL_CONFIG['LOCAL_PATH']
RAW_DATA_PATH = LOCAL_CONFIG['RAW_DATA_PATH']
DATA_PATH = LOCAL_CONFIG['DATA_PATH']

sys.path.append(os.path.join(LOCAL_PATH, 'src/python'))

import globals

def find_subowner(df, left_time_col='created_at'):
    df = df.copy()
    tdf = get_territory_transfers()
    tdf = tdf.rename(columns={'timeStamp': left_time_col, 'userIdTo': 'subOwner'})
    tdf = tdf.sort_values(by=left_time_col).reset_index(drop=True)
    df = df.sort_values(by=left_time_col).reset_index(drop=True)
    df = pd.merge_asof(
        left = df,
        right = tdf[['subName', left_time_col, 'subOwner']],
        by = 'subName',
        on = left_time_col,
        direction = 'backward'
    )
    return df

def get_territories(overwrite=False):
    filename = os.path.join(DATA_PATH, "territories.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)
    sdf = pd.read_parquet(os.path.join(RAW_DATA_PATH, "sub.parquet"))
    sdf = sdf.rename(columns={'name': 'subName'})
    sdf['subName'] = sdf['subName'].str.lower()
    sdf['created_at'] = sdf['created_at'].dt.tz_localize('UTC')

    idf = pd.read_parquet(os.path.join(RAW_DATA_PATH, "item.parquet"))
    idf['subName'] = idf['subName'].fillna('').str.lower()
    idf['created_at'] = idf['created_at'].dt.tz_localize('UTC')

    out_df = []
    for idx, row in sdf.iterrows():
        subName = row['subName']
        userId = row['userId']
        created_at = row['created_at']
        if idf.loc[idf['subName']==subName, 'created_at'].min() < created_at:
            created_at = idf.loc[idf['subName']==subName, 'created_at'].min()
        out_df.append({
            'subName': subName,
            'created_at': created_at,
            'currentOwner': userId,
        })
    out_df = pd.DataFrame(out_df)
    out_df = out_df.sort_values(by='created_at', ascending=True).reset_index(drop=True)
    out_df.to_parquet(filename, index=False)
    return out_df

def get_territory_transfers(overwrite=False):
    filename = os.path.join(DATA_PATH, "territory_transfers.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)

    sdf = get_territories()

    tdf = pd.read_parquet(os.path.join(RAW_DATA_PATH, "territorytransfer.parquet"))
    tdf['subName'] = tdf['subName'].str.lower()
    tdf['created_at'] = tdf['created_at'].dt.tz_localize('UTC')
    tdf = tdf.sort_values(by=['subName', 'created_at'], ascending=True)    

    out_df = []
    for idx, row in sdf.iterrows():
        subName = row['subName']
        userId = row['currentOwner']
        created_at = row['created_at']
        if subName not in tdf['subName'].unique():
            out_df.append({
                'timeStamp': created_at,
                'subName': subName,
                'userIdFrom': 0,
                'userIdTo': userId,
            })
            continue
        temp_df = tdf[tdf['subName']==subName].copy().reset_index(drop=True)
        out_df.append({
            'timeStamp': created_at,
            'subName': subName,
            'userIdFrom': 0,
            'userIdTo': temp_df.loc[0, 'oldUserId']
        })
        for idx2, row2 in temp_df.iterrows():
            out_df.append({
                'timeStamp': row2['created_at'],
                'subName': subName,
                'userIdFrom': row2['oldUserId'],
                'userIdTo': row2['newUserId']
            })
    out_df = pd.DataFrame(out_df)
    out_df = out_df.sort_values(by=['subName', 'timeStamp'], ascending=True).reset_index(drop=True)
    out_df.to_parquet(filename, index=False)
    return out_df

def get_items(overwrite=False):
    filename = os.path.join(DATA_PATH, "items.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)
    item_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'item.parquet'))
    item_df = item_df.rename(columns={'id': 'itemId'})
    item_df['subName'] = item_df['subName'].str.lower()
    item_df['created_at'] = item_df['created_at'].dt.tz_localize('UTC')
    item_df['saloon'] = item_df['title'] == "Stacker Saloon"

    # attach root subname and saloon info
    root_df = item_df.loc[item_df['parentId'].isnull()].reset_index(drop=True).copy()
    root_df = root_df[['itemId', 'subName', 'saloon']]
    root_df = root_df.rename(columns={
        'itemId': 'rootId', 
        'subName': 'root_subName', 
        'saloon': 'root_is_saloon',
        'bio': 'root_is_bio'
    })
    item_df = item_df.merge(root_df, on='rootId', how='left')

    # attach n_uploads
    itemupload_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, 'itemupload.parquet'))
    n_uploads_df = itemupload_df.groupby('itemId').agg(n_uploads=('uploadId', 'count')).reset_index()
    item_df = item_df.merge(n_uploads_df, on='itemId', how='left')
    item_df['n_uploads'] = item_df['n_uploads'].fillna(0).astype(int)
    
    item_df = item_df.sort_values(by='created_at', ascending=True).reset_index(drop=True)

    item_df.to_parquet(filename, index=False)
    return item_df

def get_posts(overwrite=False):
    filename = os.path.join(DATA_PATH, "posts.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)

    df = get_items()
    df = df.loc[df['parentId'].isnull()].reset_index(drop=True)

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

    df = df.sort_values(by='created_at', ascending=True).reset_index(drop=True)
    df.to_parquet(filename, index=False)
    return df


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


