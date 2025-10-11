import os
import sys
import yaml
import pandas as pd
import re
from itertools import product

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

IMG_LINK_PATTERN = re.compile(
    r'(https?://\S+)|'
    r'!\[.*?\]\(.*?\)|'
    r'\[.*?\]\(.*?\)'
)
def contains_image_or_links(text):
    return bool(IMG_LINK_PATTERN.search(text))

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

def get_territory_billing_cycles():
    sadf = pd.read_parquet(os.path.join(RAW_DATA_PATH, "subact.parquet"))
    sadf['subName'] = sadf['subName'].str.lower()
    sadf['sats'] = sadf['msats']/1000
    sadf['created_at'] = sadf['created_at'].dt.tz_localize('UTC')
    sadf = sadf.loc[sadf['type'] == 'BILLING'].reset_index(drop=True)

    sadf['period'] = ''
    for idx, row in sadf.iterrows():
        created_at = row['created_at']
        sats = row['sats']
        if created_at < globals.sub_cost_drop_date:
            if sats <= globals.sub_cost_monthly_pre:
                sadf.at[idx, 'period'] = 'MONTHLY'
            elif sats <= globals.sub_cost_yearly_pre:
                sadf.at[idx, 'period'] = 'YEARLY'
            else:
                sadf.at[idx, 'period'] = 'PERPETUAL'
        elif created_at > globals.sub_cost_drop_date:
            if sats <= globals.sub_cost_monthly_post:
                sadf.at[idx, 'period'] = 'MONTHLY'
            elif sats <= globals.sub_cost_yearly_post:
                sadf.at[idx, 'period'] = 'YEARLY'
            else:
                sadf.at[idx, 'period'] = 'PERPETUAL'

    sadf['billing_cycle_end'] = pd.NaT
    sadf['billing_cycle_end'] = sadf['billing_cycle_end'].dt.tz_localize('UTC')
    for idx, row in sadf.iterrows():
        # special handling of mempool and art due to special circumstances granting pereptual
        if row['subName'] in ['mempool', 'art']:
            sadf.at[idx, 'billing_cycle_end'] = row['created_at'] + pd.DateOffset(years=100)
            continue
        if row['period'] == 'MONTHLY':
            sadf.at[idx, 'billing_cycle_end'] = row['created_at'] + pd.DateOffset(months=1) + pd.DateOffset(days=5)
        elif row['period'] == 'YEARLY':
            sadf.at[idx, 'billing_cycle_end'] = row['created_at'] + pd.DateOffset(years=1) + pd.DateOffset(days=5)
        elif row['period'] == 'PERPETUAL':
            sadf.at[idx, 'billing_cycle_end'] = row['created_at'] + pd.DateOffset(years=100)

    sadf = sadf.rename(columns={'created_at': 'billing_cycle_start'})
    keep_cols = ['subName', 'userId', 'billing_cycle_start', 'billing_cycle_end', 'sats', 'period']
    sadf = sadf[keep_cols]
    sadf = sadf.sort_values(by=['subName', 'billing_cycle_start'], ascending=True).reset_index(drop=True)
    return sadf 

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
    
    # check if text contains images or links
    item_df['hasImageOrLink'] = item_df['text'].fillna('').apply(contains_image_or_links)

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
        if row['bio']:
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

def get_territory_post_fee_histories(overwrite=False):
    filename = os.path.join(DATA_PATH, "territory_post_fee_histories.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)
    pdf = get_posts()
    pdf = find_subowner(pdf)
    pdf['user_is_subOwner'] = pdf['userId']==pdf['subOwner']
    pdf['date'] = pdf['created_at'].dt.floor('D')
    
    mask = (pdf['invoiceActionState']!='FAILED') & \
        (~pdf['bio']) & (~pdf['freebie']) & (~pdf['saloon']) & \
        (pdf['cost']>0) & (~pdf['user_is_subOwner']) & \
        (pdf['cost_modifier']==0) & (pdf['n_uploads']==0) & \
        (~pdf['itemId'].isin(globals.perennial_item_ids)) & \
        (pdf['userId']!=globals.spammer_id) & \
        (pdf['userId']!=globals.anon_id) & \
        (~pdf['hasImageOrLink'])

    fee_df = pdf.loc[mask].groupby(['subName', 'date']).agg(
        posting_fee = ('cost', 'min')
    ).sort_values(
        by=['subName', 'date'], ascending=True
    ).reset_index()

    for idx in range(1, len(fee_df)-1):
        prev_sub = fee_df.at[idx-1, 'subName']
        curr_sub = fee_df.at[idx, 'subName']
        next_sub = fee_df.at[idx+1, 'subName']
        prev_fee = fee_df.at[idx-1, 'posting_fee']
        curr_fee = fee_df.at[idx, 'posting_fee']
        next_fee = fee_df.at[idx+1, 'posting_fee']
        if (prev_sub==curr_sub) and (curr_sub==next_sub) and (curr_fee>prev_fee) and (curr_fee>next_fee):
            fee_df.at[idx, 'posting_fee'] = next_fee
        if (prev_sub==curr_sub) and (curr_sub==next_sub) and (curr_fee<prev_fee) and (curr_fee<next_fee):
            fee_df.at[idx, 'posting_fee'] = next_fee
    
    fee_df = fee_df.sort_values(by=['subName', 'date'], ascending=True).reset_index(drop=True)
    fee_df.to_parquet(filename, index=False)
    return fee_df

def get_territory_by_day_panel():
    fees_df = get_territory_post_fee_histories()

    posts_df = get_posts()
    posts_df['subName'] = posts_df['subName'].fillna('')
    posts_df['subName'] = posts_df['subName'].astype(str)

    billing_cycles = get_territory_billing_cycles()
    billing_cycles['billing_cycle_start'] = billing_cycles['billing_cycle_start'].dt.floor('D')
    billing_cycles['billing_cycle_end'] = billing_cycles['billing_cycle_end'].dt.floor('D')
    billing_cycles['subName'] = billing_cycles['subName'].fillna('')
    billing_cycles['subName'] = billing_cycles['subName'].astype(str)

    # Initialize territory-daily panel
    subs = list(posts_df['subName'].unique())
    N_days = (globals.data_end.date() - globals.data_start.date()).days
    dates = [(globals.data_start.date() + pd.DateOffset(days=i)).floor('D') for i in range(N_days+1)]
    tdf = pd.DataFrame(list(product(subs, dates)), columns=['subName', 'date'])
    tdf['date'] = tdf['date'].dt.tz_localize('UTC')     

    # first post date for each sub
    # drop dates before first post date
    first_post_dates = posts_df.groupby('subName').agg(
        first_post_date = ('created_at', 'min')
    ).reset_index()
    first_post_dates['first_post_date'] = first_post_dates['first_post_date'].dt.floor('D')

    tdf = tdf.merge(first_post_dates, on='subName', how='left')
    tdf = tdf[tdf['date'] >= tdf['first_post_date']].reset_index(drop=True)
    tdf = tdf.drop(columns=['first_post_date'])

    # merge on billing cycles
    tdf = tdf.sort_values(by='date', ascending=True)
    billing_cycles = billing_cycles.sort_values(by='billing_cycle_start', ascending=True)
    tdf = pd.merge_asof(
        tdf,
        billing_cycles[['subName', 'billing_cycle_start', 'billing_cycle_end']],
        by='subName',
        left_on='date',
        right_on='billing_cycle_start',
        direction='backward'
    )

    # merge on n_posts
    mask = posts_df['invoiceActionState'] != 'FAILED'
    posts_df['date'] = posts_df['created_at'].dt.floor('D')
    n_posts = posts_df.loc[mask].groupby(['subName', 'date']).agg(
        n_posts = ('itemId', 'count')
    ).reset_index()
    tdf = tdf.merge(n_posts, on=['subName', 'date'], how='left')
    tdf['n_posts'] = tdf['n_posts'].fillna(0)

    # sanity check: no posts on dates outside billing cycle
    # drop dates outside billing cycle with zero posts
    bad = (tdf['date'] > tdf['billing_cycle_end']) & (tdf['n_posts'] > 0)
    assert bad.sum() == 0
    todrop = (tdf['date'] > tdf['billing_cycle_end']) & (tdf['n_posts'] == 0)
    tdf = tdf[~todrop].reset_index(drop=True)
    tdf = tdf.drop(columns=['billing_cycle_start', 'billing_cycle_end'])

    # drop territories with no posts and null territory
    todrop = tdf['subName']==''
    tdf = tdf[~todrop].reset_index(drop=True)
    tdf['sub_n_posts'] = tdf.groupby('subName')['n_posts'].transform('sum')
    tdf = tdf[tdf['sub_n_posts']>0].reset_index(drop=True)
    tdf = tdf.drop(columns=['sub_n_posts'])

    # merge on posting fees
    tdf = tdf.sort_values(by='date', ascending=True)
    fees_df = fees_df.sort_values(by='date', ascending=True)
    tdf = pd.merge_asof(tdf, fees_df, by='subName', on='date', direction='backward')

    # backfill posting fees
    tdf = tdf.sort_values(by=['subName', 'date'], ascending=False).reset_index(drop=True)
    currsub = tdf['subName'].iloc[0]
    currfee = tdf['posting_fee'].iloc[0]
    for idx in range(1, len(tdf)):
        sub = tdf.at[idx, 'subName']
        fee = tdf.at[idx, 'posting_fee']
        if sub == currsub:
            if pd.isna(fee):
                tdf.at[idx, 'posting_fee'] = currfee
            else:
                currfee = fee
        else:
            currsub = sub
            currfee = fee

    # drop null posting fees
    todrop = tdf['posting_fee'].isnull()
    tdf = tdf[~todrop].reset_index(drop=True)
    tdf = tdf.sort_values(by=['subName', 'date'], ascending=True).reset_index(drop=True)
    return tdf

def get_zaps(overwrite=False):
    filename = os.path.join(DATA_PATH, "zaps.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)

    df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "itemact.parquet"))

    df = df.loc[df['invoiceActionState'] != 'FAILED'].reset_index(drop=True)
    df = df.sort_values(by=['itemId','userId','created_at']).reset_index(drop=True)
    df['sats'] = df['msats'] / 1000

    fees = df.loc[df['act']=='FEE'].groupby(['itemId', 'userId', 'created_at']).agg(
        fee_sats = ('sats', 'sum')
    ).reset_index()

    tips = df.loc[df['act']=='TIP'].groupby(['itemId', 'userId', 'created_at']).agg(
        tip_sats = ('sats', 'sum')
    ).reset_index()

    zaps = tips.merge(
        fees, on=['itemId', 'userId', 'created_at'], how='left'
    ).reset_index(drop=True)

    zaps['tip_sats'] = zaps['tip_sats'].fillna(0)
    zaps['fee_sats'] = zaps['fee_sats'].fillna(0)
    zaps['sats'] = zaps['tip_sats'] + zaps['fee_sats']
    zaps['created_at'] = zaps['created_at'].dt.tz_localize('UTC')
    zaps = zaps.rename(columns={'created_at': 'zap_time'})
    zaps['sybil_rate'] = zaps['fee_sats'] / zaps['sats']

    zaps.to_parquet(filename)
    return zaps
