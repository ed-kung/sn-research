# find_subowner
# rolling_sum
# contains_image_or_links
# extract_internal_links
# as_week, as_date, as_month, as_quarter
# get_territories (subName)
# get_territory_transfers (subName, timeStamp, userIdFrom, userIdTo)
# get_territory_billing_cycles (subName, userId, billing_cycle_start)
# get_items (itemId)
# get_posts (itemId)
# get_comments (itemId)
# get_territory_post_fee_histories (subName, date)
# get_territory_by_day_panel (subName, date)
# get_zaps (itemId, userId, created_at)
# get_downzaps (itemId, userId, created_at)
# get_price_daily (timeOpen), get_price_weekly (week)
# get_users (userId)
# get_user_stats_days (userId, date)
# get_user_by_week_panel (userId, week)
# get_post_quality_analysis_data (itemId)
# get_post_quantity_analysis_data (subName, week)
# get_internal_digraph


import os
import sys
from duckdb import df
import yaml
import pandas as pd
import re
import numpy as np
from itertools import product
import networkx as nx

with open("../../config.yaml.local", "r") as f:
    LOCAL_CONFIG = yaml.safe_load(f)

LOCAL_PATH = LOCAL_CONFIG['LOCAL_PATH']
RAW_DATA_PATH = LOCAL_CONFIG['RAW_DATA_PATH']
DATA_PATH = LOCAL_CONFIG['DATA_PATH']

sys.path.append(os.path.join(LOCAL_PATH, 'src/python'))

import globals

# ---- Attach a column for the owner of the sub based on the time column
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

# ---- Compute rolling sums of columns
def rolling_sum(df, group_col, time_col, sum_cols, window, lag=0):
    df = df.sort_values(by=[group_col, time_col], ascending=True).reset_index(drop=True)
    for col in sum_cols:
        rolling = df.groupby(group_col)[col].shift(lag).rolling(window=window, min_periods=window).sum().reset_index(drop=True)
        df[f'rolling_{col}'] = rolling
    return df

# ---- Check if a markdown string contains images or links
IMG_LINK_PATTERN = re.compile(
    r'(https?://\S+)|'
    r'!\[.*?\]\(.*?\)|'
    r'\[.*?\]\(.*?\)'
)
def contains_image_or_links(text):
    return bool(IMG_LINK_PATTERN.search(text))
def count_image_or_links(text):
    return len(IMG_LINK_PATTERN.findall(text))

# ---- Extract internal links
def extract_internal_links(text):
    pattern = r"https://stacker\.news/items/(\d+)(?:[/?#][^\s]*)?"
    return re.findall(pattern, text)

# ---- Convert a pandas datetime series to week
def as_week(x):
    if type(x)==pd.core.series.Series:
        return x.dt.to_period('W-SAT').dt.start_time
    else:
        return x.to_period('W-SAT').start_time
def as_date(x):
    if type(x)==pd.core.series.Series:
        return x.dt.floor('D')
    else:
        return x.floor('D')
def as_month(x):
    if type(x)==pd.core.series.Series:
        return x.dt.to_period('M').dt.start_time
    else:
        return x.to_period('M').start_time
def as_quarter(x):
    if type(x)==pd.core.series.Series:
        return x.dt.to_period('Q').dt.start_time
    else:
        return x.to_period('Q').start_time

# ---- Get dataframe on territories
# ---- Each row is a unique territory (subName)
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

# ---- Get dataframe on territory ownership transfers
# ---- Each row is a unique transfer (subName, timeStamp, userIdFrom, userIdTo)
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

# ---- Get dataframe on territory billing cycles
# ---- Each row is a unique billing cycle (subName, userId, billing_cycle_start)
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

# ---- Get dataframe on items (posts and comments)
# ---- Each row is a unique item (itemId)
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

    # attach sats in first 48 hours
    zap_df = get_zaps()
    right = item_df[['itemId', 'created_at']].rename(
        columns = {'created_at': 'item_created_at'}
    )
    zap_df = zap_df.merge(right, on='itemId', how='inner')
    zap_df['hours_after_post'] = np.ceil((zap_df['zap_time'] - zap_df['item_created_at']).dt.total_seconds() / 3600)
    sats48 = zap_df.loc[zap_df['hours_after_post']<=48].groupby('itemId').agg(
        sats48 = ('sats', 'sum'),
        zappers48 = ('userId', 'nunique')
    ).reset_index()
    item_df = item_df.merge(sats48, on='itemId', how='left')
    item_df['sats48'] = item_df['sats48'].fillna(0)
    item_df['zappers48'] = item_df['zappers48'].fillna(0)

    # attach downzaps in first 48 hours
    downzap_df = get_downzaps()
    right = item_df[['itemId', 'created_at']].rename(
        columns = {'created_at': 'item_created_at'}
    )
    downzap_df = downzap_df.merge(right, on='itemId', how='inner')
    downzap_df['hours_after_post'] = np.ceil((downzap_df['downzap_time'] - downzap_df['item_created_at']).dt.total_seconds() / 3600)
    downsats48 = downzap_df.loc[downzap_df['hours_after_post']<=48].groupby('itemId').agg(
        downsats48 = ('downzap_sats', 'sum'),
        downzappers48 = ('userId', 'nunique')
    ).reset_index()
    item_df = item_df.merge(downsats48, on='itemId', how='left')
    item_df['downsats48'] = item_df['downsats48'].fillna(0)
    item_df['downzappers48'] = item_df['downzappers48'].fillna(0)

    item_df = item_df.sort_values(by='created_at', ascending=True).reset_index(drop=True)

    item_df.to_parquet(filename, index=False)
    return item_df

# ---- Get dataframe on posts only (no comments)
# ---- Each row is a unique post (itemId)
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
    
    # attach comments in first 48 hours
    comments = get_comments()
    mask = (comments['invoiceActionState'] != 'FAILED')
    comments = comments.loc[mask].reset_index(drop=True)
    right = df[['itemId', 'created_at']].rename(
        columns = {'itemId': 'rootId', 'created_at': 'post_created_at'}
    )
    comments = comments.merge(right, on='rootId', how='inner')
    comments['hours_after_post'] = np.ceil((comments['created_at'] - comments['post_created_at']).dt.total_seconds() / 3600)
    comments48 = comments.loc[
        comments['hours_after_post'] <= 48
    ].groupby('rootId').agg(
        comments48 = ('itemId', 'count')
    ).reset_index().rename(columns={'rootId': 'itemId'})
    df = df.merge(comments48, on='itemId', how='left')
    df['comments48'] = df['comments48'].fillna(0)

    df = df.sort_values(by='created_at', ascending=True).reset_index(drop=True)
    df.to_parquet(filename, index=False)
    return df

# ---- Get dataframe on comments only (no posts)
# ---- Each row is a unique comment (itemId)
def get_comments(overwrite=False):
    filename = os.path.join(DATA_PATH, "comments.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)
    df = get_items()
    df = df.loc[~df['parentId'].isnull()].reset_index(drop=True)
    df.to_parquet(filename, index=False)
    return df

# ---- Get dataframe on territory posting fee histories
# ---- Each row is territory/day and the posting fee on that day (subName, date)
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

# ---- Get territory-by-day panel dataframe
# ---- Each row is a territory/day (subName, date)
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

    # add on subowner info
    tdf = find_subowner(tdf, left_time_col='date')

    # backfill subowner info
    tdf = tdf.sort_values(by=['subName', 'date'], ascending=False).reset_index(drop=True)
    currsub = tdf['subName'].iloc[0]
    currowner = tdf['subOwner'].iloc[0]
    for idx in range(1, len(tdf)):
        sub = tdf.at[idx, 'subName']
        owner = tdf.at[idx, 'subOwner']
        if sub == currsub:
            if pd.isna(owner):
                tdf.at[idx, 'subOwner'] = currowner
            else:
                currowner = owner
        else:
            currsub = sub
            currowner = owner

    tdf = tdf.sort_values(by=['subName', 'date'], ascending=True).reset_index(drop=True)
    return tdf

# ---- Get dataframe on zaps
# ---- Each row is a unique zap (itemId, userId, created_at)
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


# ---- Get dataframe on downzaps
# ---- Each row is a unique downzap (itemId, userId, created_at)
def get_downzaps(overwrite=False):
    filename = os.path.join(DATA_PATH, "downzaps.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)

    df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "itemact.parquet"))

    df = df.loc[df['invoiceActionState'] != 'FAILED'].reset_index(drop=True)
    df = df.sort_values(by=['itemId','userId','created_at']).reset_index(drop=True)
    df['sats'] = df['msats'] / 1000

    downzaps = df.loc[df['act']=='DONT_LIKE_THIS'].groupby(['itemId', 'userId', 'created_at']).agg(
        downzap_sats = ('sats', 'sum')
    ).reset_index()

    downzaps['downzap_sats'] = downzaps['downzap_sats'].fillna(0)
    downzaps['created_at'] = downzaps['created_at'].dt.tz_localize('UTC')
    downzaps = downzaps.rename(columns={'created_at': 'downzap_time'})
    downzaps.to_parquet(filename)
    return downzaps


# ---- Get daily price data
# ---- Each row is a day (timeOpen)
def get_price_daily():
    df = pd.read_csv(
        os.path.join(RAW_DATA_PATH, "coinmarketcap-daily-historical.csv"),
        delimiter=";"
    )
    df['timeOpen'] = pd.to_datetime(df['timeOpen'])
    df['timeClose'] = pd.to_datetime(df['timeClose'])
    df['price_mid'] = (df['low'] + df['high']) / 2
    df = df.rename(columns={
        'high': 'price_high',
        'low': 'price_low',
        'open': 'price_open',
        'close': 'price_close',
    })
    keep_cols = ['timeOpen', 'timeClose', 'price_open', 'price_close',
                 'price_low', 'price_mid', 'price_high']

    return df[keep_cols]

# ---- Get weekly price data
# ---- Each row is a week
def get_price_weekly():
    daily = get_price_daily()
    daily['week'] = as_week(daily['timeOpen'])
    weekly = daily.groupby('week').agg(
        btc_price = ('price_mid', 'mean')
    ).reset_index()
    return weekly

# ---- Get users
# ---- Each row is a user (userId)
def get_users(overwrite=False):
    filename = os.path.join(DATA_PATH, "users.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)
    
    items = get_items()
    items = items.loc[items['invoiceActionState'] != 'FAILED'].reset_index(drop=True)
    items['is_post'] = items['parentId'].isnull()
    items['is_comment'] = items['parentId'].notnull()
    users = items.groupby('userId').agg(
        first_item_date = ('created_at', 'min'),
        n_posts = ('is_post', 'sum'), 
        n_comments = ('is_comment', 'sum')
    ).reset_index()

    # first autowithdraw date
    withdrawal_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "withdrawal.parquet"))
    selector = (withdrawal_df['autoWithdraw']) & (withdrawal_df['status'] == 'CONFIRMED')
    merge_df = withdrawal_df.loc[selector].groupby('userId').agg(
        first_autowithdraw_date = ('created_at', 'min')
    ).reset_index()
    users = users.merge(merge_df, on='userId', how='left')

    # first p2p zap date
    itemact_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "itemact.parquet"))
    invoice_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "invoice.parquet"))
    itemact_df = itemact_df.merge(
        invoice_df[['id','confirmedAt']].rename(columns={'id':'invoiceId'}),
        how='left', on='invoiceId'
    )
    selector = (itemact_df['invoiceId'].notnull()) & \
               (itemact_df['confirmedAt'].notnull()) & \
               (itemact_df['act'] == 'TIP')
    merge_df = itemact_df.loc[selector].groupby('userId').agg(
        first_p2p_zap_date = ('created_at', 'min')
    ).reset_index()
    users = users.merge(merge_df, on='userId', how='left')

    # get list of user wallets
    wallet_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "wallet.parquet"))
    protocol_df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "walletprotocol.parquet"))

    wallet_df = wallet_df.rename(columns={
        'id': 'walletId',
        'templateName': 'walletName'
    })
    can_send_df = protocol_df.loc[
        protocol_df['send']==True
    ].groupby('walletId').agg(
        can_send = ('send', 'max')
    ).reset_index()

    wallet_df = wallet_df.merge(can_send_df, on='walletId', how='left')
    wallet_df['can_send'] = wallet_df['can_send'].fillna(False)

    users['main_wallet'] = ''
    users['send_wallets'] = ''
    users['wallets'] = ''
    users['n_wallets'] = 0
    users['n_send_wallets'] = 0

    for idx, row in users.iterrows():
        user = row['userId']
        selector = wallet_df['userId']==user
        my_wallets = wallet_df.loc[selector].sort_values(by='priority', ascending=True)
        if len(my_wallets) == 0:
            continue
        my_main_wallet = my_wallets.iloc[0]['walletName']
        my_send_wallets = my_wallets.loc[my_wallets['can_send'], 'walletName'].tolist()
        my_wallets = my_wallets['walletName'].tolist()
        n_wallets = len(my_wallets)
        n_send_wallets = len(my_send_wallets)
        users.at[idx, 'main_wallet'] = my_main_wallet
        users.at[idx, 'send_wallets'] = ','.join(my_send_wallets)
        users.at[idx, 'wallets'] = ','.join(my_wallets)
        users.at[idx, 'n_wallets'] = n_wallets
        users.at[idx, 'n_send_wallets'] = n_send_wallets

    users.to_parquet(filename)
    return users


# ---- Get user stats by day
# ---- Each row is a user/day (userId, date)
def get_user_stats_days():
    df = pd.read_parquet(os.path.join(RAW_DATA_PATH, "user_stats_days.parquet"))
    df = df.loc[df['id'].notnull()].reset_index(drop=True)
    df['t'] = df['t'].dt.tz_localize('UTC')
    df['t'] = as_date(df['t'])
    df = df.rename(columns={'t':'date', 'id':'userId'})
    for col in ['tipped', 'rewards', 'referrals', 'one_day_referrals', 'revenue', 'stacked', 'fees', 'donated', 'billing', 'zaps', 'spent']:
        df[f"sats_{col}"] = df[f"msats_{col}"] / 1000
        df = df.drop(columns=[f"msats_{col}"])
    df = df.fillna(0)
    return df

# ---- Get user by week panel
# ---- Each row is a user/day (userId, week)
def get_user_by_week_panel(overwrite=False):
    filename = os.path.join(DATA_PATH, "user_by_week_panel.parquet")
    if (not overwrite) and os.path.exists(filename):
        return pd.read_parquet(filename)

    user_stats = get_user_stats_days()
    user_stats['week'] = as_week(user_stats['date'])

    items = get_items()
    items['week'] = as_week(items['created_at'])

    # Initialize user-weekly panel
    users = list(items['userId'].unique())
    week_start = globals.data_start.to_period('W-SAT').start_time
    week_end = globals.data_end.to_period('W-SAT').start_time
    weeks = list(pd.date_range(start=week_start, end=week_end, freq='7D'))
    df = pd.DataFrame( list(product(users, weeks)), columns=['userId', 'week'])

    # Merge on user first item and drop weeks before first item
    first_items = items.groupby('userId').agg(
        first_item_week = ('week', 'min')
    ).reset_index()
    df = df.merge(first_items, on='userId', how='left')
    df = df.loc[df['week'] >= df['first_item_week']].reset_index(drop=True)
    df = df.drop(columns=['first_item_week'])

    # User weekly stats
    weekly_stats = user_stats.drop(columns=['date'])
    weekly_stats = weekly_stats.groupby(['userId', 'week']).sum().reset_index()

    # Merge on weekly stats
    df = df.merge(weekly_stats, on=['userId', 'week'], how='left')
    df = df.fillna(0)

    # define activity as any comment, post, or zap
    df['activity'] = (
        (df['comments'] > 0) | 
        (df['posts'] > 0) |
        (df['sats_spent'] > 0) 
    )
    df['items'] = df['posts'] + df['comments']

    # Weeks since last activity
    weeks_since_last_activity = 0
    curr_user = 0
    df = df.sort_values(by=['userId', 'week'], ascending=True).reset_index(drop=True)
    df['weeks_since_last_activity'] = 0
    for idx, row in df.iterrows():
        userId = row['userId']
        activity = row['activity']
        if userId != curr_user:
            curr_user = userId
            weeks_since_last_activity = 0
        elif activity:
            weeks_since_last_activity = 0
        else:
            weeks_since_last_activity += 1
        df.at[idx, 'weeks_since_last_activity'] = weeks_since_last_activity

    # find length of each inactive spell
    df = df.sort_values(by=['userId', 'week'], ascending=False).reset_index(drop=True)
    length_of_inactivity = 0
    curr_user = 0
    df['length_of_inactivity'] = 0
    for idx, row in df.iterrows():
        userId = row['userId']
        activity = row['activity']
        weeks_since_last_activity = row['weeks_since_last_activity']
        if userId != curr_user:
            curr_user = userId
            length_of_inactivity = weeks_since_last_activity
        elif activity:
            length_of_inactivity = 0
        elif length_of_inactivity==0:
            length_of_inactivity = weeks_since_last_activity
        df.at[idx, 'length_of_inactivity'] = length_of_inactivity

    # compute two types of profit, overall and excluding zaps, donations
    df['profit0'] = df['sats_stacked'] - df['sats_spent']
    df['profit1'] = df['sats_stacked'] - df['sats_fees'] - df['sats_billing']

    # compute rolling profit 
    df = rolling_sum(df, group_col='userId', time_col='week', sum_cols=['profit0', 'profit1', 'posts', 'items'], window=8, lag=1)

    df = df.sort_values(by=['userId', 'week'], ascending=True).reset_index(drop=True)
    df.to_parquet(filename, index=False)
    return df


# ---- Get post quality analysis data
# ---- Each row is a unique post (itemId)
def get_post_quality_analysis_data():
    posts = get_posts()
    prices = get_price_daily()

    # select posts to consider
    mask = (posts['invoiceActionState'] != 'FAILED') & \
        (~posts['bio']) & (~posts['freebie']) & (~posts['saloon']) & \
        (posts['subName'].notnull()) & (posts['subName'] != '') & \
        (~posts['subName'].isin(['jobs', 'ama'])) & \
        (globals.data_end - posts['created_at'] >= pd.Timedelta(hours=48))
    posts = posts.loc[mask].reset_index(drop=True)

    # add subOwner info
    posts = find_subowner(posts)

    # select only non-zero cost posts where poster != subOwner
    mask = (posts['userId'] != posts['subOwner']) & \
        (posts['cost'] > 0)
    posts = posts.loc[mask].reset_index(drop=True)

    # get the week of the post
    posts['week'] = as_week(posts['created_at'])

    # merge on weekly price data
    prices['week'] = as_week(prices['timeOpen'])
    weekly_prices = prices.groupby('week').agg(
        btc_price = ('price_mid', 'mean')
    ).reset_index()
    posts = posts.merge(weekly_prices, on='week', how='left')

    # generate categorical subId and weekId
    posts['subId'], uniques = pd.factorize(posts['subName'])
    posts['weekId'], uniques = pd.factorize(posts['week'])

    # generate categorial for sub_subOwner_id
    posts['sub_subOwner'] = posts['subName'] + '_' + posts['subOwner'].astype(str)
    posts['sub_subOwner_id'], uniques = pd.factorize(posts['sub_subOwner'])

    # generate post metrics
    posts['text'] = posts['text'].fillna('')
    posts['num_img_or_links'] = posts['text'].apply(count_image_or_links)
    posts['num_words'] = posts['text'].apply(lambda x: len(x.split()))
    posts['is_link_post'] = (posts['url'].notnull()) & (posts['url'] != '')
    posts['link_only'] = posts['is_link_post'] & (posts['text'].str.strip() == '')
    
    # keep columns
    keep_cols = [
        'itemId', 'userId',
        'subName', 'subId', 'subOwner', 'sub_subOwner', 'sub_subOwner_id',
        'created_at', 'week', 'weekId', 'btc_price', 
        'title', 'text', 'url', 
        'cost', 'sats48', 'zappers48', 'comments48', 
        'downsats48', 'downzappers48',
        'num_img_or_links', 'num_words', 'is_link_post', 'link_only'
    ]

    return posts[keep_cols]

# ---- Get post quantity analysis data
# ---- Each row is a territory/week (subName, week)
def get_post_quantity_analysis_data():
    tdf = get_territory_by_day_panel()
    prices = get_price_daily()

    # generate territory/week level data
    tdf['week'] = as_week(tdf['date'])
    df = tdf.groupby(['subName', 'week', 'subOwner']).agg(
        n_posts = ('n_posts', 'sum'),
        posting_fee = ('posting_fee', 'mean')
    ).reset_index()

    # merge on weekly price data
    prices['week'] = as_week(prices['timeOpen'])
    weekly_prices = prices.groupby('week').agg(
        btc_price = ('price_mid', 'mean')
    ).reset_index()
    df = df.merge(weekly_prices, on='week', how='left')

    # generate categorical ids
    df['subId'], uniques = pd.factorize(df['subName'])
    df['weekId'], uniques = pd.factorize(df['week'])
    df['sub_subOwner'] = df['subName'] + '_' + df['subOwner'].astype(str)
    df['sub_subOwner_id'], uniques = pd.factorize(df['sub_subOwner'])

    keep_cols = [
        'subName', 'subId', 'subOwner', 'sub_subOwner', 'sub_subOwner_id',
        'week', 'weekId', 'btc_price',
        'n_posts', 'posting_fee'
    ]

    return df[keep_cols]

# ---- Digraph of internal links
def get_internal_digraph(overwrite=False):
    filename = os.path.join(DATA_PATH, 'internal_digraph.gml')
    if (not overwrite) and os.path.exists(filename):
        return nx.read_gml(filename)
    items = get_items()
    selector = (items['invoiceActionState'] != 'FAILED') & (items['text'].notnull()) & (items['text'].str.len() > 0)
    items = items.loc[selector].reset_index(drop=True)
    items['num_internal_links'] = items['text'].apply(
        lambda x: len(extract_internal_links(x))
    )
    items_w_links = items.loc[items['num_internal_links'] > 0].reset_index(drop=True)

    DG = nx.DiGraph()
    for idx, row in items_w_links.iterrows():
        source_id = int(row['itemId'])
        internal_links = extract_internal_links(row['text'])
        for link in internal_links:
            target_id = int(link)
            target_row = items.loc[items['itemId'] == target_id]
            if (not target_row.empty) and (target_id != source_id):
                DG.add_node(source_id)
                DG.add_node(target_id)
                DG.add_edge(source_id, target_id)

    nx.write_gml(DG, filename)
    return DG