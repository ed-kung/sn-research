import pandas as pd

#---- important user ids ----

# user_ids for SN founder and employees
k00b_id = 616 # @k00b
sn_id = 4502 # @sn
ek_id = 6030 # @ek
sox_id = 26458 # @sox
sn_employee_ids = [k00b_id, sn_id, ek_id, sox_id]

# anon
anon_id = 27

# my user_id
self_id = 5597

# user_id of someone who spammed freebies
spammer_id = 30375
spam_date = "2025-08-25"  # date in which spamming occurred

#---- important item ids ----

# item_ids for perennial posts
faq_id = 349
story_id = 1620
changelog_id = 78763
guide_id = 81862
tos_id = 338393
privacy_id = 338369
copyright_id = 338453
perennial_item_ids = [faq_id, story_id, changelog_id, guide_id, tos_id, privacy_id, copyright_id]

#---- important dates and posts ----

# 10% sybil fees
sf10_item_id = 98002
sf10_date = pd.to_datetime("2022-11-23 19:12:15.571", utc=True)

# post fees increase to 10 sats
fee10_item_id = 313522
fee10_date = pd.to_datetime("2023-11-12 18:39:34.742", utc=True)

# sybil fee up to 30%
sf30_item_id = 692150 # item id where k00b announced sybil fee going to 30%
sf30_date = pd.to_datetime("2024-09-19 21:38:43.918", utc=True)

# SN goes non-custodial
nc_item_id = 835465
nc_date = pd.to_datetime("2025-01-03 19:06:36.050", utc=True)

# data start and end
data_start = pd.to_datetime("2021-06-11 19:26:02.662000+00:00", utc=True)
data_end = pd.to_datetime("2025-10-05 19:53:06.794000+00:00", utc=True)

