import json
import pandas as pd
import datetime

"""Returns some dfs in which calculations can be done"""
df = pd.DataFrame()
search_str = "coil_ratings_2"  # takes every record with this. Each agent is sending that info while alive communicating to log.
l = []
N = 100000
#"C:/Users/ALES/.PyCharmCE2019.3/2020/Version_2/log.log"
with open("C:/Users/ALES/Desktop/log.SIMU1BUENA.log") as f:
    for line in f.readlines()[-N:]:  # from the last 1000 lines
        if search_str in line:  # find search_str
            n = line.find("{")
            a = line[n:]
            l.append(a)
df_0 = pd.DataFrame(l, columns=['register'])
for ind in df_0.index:
    if ind == 0:
        element = df_0.loc[ind, 'register']
        z = json.loads(element)
        df = pd.DataFrame.from_dict(z)
    else:
        element = df_0.loc[ind, 'register']
        y = json.loads(element)
        b = pd.DataFrame.from_dict(y)
        df = df.append(b)
df = df.reset_index(drop=True)
print(df.to_string())
df['pre_auction_start'] = pd.to_datetime(df['pre_auction_start'], unit='ms')
df['auction_start'] = pd.to_datetime(df['auction_start'], unit='ms')
df['auction_finish'] = pd.to_datetime(df['auction_finish'], unit='ms')
df['tr_booking_confirmation_at'] = pd.to_datetime(df['tr_booking_confirmation_at'], unit='ms')
df['wh_booking_confirmation_at'] = pd.to_datetime(df['wh_booking_confirmation_at'], unit='ms')
df['slot_1_start'] = pd.to_datetime(df['slot_1_start'], unit='ms')
df['slot_1_end'] = pd.to_datetime(df['slot_1_end'], unit='ms')
df['slot_2_start'] = pd.to_datetime(df['slot_2_start'], unit='ms')
df['slot_2_end'] = pd.to_datetime(df['slot_2_end'], unit='ms')
df.at[0, 'pre_auction_duration'] = df.loc[0, 'wh_booking_confirmation_at'] - df.loc[0, 'pre_auction_start']
df.at[0, 'auction_duration'] = df.loc[0, 'auction_finish'] - df.loc[0, 'auction_start']
df2 = pd.DataFrame()
d = df['id'].tolist()
a = list(set(d))
for i in a:
    df1 = df.loc[df['id'] == i]
    df1 = df1.reset_index(drop=True)
    indexes = df1.index.values.tolist()
    df1['auction_id_ind'] = indexes
    df1['auction_id_ind'] += 1
    df2 = df2.append(df1)
df2 = df2[['coil_length', 'auction_id_ind']]
df = df.merge(df2, on='coil_length')
print(df.to_string())

gantt_df = pd.DataFrame()
coil_loc_diagram = pd.DataFrame()
for ind in df.index:
    gantt_df_0 = pd.DataFrame.from_dict(df.loc[ind, 'gantt'])
    gantt_df_0['auction_id'] = ind + 1
    gantt_df = gantt_df.append(gantt_df_0)
    coil_loc_diagram_0 = pd.DataFrame.from_dict(df.loc[ind, 'location_diagram'])
    coil_loc_diagram_0['auction_id'] = ind + 1
    coil_loc_diagram = coil_loc_diagram.append(coil_loc_diagram_0)

#gantt_df['start'] = gantt_df['start'] / 1000
#gantt_df['finish'] = gantt_df['finish'] / 1000
gantt_df['start'] = pd.to_datetime(gantt_df['start'], unit='ms')
gantt_df['finish'] = pd.to_datetime(gantt_df['finish'], unit='ms')
gantt_df['duration'] = gantt_df['duration'] * (1/60) * (1/1000) * (1/60) * (1/24)
gantt_df['Duration_minutes'] = gantt_df['duration'] * 60 * 24
gantt_df['Duration_seconds'] = gantt_df['duration'] * 60 * 24 * 60

print(gantt_df.to_string())
print(coil_loc_diagram.to_string())

####coil_ratings_1
ratings_1 = pd.DataFrame()
var = []
for i in range(len(df['id'])):
    ratings_1_0 = pd.DataFrame(df.loc[i, 'coil_ratings_1'][0])
    ratings_1_0 = ratings_1_0.reset_index(drop=True)
    #print(list(ratings_1_0.columns.values))
    ratings_1_0 = ratings_1_0[['id', 'location', 'int_fab', 'bid', 'budget', 'ship_date_seconds', 'ship_date_rating', 'temp_rating', 'bid_rating', 'int_fab_rating', 'rating']]
    ratings_1_0.insert(loc=0, column='auction_owner', value=df.loc[i, 'id'])
    ratings_1_0.insert(loc=1, column='auction_id', value=i+1)
    ratings_1_0.insert(loc=2, column='auction_id_ind', value=df.loc[i, 'auction_id_ind'])
    var1 = ratings_1_0['id'].to_list()
    var.append(var1)
    ratings_1 = ratings_1.append(ratings_1_0)

ratings_1 = ratings_1.reset_index(drop=True)
ratings_1_list = list(['ship_date_rating', 'temp_rating', 'bid_rating', 'int_fab_rating'])
dfx1 = pd.DataFrame([], columns=['auction_id', 'auction_owner', 'coil_id', 'rating_type', 'rating'])
dfx = pd.DataFrame([], columns=['auction_id', 'auction_owner', 'coil_id', 'rating_type', 'rating'])
ratings_1['int_fab_rating'] = 0.0

for i in range(len(ratings_1['auction_owner'].to_list())):
    dfa = ratings_1[:i+1]
    dfb = dfa[['id', 'location', 'int_fab', 'bid', 'budget', 'ship_date_seconds']]
    for x in range(len(ratings_1_list)):
        dfx1.at[x, 'rating_type'] = ratings_1_list[x]
        dfx1.at[x, 'rating'] = dfa.loc[i, str(ratings_1_list[x])]
        a = dfa.loc[i, str(ratings_1_list[x])]
        dfx1['auction_owner'] = dfa.loc[i, 'auction_owner']
        dfx1['auction_id'] = dfa.loc[i, 'auction_id']
        dfx1['auction_id_ind'] = dfa.loc[i, 'auction_id_ind']
        dfx1['coil_id'] = dfa.loc[i, 'id']
        dfx1['bid'] = dfa.loc[i, 'bid']
        dfx1['budget'] = dfa.loc[i, 'budget']
        dfx1['ship_date'] = dfa.loc[i, 'ship_date_seconds']

    dfx = dfx.append(dfx1)
ratings_1_df = dfx.reset_index(drop=True)
print(ratings_1.to_string())

#### coil_bids
df_coils = pd.DataFrame()
search_str = "initial_bid"  # takes every record with this. Each agent is sending that info while alive communicating to log.
l = []
N = 100000
with open("C:/Users/ALES/.PyCharmCE2019.3/2020/Version_2/log.log") as f:
    for line in f.readlines()[-N:]:  # from the last 1000 lines
        if search_str in line:  # find search_str
            n = line.find("{")
            a = line[n:]
            l.append(a)
df_coils_0 = pd.DataFrame(l, columns=['register'])
for ind in df_coils_0.index:
    if ind == 0:
        element = df_coils_0.loc[ind, 'register']
        z = json.loads(element)
        df_coils = pd.DataFrame.from_dict(z)
    else:
        element = df_coils_0.loc[ind, 'register']
        y = json.loads(element)
        b = pd.DataFrame.from_dict(y)
        df_coils = df_coils.append(b)
df_coils = df_coils.reset_index(drop=True)
print(f'df_coils: {df_coils.to_string()}')
df_coils = df_coils.drop_duplicates()
print(f'df_coils: {df_coils.to_string()}')
df_coils = df_coils.sort_values(['id', 'auction_owner', 'initial_bid', 'second_bid', 'won_bid', 'accepted_bid'], ascending=[True, True, True, False, True, True])
print(f'df_coils: {df_coils.to_string()}')