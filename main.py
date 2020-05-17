import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from extract_data import read_chat
from datetime import datetime

# Read chats into dataframes
jcldrs = read_chat('jcleaders1.txt')
ss = read_chat('ss.txt')

# Messages per month
num_of_messages = jcldrs.groupby(by=[jcldrs.time.dt.year,jcldrs.time.dt.month])['messages'].size()
num_of_messages.plot(kind='bar')

jcldrs['messages'] = jcldrs['messages'].astype('str')

# Number of announcements per month
announcements = jcldrs.loc[jcldrs['messages'].str.len() > 200]
num_of_announcements = announcements.groupby(by=[announcements.time.dt.year,announcements.time.dt.month])['messages'].size()
num_of_announcements.plot(kind='bar')

# Most frequent author
frequent_authors = jcldrs.authors.value_counts()
frequent_authors[:10]

# Identifying key messages

## Levenshtein
import Levenshtein

levenshtein = [Levenshtein.distance(announcements.iloc[i]['messages'],
                                    announcements.iloc[i+1]['messages'])
               for i in range(len(announcements)-1)
               ]
announcements['levenshtein'] = [np.NaN] + levenshtein

## SequenceMatcher
seq = [SequenceMatcher(None, announcements.iloc[i]['messages'],
                                       announcements.iloc[i + 1]['messages']).ratio()
       for i in range(len(announcements) - 1)
]
announcements['sequence'] = pd.Series([np.NaN] + seq)

# Merge
y = pd.merge(jcldrs,announcements,left_on='time',right_on='time', how='outer')

# Filtering out chain messages
no_chain = y.loc[(y['sequence'] < 0.01) | (y['sequence'].isnull())][['time', 'authors_x', 'messages_x', 'sequence']]
len(no_chain)
## Finding clusters
# Adding indicators to long messages
length_of_messages = []
for i in range(len(no_chain)):
    length_of_messages.append(len(no_chain.iloc[i]['messages_x']))

no_chain['long_msg'] = pd.Series(length_of_messages).values

no_chain.sort_index()
no_chain.to_excel('no_chain.xlsx')

pd.Series(length_of_messages).describe()

class Chunk():
    def __init__(self, og='TEST', time=datetime(2012,6,3)):
        self.original = og
        self.time = time
        self.messages = []
        self.time_lags = []

    def calculate_lags(self):
        return np.mean(self.time_lags)

    def __repr__(self):
        return self.original

    def __eq__(self, other):
        return self.original == other.original

last_chunk = Chunk()
chunks = []

for col, row in no_chain.iterrows():
    if len(row['messages_x']) > 200:
        chunks.append(last_chunk)
        last_chunk = Chunk(row['messages_x'], row['time'])
    else:
        last_chunk.messages.append(row['messages_x'])
        td = row['time'] - last_chunk.time
        last_chunk.time_lags.append(td.seconds)

response_time = {
    'time' : [x.time for x in chunks],
    'message' : [x.original for x in chunks],
    'time_lags' : [x.calculate_lags() for x in chunks],
    'no_of_msgs' : [len(x.messages) for x in chunks],
}

response_lag = pd.DataFrame.from_dict(response_time)
response_lag = response_lag.loc[response_lag.numofmsgs > 3] # Filtering out messages with less than 3 responses
response_lag.to_excel('response_lag.xlsx')



