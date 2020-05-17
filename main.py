import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from extract_data import read_chat
from datetime import datetime

# Read chats into dataframes
jcldrs = read_chat('jcleaders1.txt')
ss = read_chat('ss.txt')

rplc = {   '‪+65 8533 0067': 'Darren Ng',
           '‪+65 9144 8875' : 'Garei',
           '‪+65 9855 6180': 'Jess',
           '‪+65 8328 4325' : 'Jamie',
           '‪+65 9389 8933' : 'lows'
           }

# test encoding jcldrs.iloc[100][1]
jcldrs['authors'] = jcldrs[['authors']].replace(rplc)

jcldrs.describe()

# Messages per month
num_of_messages = jcldrs.groupby(by=[jcldrs.time.dt.year,jcldrs.time.dt.month])['messages'].size()
num_of_messages.plot(kind='bar')

jcldrs['messages'] = jcldrs['messages'].astype('str')

# Number of announcements per month
announcements = jcldrs.loc[jcldrs['messages'].str.len() > 200]
num_of_announcements = announcements.groupby(by=[announcements.time.dt.year,announcements.time.dt.month])['messages'].size()
num_of_announcements.plot(kind='bar')

# Most frequent author
talk_the_most = jcldrs.authors.value_counts()
talk_the_most[:10]

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
y_1 = y.loc[(y['sequence'] < 0.01) | (y['sequence'].isnull())][['time','authors_x','messages_x','sequence']]
len(y_1)
## Finding clusters
# Adding indicators to long messages
indicator = []
for i in range(len(y_1)):
    indicator.append(len(y_1.iloc[i]['messages_x']))

y_1['long_msg'] = pd.Series(indicator).values

y_1.sort_index()
y_1.to_excel('y_1.xlsx')

pd.Series(indicator).describe()

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

for col, row in y_1.iterrows():
    if len(row['messages_x']) > 200:
        chunks.append(last_chunk)
        last_chunk = Chunk(row['messages_x'], row['time'])
    else:
        last_chunk.messages.append(row['messages_x'])
        td = row['time'] - last_chunk.time
        last_chunk.time_lags.append(td.seconds)

time = [x.time for x in chunks]
message = [x.original for x in chunks]
time_lags = [x.calculate_lags() for x in chunks]
no_of_msgs = [len(x.messages) for x in chunks]

j = pd.DataFrame(list(zip(time,message,time_lags,no_of_msgs)), columns=['time','messages','time_lags','numofmsgs'])
j_1 = j.loc[j.numofmsgs > 3]
j_1.to_excel('j_1.xlsx')



