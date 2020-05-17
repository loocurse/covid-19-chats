import pandas as pd
import re
from datetime import datetime


def get_datetime(msg):
    '''Input: message, Output: datetime object'''
    time_re = re.compile(r'\d+/\d+/\d+,\s\d+:\d+:\d+\s\w{2}')
    x = re.findall(time_re, msg)[0]  # 18/12/17, 5:20:06 PM
    datetime_object = datetime.strptime(x, '%d/%m/%y, %I:%M:%S %p')
    return datetime_object

def get_author(msg):
    author_re = r'\d+/\d+/\d+,\s\d+:\d+:\d+\s\w{2}\]\s(.*?):'
    y = re.search(author_re, msg)
    if y:
        return y.group(1)
    else:
        return None

def get_message(msg):
    x = re.split(r'\d+/\d+/\d+,\s\d+:\d+:\d+\s\w{2}\]\s(.*?):([.\n]*)\s+',msg)[-1]
    return x


def read_chat(file):
    raw_chat = open(file, encoding="utf8").read()

    # match [13/5/20, 11:16:26 AM]
    pattern = re.compile(r'\[\d+/\d+/\d+,\s\d+:\d+:\d+\s\w{2}\][^\[]+')
    messages = re.findall(pattern, raw_chat)

    chat = {
        'time': list(),
        'authors': list(),
        'messages': list()
    }

    for msg in messages:
        chat['time'].append(get_datetime(msg))
        chat['authors'].append(get_author(msg))
        chat['messages'].append(get_message(msg))

    return pd.DataFrame.from_dict(chat)