# python 3.7
import sys
import json
import datetime

import pandas as pd
import sqlite3

import pdutil


# copied from an old file idk if this works lol
def get_phone_number_row_ids(handles, phone_number):
    handles_for_phone_number = handles[handles['id'] == phone_number]
    return list(handles_for_phone_number['ROWID'])


# copied from an old file idk if this works lol
def load_messages_from(conn, phone_number):
    phone_number = str(phone_number)
    handles = pd.read_sql_query("SELECT * FROM handle", conn)
    handles_for_phone_number = handles[handles['id'] == phone_number]
    row_ids_for_phone_number = list(handles['ROWID'])

    query_base = "SELECT * FROM message WHERE "
    query_filters = [f"handle_id={row_id}" for row_id in row_ids_for_phone_number]
    full_query = query_base + " OR ".join(query_filters)
    return pd.read_sql_query(full_query, conn)


def load_db(path):
    return sqlite3.connect(path)

def get_handles(conn):
    return pd.read_sql_query("SELECT * FROM handle", conn)

def get_messages(conn):
    messages = pd.read_sql_query("SELECT * FROM message", conn)
    dt_epoch = datetime.datetime.utcfromtimestamp(0)
    dt_2000 = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')
    ts_2000 = (dt_2000 - dt_epoch).total_seconds() * 1000000000
    messages['date_utc'] = pd.to_datetime(messages['date'] + ts_2000, unit='ns')
    return messages

def get_chats(conn):
    return pd.read_sql_query("SELECT * FROM chat", conn)

def get_chat_message_join(conn):
    return pd.read_sql_query("SELECT * FROM chat_message_join", conn)

def list_tables(conn):
    cur = conn.cursor()
    cur.execute(" select name from sqlite_master where type = 'table' ")
    for name in cur.fetchall():
        print(name)

def join_messages_with_chat_id(messages, chat_message_join):
    messages = messages.rename(columns={'ROWID': 'message_id'})
    return pd.merge(
        messages, 
        chat_message_join[['message_id','chat_id']],
        on='message_id',
        how='left'
    )

def extract_chat_dataset(chat_id, conn):
    chat_id = int(chat_id)
    handles = get_handles(conn)
    messages = get_messages(conn)
    chats = get_chats(conn)
    chat_message_join = get_chat_message_join(conn)

    messages_with_chat_id = join_messages_with_chat_id(messages, chat_message_join)

    return pdutil.filter_df(messages_with_chat_id, 'chat_id', chat_id)

if __name__ == "__main__":
    data_fmt = "../data/{}"
    with open(data_fmt.format('config.json')) as f:
        config = json.load(f)
    
    chat_db_path = data_fmt.format(config['chat_db'])
    chat_id = config['chat_id']

    conn = load_db(chat_db_path)
    chat_dataset = extract_chat_dataset(chat_id, conn)

    chat_dataset.to_pickle(data_fmt.format(config['messages_pickle']))