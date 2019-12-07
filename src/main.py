# python 3.7
import sqlite3
import pandas as pd

def load_db(path):
    return sqlite3.connect(path)

def load_messages_from(conn, phone_number):
    phone_number = str(phone_number)
    handles = pd.read_sql_query("SELECT * FROM handle", conn)
    handles_for_phone_number = handles[handles['id'] == phone_number]
    row_ids_for_phone_number = list(handles['ROWID'])

    query_base = "SELECT * FROM message WHERE "
    query_filters = [f"handle_id={row_id}" for row_id in row_ids_for_phone_number]
    full_query = query_base + " OR ".join(query_filters)
    return pd.read_sql_query(full_query, conn)



