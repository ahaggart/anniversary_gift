# python 3.7
import pandas as pd

def filter_df(df, column_name, value, is_equal=True):
    if is_equal:
        return df[df[column_name] == value]
    else:
        return df[df[column_name] != value]