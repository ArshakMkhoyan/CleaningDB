"""
Module contains general functions.
"""

from datetime import datetime as dt
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
import string


def remove_punctuations(input_str: str) -> str:
    """Removes punctuations from a string"""
    exclude = set(string.punctuation)
    return ''.join(ch for ch in input_str if ch not in exclude)


def remove_stopwords(input_str: str) -> str:
    """Removes stopwords from a string"""
    stop_words = stopwords.words('english')
    separated_words = input_str.split()
    percentage = sum(1 for word in separated_words if word in stop_words) / len(separated_words)
    if percentage < 0.6:
        return ' '.join(word for word in separated_words if word not in stop_words)
    else:
        return ' '.join(separated_words)


def imply_stemming(input_str: str) -> str:
    """Stems the string"""
    stemmer = PorterStemmer()
    return ' '.join(stemmer.stem(word) for word in input_str.split())


def str2datetime(s: str) -> dt.strptime:
    """Converts string to datetime object"""
    try:
        return dt.strptime(s, '%Y-%m-%dT%H:%M:%S')
    except:
        return None


def datetime2date(s: dt.strptime):
    """Parses date from datetime object"""
    try:
        return s.date()
    except:
        return s


def remove_html_tag(desc: str) -> str:
    """Removes html tags from string"""
    if desc and '<a href' in desc:
        return BeautifulSoup(desc).text.strip()
    return desc


def add_data(df: pd.DataFrame, sql_table_name: str, connection: mysql.connector.connect, batch_size: int = 5000,
             return_mapping: bool = True) -> dict:
    """
    Pushes given df to DB and returns id mapping. When data is added to DB new ids are generated in DB.
    Thus, mapping is returned to latter use it to create connections between 2 tables.
    """
    if return_mapping:
        ids = df[['id']]
        df = df.drop(columns=['id'])

    df.to_sql(sql_table_name, connection, if_exists='append', index=False, chunksize=batch_size)
    print(f'Added table: {sql_table_name}')

    if return_mapping:
        last_id = pd.read_sql_query(f'select max(id) + 1 from {sql_table_name}', connection).iloc[0, 0]
        ids_added = list(range(last_id - len(df), last_id))

        ids['id_db'] = ids_added
        mapping = ids[['id', 'id_db']].set_index('id')['id_db'].to_dict()

        return mapping
