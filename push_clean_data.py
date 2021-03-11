"""
Module is used to push cleaned data to processing DB
"""

import pandas as pd
from connect_to_db import connect_to_mpdprocessing_new_engine
from utils import add_data


def push_clean_data(event_df: pd.DataFrame, time_df: pd.DataFrame, price_df: pd.DataFrame) -> dict:
    """
    Pushes clean data to processing DB.
    Params:
        event_df: processed data from raw_event table in Scraping DB
        time_df: processed data from raw_time table in Scraping DB
        price_df: processed data from raw_price table in Scraping DB
    Returns:
        Mapping from old ids from scraping event table to new ids from processed event table
    """
    mpdprocessing_new_connection = connect_to_mpdprocessing_new_engine()

    # Push event data
    event2time = add_data(df=event_df.drop(columns=['tags']), sql_table_name='event',
                          connection=mpdprocessing_new_connection, batch_size=5000,
                          return_mapping=True)

    # Push time data
    time_df['raw_event_id'] = time_df['raw_event_id'].apply(lambda x: event2time[x])
    time_new_df = time_df.rename(columns={'raw_event_id': 'event_id'})
    time_new_df.loc[:, ['start_time', 'end_time']] = time_new_df.loc[:, ['start_time', 'end_time']].applymap(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)

    time2price = add_data(df=time_new_df, sql_table_name='time', connection=mpdprocessing_new_connection,
                          batch_size=5000, return_mapping=True)

    # Push price data
    price_df['raw_time_id'] = price_df['raw_time_id'].apply(lambda x: time2price[x])
    price_df = price_df.rename(columns={'raw_time_id': 'time_id'})
    price_df = price_df.drop(columns=['id'])

    add_data(df=price_df, sql_table_name='price', connection=mpdprocessing_new_connection, batch_size=5000,
             return_mapping=False)

    return event2time
