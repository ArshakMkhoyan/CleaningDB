"""
This module is used to process raw data.
"""

from utils import remove_punctuations, remove_stopwords, imply_stemming, str2datetime, datetime2date, remove_html_tag
from connect_to_db import connect_to_mpdscraping
from typing import Tuple
import mysql.connector
import pandas as pd
import datetime
import html


def process_initial_events(event_df: pd.DataFrame) -> pd.DataFrame:
    """Initial processing of event table. All processing steps are written in comments below"""
    # Leave only good events (good event is the one that has both title and url)
    event_df = event_df.dropna(subset=['title', 'url'])
    # Escape html symbols
    event_df[['title', 'description']] = event_df[['title', 'description']].applymap(
        lambda x: html.unescape(str(x)) if pd.notna(x) else x)
    # Remove html anchors from description
    event_df['description'] = event_df['description'].apply(remove_html_tag)
    # Removing totally similar events
    event_df = event_df.drop_duplicates(
        subset=[col for col in event_df.columns if col not in ['id']])
    # Removing canceled events
    cancel = event_df['title'].apply(lambda x: 'cancel' in x.lower() or 'pospone' in x.lower() if x else False)
    event_df = event_df[~cancel]
    print('Shape of processing data:', event_df.shape)
    # Process title
    event_df['title_modified'] = event_df['title'].str.lower() \
        .apply(remove_punctuations) \
        .apply(remove_stopwords) \
        .apply(imply_stemming) \
        .apply(lambda x: ' '.join(sorted(x.split())))
    return event_df


def process_initial_time(time_df: pd.DataFrame) -> pd.DataFrame:
    """Initial processing of time table. All processing steps are written in comments below"""
    current_date = datetime.date.today()
    year_date = datetime.date.today() + datetime.timedelta(days=365 * 4)

    # Drop duplicated records
    time_df = time_df.drop_duplicates(subset=[col for col in time_df.columns if col not in ['id']])

    time_df['start_time'] = time_df['start_time'].apply(str2datetime)
    time_df['end_time'] = time_df['end_time'].apply(str2datetime)
    # Drop records that don't have start time
    time_df = time_df.dropna(subset=['start_time'])
    # Drop records that are planned to happen more than 4 years from now
    time_df = time_df[time_df['start_time'].apply(lambda x: current_date <= datetime2date(x) <= year_date)]
    return time_df


def get_filter_query(filter_list_num: int, table_name: str, column_name: str) -> str:
    """Creates command to use it to query DB and get needed data"""
    placeholder = '%s'
    placeholders = ', '.join(placeholder for _ in range(filter_list_num))
    query_to_filter = f'select * from {table_name} where {column_name} in (%s)' % placeholders
    return query_to_filter


def get_event_time_concated(source: str, is_affiliate: bool, connection: mysql.connector.connect,
                            main_df_event: pd.DataFrame, main_df_time: pd.DataFrame) -> Tuple[
                            pd.DataFrame, pd.DataFrame]:
    """Gets raw event and time tables and processes them"""
    event_chunk = pd.read_sql_query(f"select * from raw_event where source = '{source}'",
                                    connection)
    event_chunk = process_initial_events(event_chunk)
    # If source is affiliate filter events both by url and title_modified, because there are sources that have
    # same event name but different urls to event. It is important to keep affiliates.
    if is_affiliate:
        event_chunk = event_chunk.drop_duplicates(['url', 'title_modified'])
    # Filter events by title_modified to keep only unique events.
    else:
        event_chunk = event_chunk.drop_duplicates('title_modified')
    # Check if event title_modified has not been already added previously.
    event_chunk = event_chunk[~event_chunk['title_modified'].isin(main_df_event['title_modified'])]

    # Process time data
    list_event_ids = event_chunk['id'].unique().tolist()
    query_to_filter = get_filter_query(len(list_event_ids), 'raw_time', 'raw_event_id')
    time_df = pd.read_sql_query(query_to_filter, connection, params=list_event_ids)

    time_df = process_initial_time(time_df)

    if time_df.shape[0] == 0:
        return main_df_event, main_df_time

    event_chunk = event_chunk[event_chunk['id'].isin(time_df['raw_event_id'].unique())]

    # Add new event and drop not needed ones
    main_df_event = pd.concat([main_df_event, event_chunk])

    main_df_time = pd.concat([main_df_time, time_df])
    main_df_time = main_df_time[main_df_time['raw_event_id'].isin(main_df_event['id'].unique())]

    return main_df_event, main_df_time


def process_raw_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Main function to process raw data"""
    mpdscraping_connection_event = connect_to_mpdscraping()

    event_new_df = pd.DataFrame([], columns=['id', 'url', 'title', 'image_url', 'description',
                                             'source', 'is_affiliate', 'title_modified'])

    time_new_df = pd.DataFrame([], columns=['id', 'raw_event_id', 'start_time', 'end_time', 'location',
                                            'processed_street_address', 'postal_code', 'longitude', 'latitude',
                                            'state'])

    affiliate_sources = \
        pd.read_sql("SELECT DISTINCT source FROM raw_event WHERE is_affiliate = '1';", mpdscraping_connection_event)[
            'source'].to_list()
    other_sources = \
        pd.read_sql("SELECT DISTINCT source FROM raw_event WHERE is_affiliate = '0';", mpdscraping_connection_event)[
            'source'].to_list()

    for affiliate_source in affiliate_sources:
        event_new_df, time_new_df = get_event_time_concated(source=affiliate_source, is_affiliate=True,
                                                            connection=mpdscraping_connection_event,
                                                            main_df_event=event_new_df, main_df_time=time_new_df)

        print(f"Source '{affiliate_source}' processed. Shape of main event data: {event_new_df.shape}")

    for other_source in other_sources:
        event_new_df, time_new_df = get_event_time_concated(source=other_source, is_affiliate=False,
                                                            connection=mpdscraping_connection_event,
                                                            main_df_event=event_new_df, main_df_time=time_new_df)

        print(f"Source '{other_source}' processed. Shape of main event data: {event_new_df.shape}")

    # Add virtual event column
    time_new_df['is_virtual'] = time_new_df['location'].isna() + 0
    # Check if event has only one type of events (virtual or not)
    check_virtual = time_new_df.groupby('raw_event_id')['is_virtual'].nunique()
    if check_virtual.nunique() != 1:
        print('Number of events that have both virtual and not virtual locations:', (check_virtual > 1).sum())
        # Drop these events for now
        time_new_df = time_new_df[check_virtual < 1]
        event_new_df = event_new_df[event_new_df['id'].isin(time_new_df['raw_event_id'].unique())]
    del check_virtual

    to_replace = time_new_df.groupby('raw_event_id')['is_virtual'].mean()
    # Add is_virtual to event table
    event_new_df['is_virtual'] = event_new_df['id'].apply(lambda x: to_replace[x])
    del to_replace
    time_new_df = time_new_df.drop(columns=['is_virtual'])

    # Add more virtual events
    virtual_mask = event_new_df['title'].apply(lambda x: 'virtual' in x.lower())
    event_new_df['is_virtual'][virtual_mask] = 1

    # Add Live Stream events
    live_mask = event_new_df['title'].apply(lambda x: 'live stream' in x.lower())
    event_new_df['is_virtual'][live_mask] = 2

    # Query price table
    list_time_ids = time_new_df['id'].unique().tolist()
    query_to_filter = get_filter_query(len(list_time_ids), 'raw_price', 'raw_time_id')
    price_df = pd.read_sql_query(query_to_filter, mpdscraping_connection_event, params=list_time_ids)
    price_df = price_df.drop_duplicates([col for col in price_df.columns if col not in ['id']])

    print('Event shape:', event_new_df.shape)
    print('Time shape', time_new_df.shape)
    print('Price shape:', price_df.shape)
    return event_new_df, time_new_df, price_df
