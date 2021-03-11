"""
Main module to process the raw data.
1. Processes the event, time, and price tables.
2. Deletes the current data in DB.
3. Fills the new data to DB.
4. Processes tags and fills it to DB.
"""

from process_raw_data import process_raw_data
from process_tags import process_tags
from push_clean_data import push_clean_data
from remove_tables import delete_data_in_all_tables

event_df, time_df, price_df = process_raw_data()
delete_data_in_all_tables()
event2time = push_clean_data(event_df, time_df, price_df)
process_tags(event_df, event2time)
