"""
Module is used to delete data in tables of processing DB.
It is done before updating the DB with new data.
"""

from connect_to_db import connect_to_mpdprocessing_new
import mysql.connector


def delete_data_in_table(cursor: mysql.connector.connect, connection: mysql.connector.connect,
                         table_name: str) -> None:
    """
    Deletes data in table.
    Params:
        cursor: cursor object
        connection: MySQL connection
        table_name: table name to delete data from
    """
    sql = f"DELETE FROM {table_name}"
    cursor.execute(sql)
    connection.commit()
    print(f'Deleted {table_name} table rows')


def delete_data_in_all_tables() -> None:
    """
    Deletes data from all tables in processing DB
    """
    mpdprocessing_new_connection = connect_to_mpdprocessing_new()
    cursor = mpdprocessing_new_connection.cursor()

    tables_to_delete_data = ['event', 'time', 'price', 'tag', 'subcategory', 'category', 'tag__event',
                             'subcategory__tag', 'category__subcategory']

    for table_to_delete_data in tables_to_delete_data:
        delete_data_in_table(cursor, mpdprocessing_new_connection, table_to_delete_data)

    cursor.close()
