"""
Module contains needed connections to scraping and processing DBs.
"""

import mysql.connector
from sqlalchemy import create_engine


def connect_to_mpdscraping() -> mysql.connector.connect:
    """
    Get connection to scraping DB.
    """
    return mysql.connector.connect(
        host="",
        user="",
        passwd="",
        database="",
        connect_timeout=10000
    )


def connect_to_mpdprocessing_new() -> mysql.connector.connect:
    """
    Get connection to processing DB.
    """
    return mysql.connector.connect(
        host="",
        user="",
        passwd="",
        database="",
        connect_timeout=10000
    )


def connect_to_mpdprocessing_new_engine() -> create_engine:
    """
    Get connection to processing DB using SQLAlchemy.
    """
    return create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                         .format(host="",
                                 user="",
                                 pw="",
                                 db=""))
