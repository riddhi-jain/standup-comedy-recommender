"""
Contains functions for interacting with MongoDB. If this file is run as a script, it will load the .pkl files created
by `data_acquisition.py`, and populate two collections in a remote Mongo database.
This file can be run as a script.

Stephen Kaplan, 2020-08-10
"""

import pandas as pd
from pymongo import MongoClient


def connect_to_mongo(username, password, db_name='standupComedyDB'):
    """

    :return:
    """
    config = {
        'host': '18.216.107.77:27017',
        'username': username,
        'password': password,
        'authSource': db_name
    }
    client = MongoClient(**config)
    db = client.standupComedyDB

    return db


def load_mongo_collection_as_dataframe(db, collection_name, index_col='comedyId'):
    """

    :param db:
    :param collection_name:
    :param index_col:
    :return:
    """
    collection = db[collection_name]
    collection_json = collection.find()

    df_collection = pd.DataFrame(collection_json)

    return df_collection
