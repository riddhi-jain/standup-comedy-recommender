"""
Contains functions for interacting with MongoDB. If this file is run as a script, it will load the .pkl files created
by `data_acquisition.py`, and populate two collections in a remote Mongo database.
This file can be run as a script.

Stephen Kaplan, 2020-08-10
"""

import pandas as pd
from pymongo import MongoClient


def connect_to_mongo(username, password, db_name='standupComedyDB', public_ip='3.20.109.89'):
    """
    Connect to and return Mongo database object using PyMongo.

    :param str username: Mongo database username
    :param str password: Mongo database password
    :param str db_name: Mongo database name. Defaults to 'standupcomedyDB'.
    :param str public_ip: IP address location of mongo database. Defaults to 3.20.109.89
    :return: Mongo database object
    :rtype: pymongo.database.Database
    """
    config = {
        'host': f'{public_ip}:27017',
        'username': username,
        'password': password,
        'authSource': db_name
    }
    client = MongoClient(**config)
    db = client.standupComedyDB

    return db


def load_mongo_collection_as_dataframe(db, collection_name):
    """
    Loads a mongo collection as a Pandas dataframe.

    :param pymongo.database.Database db: Mongo database object.
    :param str collection_name: Name of Mongo collection
    :return: Dataframe containing Mongo collection contents
    :rtype: pandas.DataFrame
    """
    # get collection
    collection = db[collection_name]

    # query all data from collection
    collection_json = collection.find()

    # convert to pandas DataFrame
    df_collection = pd.DataFrame(collection_json)

    return df_collection
