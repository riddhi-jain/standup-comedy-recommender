import pandas as pd
from app.db import connect_to_mongo
from app.creds import USERNAME, PWD


def insert_to_mongo(db, collection_name, columns, file_path=None, df=None):
    """

    :param db:
    :param collection_name:
    :param columns:
    :param file_path:
    :param df:
    """
    # create collections
    collection = db[collection_name]

    # drop in case data is already there
    collection.drop()
    collection = db[collection_name]

    if (file_path is None) and (df is None):
        raise ValueError('You must specify a file path to a .pkl file or a dataframe.')

    # load data frame from local .pkl files
    if file_path is not None:
        df = pd.read_pickle(file_path)

    # format columns and match mongo naming conventions
    df.reset_index(inplace=True)
    df.columns = columns

    # convert to lists of dictionaries
    data_json = df.to_dict('records')

    # insert into mongo collections
    collection.insert_many(data_json)


if __name__ == '__main__':
    # initial mongo DB collection insertion
    comedy_mongo_db = connect_to_mongo(username=USERNAME, password=PWD)
    insert_to_mongo(db=comedy_mongo_db,
                    collection_name='metadata',
                    file_path='data/standup_comedy_metadata.pkl',
                    columns=['comedyId', 'comedian', 'title', 'year', 'imageUrl'])
    insert_to_mongo(db=comedy_mongo_db,
                    collection_name='transcripts',
                    file_path='data/raw_standup_comedy_transcripts.pkl',
                    columns=['comedyId', 'text'])
