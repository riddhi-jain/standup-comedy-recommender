import pandas as pd
from app.db import connect_to_mongo
from app.creds import USERNAME, PWD


def insert_comedy_data_to_mongo(db):
    """

    :param db:
    """
    # create collections
    transcripts = db['transcripts']
    metadata = db['metadata']

    # load data frames from local .pkl files
    df_transcripts = pd.read_pickle('data/raw_standup_comedy_transcripts.pkl')
    df_metadata = pd.read_pickle('data/standup_comedy_metadata.pkl')

    # format columns and match mongo naming conventions
    df_transcripts.reset_index(inplace=True)
    df_transcripts.columns = ['comedyId', 'text']
    df_metadata.reset_index(inplace=True)
    df_metadata.columns = ['comedyId', 'comedian', 'title', 'year', 'imageUrl']

    # convert to lists of dictionaries
    transcripts_json = df_transcripts.to_dict('records')
    metadata_json = df_metadata.to_dict('records')

    # insert into mongo collections
    transcripts.insert_many(transcripts_json)
    metadata.insert_many(metadata_json)


if __name__ == '__main__':
    comedy_mongo_db = connect_to_mongo(username=USERNAME, password=PWD)
    insert_comedy_data_to_mongo(comedy_mongo_db)
