from flask import Flask, render_template, request
import joblib
import dill as pickle
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

from app.db import load_mongo_collection_as_dataframe, connect_to_mongo
from app.creds import USERNAME, PWD

import sys
from app import nlp_pipeline

sys.modules['nlp_pipeline'] = nlp_pipeline

# initialize flask app
app = Flask(__name__, static_folder='static', template_folder='templates')
app.config.from_object('app.config.ProductionConfig')

# load metadata from mongo and convert topic weights to topic booleans based on threshold
METADATA = load_mongo_collection_as_dataframe(db=connect_to_mongo(USERNAME, PWD), collection_name='metadata')
THRESHOLD = 0.2

# load NLP models
PIPELINE = pickle.load(open('app/static/ml_models/tfidf_pipeline.pkl', 'rb'))
TOPIC_MODEL = joblib.load('app/static/ml_models/tfidf_nmf_model.pkl')


def apply_threshold_to_topics(data):
    """
    Converts topic weights in metadata to string representation of boolean. If topic weight is above the threshold, then
    the document is considered to be a member of that topic.

    :param pandas.DataFrame data: Metadata pandas dataframe, where columns 6 through the last column each represent a
                                  topic and contain topic weights for each document.
    :return: Dataframe with topic weight columns converted to boolean in string form ('1', '0').
    :rtype: pandas.DataFrame
    """
    for col in data.columns[6:]:
        data[col] = data[col] > THRESHOLD
        data[col].replace(True, '1', inplace=True)
        data[col].replace(False, '0', inplace=True)

    return data


@app.route('/')
def index():
    """
    Loads initial home page.
    """
    metadata = apply_threshold_to_topics(METADATA.copy())

    return render_template('index.html',
                           comedy_info=metadata.values.tolist(),
                           dropdown_options=['Observational', 'The Black Experience', 'British & Australian',
                                             'Political', 'Immigrant Upbringing', 'Relationships & Sex'],
                           search_text='')


@app.route('/search', methods=['POST'])
def search():
    """
    Finds comedy specials similar to the search term entered by the user.
    """
    metadata = METADATA.copy()

    # extract search term from the HTML form
    search_term = request.form['search']

    if search_term != '':
        # put search term in same vector space as data model was trained on
        search_transformed = PIPELINE.transform([search_term])
        # put search in topic space
        search_topics = TOPIC_MODEL.transform(search_transformed)

        # calculate cosine similarity matrix
        doc_topic_matrix = metadata[metadata.columns[6:]].values
        similarity = cosine_similarity(doc_topic_matrix, search_topics)
        top_10_idx = np.argsort(similarity.flatten())[-10:]
        metadata = metadata.iloc[list(top_10_idx)]

    metadata = apply_threshold_to_topics(metadata)

    return render_template('index.html',
                           comedy_info=metadata.values.tolist(),
                           dropdown_options=['Observational', 'Black Culture', 'British & Australian',
                                             'Political', 'Immigrant Upbringing', 'Relationships & Sex'],
                           search_text=search_term)


if __name__ == '__main__':
    app.run(debug=True)
