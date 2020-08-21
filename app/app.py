from flask import Flask, render_template, request
import joblib
import dill as pickle
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

from app.db import load_mongo_collection_as_dataframe, connect_to_mongo
from app.creds import USERNAME, PWD
from app import nlp_pipeline
import sys
sys.modules['nlp_pipeline'] = nlp_pipeline


# initialize flask app
app = Flask(__name__, static_folder='static', template_folder='templates')
#app.config.from_object('app.config.ProductionConfig')
app.config.from_object('app.config.ProductionConfig')

# load metadata from mongo and convert topic weights to topic booleans based on threshold
METADATA = load_mongo_collection_as_dataframe(db=connect_to_mongo(USERNAME, PWD), collection_name='metadata')
THRESHOLD = 0.2

# load NLP models
PIPELINE = pickle.load(open('app/static/ml_models/tfidf_pipeline.pkl', 'rb'))
TOPIC_MODEL = joblib.load('app/static/ml_models/tfidf_nmf_model.pkl')


@app.route('/')
def index():
    """
    Loads initial home page.
    """
    metadata = METADATA.copy()
    for col in metadata.columns[6:]:
        metadata[col] = metadata[col] > THRESHOLD
        metadata[col].replace(True, '1', inplace=True)
        metadata[col].replace(False, '0', inplace=True)

    return render_template('index.html',
                           comedy_info=metadata.values.tolist(),
                           dropdown_options=['Observational', 'The Black Experience', 'British & Australian',
                                             'Political', 'Immigrant Upbringing', 'Relationships & Sex'],
                           search_text='')


# Reshape cards list

@app.route('/search', methods=['POST'])
def search():
    """
    Sort

    :return:
    """
    metadata = METADATA.copy()

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

    for col in metadata.columns[6:]:
        metadata[col] = metadata[col] > THRESHOLD
        metadata[col].replace(True, '1', inplace=True)
        metadata[col].replace(False, '0', inplace=True)

    return render_template('index.html',
                           comedy_info=metadata.values.tolist(),
                           dropdown_options=['Observational', 'Black Culture', 'British & Australian',
                                             'Political', 'Immigrant Upbringing', 'Relationships & Sex'],
                           search_text=search_term)
