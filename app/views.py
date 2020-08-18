from flask import Flask, render_template, request
import pickle

from db import load_mongo_collection_as_dataframe, connect_to_mongo

# initialize flask app
app = Flask(__name__, static_folder='static', template_folder='templates')
app.config.from_object('config')

# load data from mongo
METADATA = load_mongo_collection_as_dataframe(db=connect_to_mongo(), collection_name='metadata')

# load NLP models
pipeline = pickle.load(open('static/ml_models/tfidf_pipeline_20200818.pkl'))


@app.route('/')
def index():
    """
    Loads initial home page.
    """

    return render_template('index.html', comedy_info=METADATA.values.tolist())


# Reshape cards list

@app.route('/search', methods=['POST'])
def search():
    search_term = request.form['search']

    # TODO do stuff in modeling.py
    # TODO choose movies and replace dummies

    return render_template('index.html', recommendations=['dummy1', 'dummy2', 'dummy3'])


if __name__ == '__main__':
    app.run(debug=True)
