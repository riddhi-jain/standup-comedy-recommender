"""
Contains functions for performing topic modeling on vectorized comedy transcript corpus.

Stephen Kaplan, 2020-08-13
"""
import pandas as pd
import numpy as np
from sklearn.decomposition import NMF
from sklearn.cluster import KMeans
import joblib

from insert_data_mongo import insert_to_mongo
from app.db import connect_to_mongo, load_mongo_collection_as_dataframe
from app.creds import USERNAME, PWD


def get_topics(model, n_components, vectorized_corpus):
    """

    :param model:
    :param n_components:
    :param vectorized_corpus:
    :return:
    """
    topic_model = model(n_components)

    df_document_topic = pd.DataFrame(topic_model.fit_transform(vectorized_corpus))
    df_topic_word = pd.DataFrame(topic_model.components_, columns=vectorized_corpus.columns)
    df_word_topic = df_topic_word.transpose()

    return df_document_topic, df_topic_word, df_word_topic, topic_model


def get_top_words(df_word_topic, n_top_words=20):
    """

    :param df_word_topic:
    :param n_top_words:
    :return:
    """
    df_top_words = pd.DataFrame()
    for col in df_word_topic.columns:
        df_top_words[col] = df_word_topic[col].sort_values(ascending=False).index[:n_top_words]

    return df_top_words


def get_top_cluster_topic_weights(centroids, n=5):
    """

    :param centroids:
    :param n:
    :return:
    """
    top_topic_weights = []
    for cluster in centroids:
        sorted_vals = sorted(cluster, reverse=True)
        sorted_topics = np.argsort(cluster)[::-1]
        top_topic_weights.append([f'({val}, {topic})' for val, topic in zip(sorted_topics[:n], sorted_vals[:n])])

    return np.array(top_topic_weights)


if __name__ == "__main__":
    # tried out LSA, LDA, and NMF with both word count and TF/IDF as input. NMF with TF/IDF seems best (for now)
    count_vectorized_comedy_corpus = pd.read_pickle('data/count_vectorized_standup_comedy_transcripts.pkl')
    tfidf_comedy_corpus = pd.read_pickle('data/tfidf_standup_comedy_transcripts.pkl')

    doc_topic, topic_word, word_topic, topic_model = get_topics(model=NMF, n_components=6,
                                                                vectorized_corpus=tfidf_comedy_corpus)

    # dump topic model to .pkl for use in search feature in flask app
    joblib.dump(topic_model, '../app/static/ml_models/tfidf_nmf_model.pkl')

    # use top words to decide on topic categories
    top_words = get_top_words(word_topic)
    topic_names = ['observational', 'blackCulture', 'britishAustralian',
                   'political', 'immigrantUpbringing', 'relationshipsSex']
    doc_topic.columns = topic_names

    # add topic weights to metadata and overwrite metadata collection in mongo db
    db = connect_to_mongo(username=USERNAME, password=PWD)
    metadata = load_mongo_collection_as_dataframe(db, 'metadata')
    metadata.drop(['comedyId', '_id'], axis=1, inplace=True)

    # fix final image URLs that were causing issues
    metadata.loc[2, 'imageUrl'] = "static/images/George Lopez - We'll Do It for Half.jpg"
    metadata.loc[193, 'imageUrl'] = "https://m.media-amazon.com/images/M/MV5BMjk0NjIwNTctMzk3ZC00OTYxLTg2NGEtNTU4OWY5MDQ3YmRlXkEyXkFqcGdeQXVyMTk3NDAwMzI@._V1_.jpg"

    metadata2 = pd.merge(metadata, doc_topic, left_index=True, right_index=True)
    columns = ['comedyId', 'comedian', 'title', 'year', 'imageUrl']
    columns.extend(topic_names)
    insert_to_mongo(db, 'metadata', columns=columns, df=metadata2)

    '''
    # create clusters
    kmeans = KMeans(n_clusters=7, random_state=0).fit(doc_topic)
    metadata['cluster'] = kmeans.labels_

    # look at topic weights for each cluster and top words for manual assignment of topic names
    cluster_centroids = kmeans.cluster_centers_
    top_words = get_top_words(word_topic)
    top_cluster_topic_weights = get_top_cluster_topic_weights(cluster_centroids)

    # extract document with weak topics and model them separately
    weak_topic_mask = metadata['cluster'].isin([1, 5])
    tfidf_comedy_corpus_weak = tfidf_comedy_corpus[weak_topic_mask]
    doc_topic2, topic_word2, word_topic2, topic_model2 = get_topics(model=NMF, n_components=5,
                                                                    vectorized_corpus=tfidf_comedy_corpus_weak)
    top_words2 = get_top_words(word_topic2)
    '''
