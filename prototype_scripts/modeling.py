"""
Contains functions for performing topic modeling on vectorized comedy transcript corpus.

Stephen Kaplan, 2020-08-13
"""
import pandas as pd
from sklearn.decomposition import NMF, TruncatedSVD, LatentDirichletAllocation
from sklearn.cluster import KMeans
import pickle

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


if __name__ == "__main__":
    n = 6

    # tried out LSA, LDA, and NMF with both word count and TF/IDF as input. NMF with TF/IDF seems best (for now)
    count_vectorized_comedy_corpus = pd.read_pickle('data/count_vectorized_standup_comedy_transcripts.pkl')
    tfidf_comedy_corpus = pd.read_pickle('data/tfidf_standup_comedy_transcripts.pkl')

    doc_topic, topic_word, word_topic, topic_model = get_topics(model=NMF, n_components=n, vectorized_corpus=tfidf_comedy_corpus)

    top_words = get_top_words(word_topic)

    pickle.dump(topic_model, open('../app/static/ml_models/tfidf_nmf_20200818.pkl', 'wb'))


    # TODO should I cluster? or just use the topics to filter? also kmeans works best for normally distributed grouops
    #kmeans = KMeans(n_clusters=10, random_state=0).fit(doc_topic)
    #db = connect_to_mongo(username=USERNAME, password=PWD)
    #metadata = load_mongo_collection_as_dataframe(db, 'metadata')
    #metadata['cluster'] = kmeans.labels_


