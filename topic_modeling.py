"""
Contains functions for performing topic modeling on vectorized comedy transcript corpus.

Stephen Kaplan, 2020-08-13
"""
import pandas as pd
from sklearn.decomposition import NMF, TruncatedSVD, LatentDirichletAllocation


def get_topics(model, n_components, vectorized_corpus, n_top_words=10):
    """

    :param model:
    :param n_components:
    :param vectorized_corpus:
    :param n_top_words:
    :return:
    """
    m = model(n_components)
    m.fit_transform(vectorized_corpus)

    df_topic_word = pd.DataFrame(m.components_, columns=vectorized_corpus.columns)
    df_word_topic = df_topic_word.transpose()
    df_word_topic.columns = [f'topic_{t + 1}' for t in range(len(df_word_topic.columns))]
    df_top_words = pd.DataFrame()
    for col in df_word_topic.columns:
        df_top_words[col] = df_word_topic[col].sort_values(ascending=False).index[:n_top_words]

    return df_top_words, m


if __name__ == "__main__":
    vectorized_comedy_corpus = pd.read_pickle('data/count_vectorized_standup_comedy_transcripts.pkl')
    nmf_topics = get_topics(model=NMF, n_components=5, vectorized_corpus=vectorized_comedy_corpus)[0]
    lsa_topics = get_topics(model=TruncatedSVD, n_components=5, vectorized_corpus=vectorized_comedy_corpus)[0]
    lda_topics = get_topics(model=LatentDirichletAllocation, n_components=5, vectorized_corpus=vectorized_comedy_corpus)[0]
