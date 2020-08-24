"""
Script for cleaning, formatting, and processing raw data acquired from running `data_acquisition.py`. It then saves the
transformed data to a .pkl file as well as persists the TranscriptProcessingPipeline instance as a .pkl to be used to
transform future text data.

Stephen Kaplan, 2020-08-10
"""
import nltk
import dill as pickle
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

from app.db import connect_to_mongo, load_mongo_collection_as_dataframe
from app.nlp_pipeline import TranscriptProcessingPipeline
from app.creds import USERNAME, PWD


if __name__ == '__main__':
    db = connect_to_mongo(username=USERNAME, password=PWD)
    df_transcripts = load_mongo_collection_as_dataframe(db, collection_name='transcripts')

    pipeline_cv = TranscriptProcessingPipeline(
        tokenizer=nltk.word_tokenize,
        stemmer=nltk.stem.PorterStemmer,
        lemmatizer=nltk.stem.WordNetLemmatizer,
        vectorizer=CountVectorizer
    )
    data_cv = pipeline_cv.fit_transform(df_transcripts['text'].to_list())
    data_cv.to_pickle('data/count_vectorized_standup_comedy_transcripts.pkl')

    pipeline_tfidf = TranscriptProcessingPipeline(
        tokenizer=nltk.word_tokenize,
        stemmer=nltk.stem.PorterStemmer,
        lemmatizer=nltk.stem.WordNetLemmatizer,
        vectorizer=TfidfVectorizer
    )
    data_tfidf = pipeline_tfidf.fit_transform(df_transcripts['text'].to_list())
    data_tfidf.to_pickle('data/tfidf_standup_comedy_transcripts.pkl')

    pickle.dump(pipeline_tfidf, open('../app/static/ml_models/tfidf_pipeline.pkl', 'wb'))
