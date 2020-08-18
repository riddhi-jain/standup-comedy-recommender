import pandas as pd
from nlp_pipeline import TranscriptProcessingPipeline
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import nltk
from sklearn.decomposition import NMF, TruncatedSVD, LatentDirichletAllocation
from topic_modeling import get_topics


# user defined search
search_term = 'current events'

# make and fit pipeline
raw_transcripts = pd.read_pickle('data/raw_standup_comedy_transcripts.pkl')
comedy_corpus = raw_transcripts['Text'].tolist()
pipeline = TranscriptProcessingPipeline(
    tokenizer=nltk.word_tokenize,
    stemmer=nltk.stem.PorterStemmer,
    lemmatizer=nltk.stem.WordNetLemmatizer,
    vectorizer=CountVectorizer
)
vectorized_comedy_corpus = pipeline.fit_transform(comedy_corpus)

# transform search term
search_transformed = pipeline.transform([search_term])

# fit model and do dimensionality reduction on search term
topics, nmf = get_topics(model=NMF, n_components=5, vectorized_corpus=vectorized_comedy_corpus)
search_topics = nmf.transform(search_transformed)
