"""
Contains functions for cleaning, formatting, and processing raw data acquired from running `data_acquisition.py`.
This includes all NLP text processing steps.

Stephen Kaplan, 2020-08-10
"""
import re
import string
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')


class TranscriptProcessingPipeline:
    """
    Natural Language Processing pipeline for cleaning, tokenizing, vectorizing, and transforming stand-up comedy
    transcripts.
    """
    def remove_stop_words(self, document):
        """

        :param document:
        :return:
        """
        words = document.split()
        words_without_stops = [word for word in words if word not in self.stop_words]

        return ' '.join(words_without_stops)

    def clean_document(self, document):
        """

        :param document:
        :return:
        """
        # first pass of removing common english words and profanity
        cleaned_document = self.remove_stop_words(document)

        # clean unwanted words and characters
        cleaned_document = cleaned_document.lower()                                     # lower case
        cleaned_document = re.sub(r'\[.*?\]', '', cleaned_document)            # text between brackets (meta-notes)
        cleaned_document = re.sub(r'\(.*?\)', '', cleaned_document)            # text between parenthesis
        cleaned_document = re.sub(r'[%s]' % re.escape(string.punctuation), ' ', cleaned_document)   # punctuation
        cleaned_document = re.sub(r'\w*\d\w*', '', cleaned_document)           # words with numbers
        cleaned_document = re.sub(r'\♪.*?\♪', '', cleaned_document)                                 # music
        cleaned_document = re.sub('[“”…]', '', cleaned_document)                                    # quotes
        cleaned_document = re.sub(r'\–', '', cleaned_document)                                      # hyphens
        cleaned_document = re.sub('\n', '', cleaned_document)                                       # line breaks
        cleaned_document = re.sub(r'\w*((\w)\2{2,})\w*', '', cleaned_document)  # remove crazy expressions like "aaahhh"

        # second pass of stop words and return
        return self.remove_stop_words(cleaned_document)

    def clean_corpus(self, corpus):
        """

        :param list corpus:
        :return:
        """
        return [self.clean_document(document) for document in corpus]

    def stem_document(self, document):
        """

        :param document:
        :return:
        """
        tokenized_document = document.split()
        stemmed_document = [self.stemmer.stem(word) for word in tokenized_document]

        return ' '.join(stemmed_document)

    def lemmatize_document(self, document):
        tokenized_document = document.split()
        lemmatized_document = [self.lemmatizer.lemmatize(word) for word in tokenized_document]
        return ' '.join(lemmatized_document)

    def stem_corpus(self, corpus):
        return [self.stem_document(document) for document in corpus]

    def lemmatize_corpus(self, corpus):
        return [self.lemmatize_document(document) for document in corpus]

    def _preprocess_data(self, corpus):
        # if a single string is provided, put it in a list
        corpus = corpus if isinstance(corpus, list) else list(corpus)
        cleaned_corpus = self.clean_corpus(corpus)
        #stemmed_corpus = self.stem_corpus(cleaned_corpus)
        lemmatized_corpus = self.lemmatize_corpus(cleaned_corpus)

        return lemmatized_corpus
        #return stemmed_corpus

    def fit_transform(self, corpus):
        preprocessed_corpus = self._preprocess_data(corpus)
        vectorized_corpus = self.vectorizer.fit_transform(preprocessed_corpus)
        self._is_fit = True

        return pd.DataFrame(vectorized_corpus.toarray(), columns=self.vectorizer.get_feature_names())

    def transform(self, corpus):
        if not self._is_fit:
            raise ValueError("Must fit the models before transforming!")

        preprocessed_corpus = self._preprocess_data(corpus)
        vectorized_corpus = self.vectorizer.transform(preprocessed_corpus)

        return pd.DataFrame(vectorized_corpus.toarray(), columns=self.vectorizer.get_feature_names())

    def __init__(self, tokenizer, stemmer, lemmatizer, vectorizer, stop_words=nltk.corpus.stopwords.words('english')):
        self.tokenizer = tokenizer
        self.stemmer = stemmer()
        self.lemmatizer = lemmatizer()
        self.vectorizer = vectorizer(stop_words='english', min_df=0.1, max_df=0.7) #ngram_range=(1, 2))
        self._is_fit = False

        self.stop_words = stop_words
        # have to append profanity to stop words, sorry!
        self.stop_words.extend(['motherfucking', 'motherfucker', 'fuck', 'fucked', 'fucking', 'nigger', 'nigga',
                                'cunt', 'hell', 'fuckin', "fuckin’", 'damn', 'shit', 'goddamn', 'bitch'])
        # add other random stop words that weren't captured
        self.stop_words.extend(['yo', 'um', "’em", 'wanna', "ain’t", 'ha', 'ok', 'ah', 'sort', 'quite',
                                'sir', 'literally', 'er', 'huh', 'dude'])


if __name__ == "__main__":
    raw_transcripts = pd.read_pickle('data/raw_standup_comedy_transcripts.pkl')
    comedy_corpus = raw_transcripts['Text'].tolist()
    pipeline = TranscriptProcessingPipeline(
        tokenizer=nltk.word_tokenize,
        stemmer=nltk.stem.PorterStemmer,
        lemmatizer=nltk.stem.WordNetLemmatizer,
        vectorizer=CountVectorizer#TfidfVectorizer
    )
    data = pipeline.fit_transform(comedy_corpus)
    data.to_pickle('data/count_vectorized_standup_comedy_transcripts.pkl')
