import io
import os
import re
import wordninja
import torch
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from collections import defaultdict
from smart_open import open as smart_open
from word_forms.lemmatizer import lemmatize
from sklearn.feature_extraction.text import CountVectorizer
from bs4 import BeautifulSoup


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bulk_processing")


KW_MODEL=  'keyword_extraction/model_store'

def download_model(model_path,
                   key='keybert.pt'):
    '''Downloads the ML model for keyword extraction'''
    with smart_open(os.path.join(os.path.realpath(model_path), key), 'rb') as f:
        buffer = io.BytesIO(f.read())
        model = torch.load(buffer)

    logger.debug('Downloaded model')

    return model


def pre_process_tokenization_function(documents: str):
    '''Pre-processes the text ready for keyword extraction'''

    # Preprocess data after embeddings are created
    text = BeautifulSoup(documents, features='html.parser').get_text()
    text = re.sub('[^a-zA-Z]', ' ', text)

    # Define stopwords
    stopwords = open('keyword_extraction/stopwords.txt', 'r')
    stopwords = stopwords.read()
    stopwords = [i for i in stopwords.split('\n')]
    stopwords.extend(['use', 'uses', 'used', 'www', 'gov',
                      'uk', 'guidance', 'pubns', 'page'])
    remove_stop_words = set(stopwords)

    text = text.lower()

    # Tokenize
    word_tokens = word_tokenize(text)
    filtered_sentence = []
    for w in word_tokens:
        if w not in remove_stop_words:
            filtered_sentence.append(w)

    # Remove any small characters remaining
    filtered_sentence = [word for word in filtered_sentence if len(word) > 1]

    # Lemmatise text
    wnl = WordNetLemmatizer()
    lemmatised_sentence = [wnl.lemmatize(word) for word in filtered_sentence]

    return lemmatised_sentence


def extract_keywords(text, kw_model):
    # TODO: replace the hardcoded regs references
    '''Extracts the keywords from the downloaded text using the downloaded model'''
    p = '|'.join(['Health and Safety Executive', 'Ofgem', 'Environmental Agency'])
    text = re.sub(p, '', text)
    text = ' '.join(wordninja.split(text))

    # Vectorizer: Prevents noise and improves representation of clusters
    vectorizer_model = CountVectorizer(
        stop_words='english',
        tokenizer=pre_process_tokenization_function
    )

    keywords = kw_model.extract_keywords(
        text,
        vectorizer=vectorizer_model,
        top_n=15
    )
    logger.debug({'keywords': keywords})

    return keywords


def get_lemma(word):
    # TODO: Docstring
    try:
        return lemmatize(word)
    except ValueError as err:
        if 'is not a real word' in err.args[0]:
            return word
        else:
            raise ValueError(err)


def get_relevant_keywords(x):
    # TODO: Docstring and name variables
    nounify = [(get_lemma(k), v) for k, v in x]
    kwds = defaultdict(list)
    for k, v in nounify:
        kwds[k].append(v)
    return [(k, max(v)) for k, v in kwds.items()][:10]


def keyword_extraction(document, title):
    # nltk.data.path.append('./nltk_data')
    logger.debug("Started initialisation...")
    kw_model = download_model(KW_MODEL)
    keywords = extract_keywords(text=document, kw_model=kw_model)
    try:
        tkeyw = extract_keywords(text=title, kw_model=kw_model)
    except:
        tkeyw = []
        
    # lemmatise keywords
    keywords = get_relevant_keywords(x=keywords+tkeyw)
    logger.debug({'relevant keywords': keywords})

    subject_keywords = [i[0] for i in keywords]

    return subject_keywords
