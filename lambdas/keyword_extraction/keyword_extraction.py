import re
import wordninja
import torch
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from collections import defaultdict
from word_forms_loc.lemmatizer import lemmatize
from sklearn.feature_extraction.text import CountVectorizer
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


def load_model(
        key='keybert.pt'):
    '''Downloads the ML model for keyword extraction'''

    model = torch.load(f"./LLM/{key}")
    logger.info('Downloaded model')

    return model


def pre_process_tokenization_function(documents: str):
    '''Pre-processes the text ready for keyword extraction'''

    # Preprocess data after embeddings are created
    text = BeautifulSoup(documents, features='html.parser').get_text()
    text = re.sub('[^a-zA-Z]', ' ', text)

    # Define stopwords
    stopwords = open('./stopwords.txt', 'r')
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


def extract_keywords(text, kw_model, n=15):
    # TODO: replace the hardcoded regulator references
    '''Extracts the keywords from the downloaded text using the downloaded model'''

    text = re.sub('Health and Safety Executive', '', text)
    text = re.sub('Ofgem', '', text)
    text = re.sub('Environmental Agency', '', text)
    text = ' '.join(wordninja.split(text))

    # Vectorizer: Prevents noise and improves representation of clusters
    vectorizer_model = CountVectorizer(
        stop_words='english',
        tokenizer=pre_process_tokenization_function
    )
    keywords = kw_model.extract_keywords(
        text,
        vectorizer=vectorizer_model,
        top_n=n
    )
    logger.info({'keywords': keywords})

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


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    title = event['title']
    text = event['text']

    logger.info('Started initialisation...')

    kw_model = load_model()
    title_keywords = extract_keywords(text=title, kw_model=kw_model, n=2)
    doc_keywords = extract_keywords(text=text, kw_model=kw_model)
    # Combine keywords
    keywords = list(set(title_keywords + doc_keywords))
    keywords = get_relevant_keywords(x=keywords)

    logger.info({'title keywords': title_keywords})
    logger.info({'doc and title keywords': keywords})

    subject_keywords = [i[0] for i in keywords]

    return {'keywords': subject_keywords,
            'title': title}
