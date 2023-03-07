import io
import os
import re
import boto3
import wordninja
import torch
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from collections import defaultdict
from smart_open import open as smart_open
from word_forms_loc.lemmatizer import lemmatize
from sklearn.feature_extraction.text import CountVectorizer
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA = os.environ['NLTK_DATA']
MODEL_PATH = os.environ['MODEL_PATH']


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')
    logger.info('Downloaded text')

    return document


def download_model(s3_client,
                   bucket=MODEL_BUCKET,
                   model_path=MODEL_PATH,
                   key='keybert.pt'):
    '''Downloads the ML model for keyword extraction'''

    s3_client.download_file(
        bucket,
        key,
        os.path.join(model_path, key)
    )
    with smart_open(os.path.join(model_path, key), 'rb') as f:
        buffer = io.BytesIO(f.read())
        model = torch.load(buffer)
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


def extract_keywords(text, kw_model):
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
        top_n=15
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

    document_uid = event['document']['document_uid']

    logger.info("Started initialisation...")
    os.makedirs(MODEL_PATH, exist_ok=True)
    os.makedirs(NLTK_DATA, exist_ok=True)
    nltk.download('wordnet', download_dir=NLTK_DATA)
    nltk.download('omw-1.4', download_dir=NLTK_DATA)
    nltk.download('punkt', download_dir=NLTK_DATA)

    s3_client = boto3.client('s3')
    document = download_text(s3_client=s3_client, document_uid=document_uid)
    kw_model = download_model(s3_client=s3_client)
    keywords = extract_keywords(text=document, kw_model=kw_model)
    keywords = get_relevant_keywords(x=keywords)
    logger.info({'relevant keywords': keywords})

    subject_keywords = [i[0] for i in keywords]

    handler_response = event
    handler_response['document']['subject_keywords'] = subject_keywords

    return handler_response
