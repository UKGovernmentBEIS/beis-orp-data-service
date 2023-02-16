import io
import os
import re
import pymongo
import boto3
import wordninja
import torch
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from collections import defaultdict
from http import HTTPStatus
from smart_open import open as smart_open
from word_forms_loc.lemmatizer import lemmatize
from sklearn.feature_extraction.text import CountVectorizer
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA = os.environ['NLTK_DATA']
MODEL_PATH = os.environ['MODEL_PATH']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


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
    text = BeautifulSoup(documents).get_text()
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
    # TODO: replace the hardcoded regs references
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
    try:
        return lemmatize(word)
    except ValueError as err:
        if 'is not a real word' in err.args[0]:
            return word
        else:
            raise ValueError(err)


def get_relevant_keywords(x):
    nounify = [(get_lemma(k), v) for k, v in x]
    kwds = defaultdict(list)
    for k, v in nounify:
        kwds[k].append(v)
    return [(k, max(v)) for k, v in kwds.items()][:10]


def mongo_connect_and_update(document_uid,
                             keywords,
                             database=DOCUMENT_DATABASE,
                             tlsCAFile='./rds-combined-ca-bundle.pem'):

    '''Connects to the DocumentDB, finds the document matching our UUID and adds the keywords to it'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info(db_client.list_database_names())
    db = db_client.bre_orp
    collection = db.documents

    # Insert document to DB
    logger.info({'document': collection.find_one(
        {'document_uid': document_uid})})
    collection.find_one_and_update({'document_uid': document_uid}, {
                                   '$set': {'subject_keywords': keywords}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document_uid']
    logger.append_keys(document_uid=document_uid)
    logger.info("Started initialisation...")
    os.makedirs(MODEL_PATH, exist_ok=True)
    

    s3_client = boto3.client('s3')
    document = download_text(s3_client, document_uid)
    kw_model = download_model(s3_client)
    keywords = extract_keywords(document, kw_model)
    # lemmatise keywords
    keywords = get_relevant_keywords(keywords)
    logger.info({'relevant keywords': keywords})
    
    subject_keywords = [i[0] for i in keywords]

    response = mongo_connect_and_update(document, subject_keywords)
    response['document_uid'] = document_uid

    return response
