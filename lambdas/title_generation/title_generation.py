import os
import re
import nltk
import boto3
import pymongo
from http import HTTPStatus
from preprocess.preprocess_functions import preprocess
from aws_lambda_powertools.logging.logger import Logger
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from postprocess.postprocess_functions import postprocess_title
from aws_lambda_powertools.utilities.typing import LambdaContext
from preprocess.preprocess_functions import removing_regulator_names
from search_metadata_title.get_title import identify_metadata_title_in_text


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
MODEL_PATH = os.environ['MODEL_PATH']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
NLTK_DATA = os.environ['NLTK_DATA']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'

os.makedirs(NLTK_DATA, exist_ok=True)
nltk.download('wordnet', download_dir=NLTK_DATA)
nltk.download('omw-1.4', download_dir=NLTK_DATA)
nltk.download('punkt', download_dir=NLTK_DATA)
nltk.download('stopwords', download_dir=NLTK_DATA)


def title_predictor(text: str) -> str:
    """
    param: text: Str document text
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function to predict a title from the document text using a pretrained model
    """

    tokenizer = AutoTokenizer.from_pretrained(
        "fabiochiu/t5-small-medium-title-generation")
    model = AutoModelForSeq2SeqLM.from_pretrained(
        "fabiochiu/t5-small-medium-title-generation")

    # Preprocess the text
    text = preprocess(text)
    inputs = ["summarize: " + text]
    inputs = tokenizer(inputs, truncation=True, return_tensors="pt")
    output = model.generate(**inputs, num_beams=10,
                            do_sample=False, min_length=10, max_new_tokens=25)
    decoded_output = tokenizer.batch_decode(
        output, skip_special_tokens=True)[0]
    predicted_title = nltk.sent_tokenize(decoded_output.strip())[0]

    # Postprocess the text
    processed_title = postprocess_title(predicted_title)
    return processed_title


def get_title(title: str,
              text: str,
              threshold: str) -> str:
    """
    param: title: Str metadata title extracted from document
    param: text: Str document text
    param: threshold: int similarity score threshold
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function that uses heuristics based on title length to either generate a title or
        use the metadata title
    """
    junk = ["Microsoft Word - ", ".Doc", ".doc"]

    # Remove junk
    for j in junk:
        title = re.sub(j, "", str(title))

    # Remove regulator names
    title = removing_regulator_names(title)

    # Remove excess whitespace
    title = re.sub(re.compile(r'\s+'), " ", title)

    # Immediately filter out long metadata titles
    if (len(title.split(" ")) > 40):
        title = title_predictor(text)
        return title

    else:
        score = identify_metadata_title_in_text(title, text)

        # If score is greater than 95% and title is less than / equal to 2 tokens
        length_of_no_punctuation_title = len(
            re.sub(r'[^\w\s]', ' ', title).split(" "))

        if score >= 95 and (length_of_no_punctuation_title <= 2):
            title = title_predictor(text)
            return title

        elif (score > threshold) and (length_of_no_punctuation_title >= 3):
            return title

        else:
            title = title_predictor(text)
            return title


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    text = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'

    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return text


def mongo_connect_and_push(document_uid,
                           title,
                           database=ddb_connection_uri,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the title to it'''

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
                                   '$set': {'title': title}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get document id
    document_uid = event['document_uid']
    metadata_title = event['title']

    # Download raw text
    s3_client = boto3.client('s3')
    text = download_text(s3_client=s3_client, document_uid=document_uid)

    title = get_title(title=metadata_title, text=text, threshold=85)
    logger.info(f"Document title is: {title}")

    response = mongo_connect_and_push(document_uid=document_uid, title=title)
    response['document_uid'] = document_uid

    return response
