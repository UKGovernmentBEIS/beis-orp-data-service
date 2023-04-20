import os
import re
import nltk
import boto3
from preprocess.preprocess_functions import preprocess
from aws_lambda_powertools.logging.logger import Logger
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from postprocess.postprocess_functions import postprocess_title
from aws_lambda_powertools.utilities.typing import LambdaContext
from preprocess.preprocess_functions import removing_regulator_names
from search_metadata_title.get_title import identify_metadata_title_in_text


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
NLTK_DATA = os.environ['NLTK_DATA']

# Download models from local path
t5_tokenizer = AutoTokenizer.from_pretrained(
        './LLM/t5_tokenizer')
t5_model = AutoModelForSeq2SeqLM.from_pretrained(
        './LLM/t5_model')

os.makedirs(NLTK_DATA, exist_ok=True)
nltk.download('punkt', download_dir=NLTK_DATA)
nltk.download('stopwords', download_dir=NLTK_DATA)
nltk.download('wordnet', download_dir=NLTK_DATA)
nltk.download('omw-1.4', download_dir=NLTK_DATA)

def title_predictor(text: str, model, tokenizer) -> str:
    '''
    param: text: Str document text
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function to predict a title from the document text using a pretrained model
    '''

    # Preprocess the text
    text = preprocess(text)
    inputs = ['summarize: ' + text]
    inputs = tokenizer(inputs, truncation=True, return_tensors='pt')
    output = model.generate(**inputs, num_beams=10,
                            do_sample=False, min_length=10)
    decoded_output = tokenizer.batch_decode(
        output, skip_special_tokens=True)[0]
    predicted_title = nltk.sent_tokenize(decoded_output.strip())[0]

    # Postprocess the text
    processed_title = postprocess_title(predicted_title)
    return processed_title


def get_title(title: str,
              text: str,
              threshold: str) -> str:
    '''
    param: title: Str metadata title extracted from document
    param: text: Str document text
    param: threshold: int similarity score threshold
    returns: processed_title: Str cleaned predicted title from text from pretrained model
        Function that uses heuristics based on title length to either generate a title or
        use the metadata title
    '''
    junk = ['Microsoft Word - ', '.Doc', '.doc']

    # Remove junk
    for j in junk:
        title = re.sub(j, '', str(title))

    # Remove regulator names
    title = removing_regulator_names(title)

    # Remove excess whitespace
    title = re.sub(re.compile(r'\s+'), ' ', title)

    # Immediately filter out long metadata titles
    if (len(title.split(' ')) > 40):
        title = title_predictor(text, model=t5_model, tokenizer=t5_tokenizer)
        return title

    else:
        score = identify_metadata_title_in_text(title, text)
        logger.info(f"Metadata score: {score}")

        # If score is greater than 95% and title is less than / equal to 2 tokens
        length_of_no_punctuation_title = len(
            re.sub(r'[^\w\s]', ' ', title).split(' '))

        if score >= 95 and (length_of_no_punctuation_title <= 2):
            title = title_predictor(text, model=t5_model, tokenizer=t5_tokenizer)
            return title

        elif (score > threshold) and (length_of_no_punctuation_title >= 3):
            return title

        else:
            title = title_predictor(text, model=t5_model, tokenizer=t5_tokenizer)
            return title


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    text = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'

    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return text


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document']['document_uid']
    metadata_title = event['document']['title']

    # Download raw text
    s3_client = boto3.client('s3')
    text = download_text(s3_client=s3_client, document_uid=document_uid)

    title = get_title(title=metadata_title, text=text, threshold=85)
    logger.info(f'Document title is: {title}')

    handler_response = event
    handler_response['lambda'] = 'title_generation'
    handler_response['document']['title'] = title

    return handler_response
