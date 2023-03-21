import os
import boto3
from boto3.dynamodb.conditions import Key
import spacy
from spacy.matcher import Matcher, PhraseMatcher
from spacy.language import Language
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
TABLE_NAME = 'legislative_origin'
# TABLE_NAME = os.environ['TABLE_NAME']
YEAR_INDEX_NAME = os.environ['YEAR_INDEX_NAME']


@Language.component('custom_sentencizer')
def custom_sentencizer(doc):
    ''' Look for sentence start tokens by scanning for periods only. '''
    for i, token in enumerate(doc[:-2]):  # The last token cannot start a sentence
        if token.text == ".":
            doc[i + 1].is_sent_start = True
        else:
            # Tell the default sentencizer to ignore this token
            doc[i + 1].is_sent_start = False
    return doc


def NLPsetup():
    nlp = spacy.load(
        "en_core_web_sm",
        exclude=[
            'tok2vec',
            'senter',
            'attribute_ruler',
            'lemmatizer',
            'ner'])
    nlp.max_length = 500000
    nlp.add_pipe('custom_sentencizer', before="parser")
    return nlp


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')
    logger.info('Downloaded text')

    return document


def detect_year_span(docobj, nlp):
    pattern = [{"SHAPE": "dddd"}]
    dmatcher = Matcher(nlp.vocab)
    dmatcher.add('date matcher', [pattern])
    dm = dmatcher(docobj)
    dates = [docobj[start:end].text for _, start, end in dm]
    dates = set([int(d) for d in dates if (len(d) == 4) & (d.isdigit())])
    return dates


def query_titles_from_years(table, index_name, dates):
    all_titles = {}
    for date in dates:
        date = str(date)
        response = table.query(
            KeyConditionExpression=Key('year').eq(date),
            IndexName=index_name
        )

        candidate_titles = [i['candidate_titles'] for i in response['Items']]

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('year').eq(date),
                IndexName=index_name,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )

            candidate_titles.extend([i['candidate_titles']
                                    for i in response['Items']])

        all_titles[date] = candidate_titles

    return all_titles


def exact_matcher(title, docobj, nlp):
    phrase_matcher = PhraseMatcher(nlp.vocab)
    phrase_list = [nlp(title)]
    phrase_matcher.add("Text Extractor", None, *phrase_list)

    matched_items = phrase_matcher(docobj)

    matched_text = []
    for _, start, end in matched_items:
        span = docobj[start: end]
        matched_text.append((span.text, start, end, 100))
    return matched_text


def lookup_pipe(titles, docobj, nlp, method):
    results = []
    # for every legislation title in the table
    for title in nlp.pipe(titles):
        # detect legislation in the judgement body
        matches = method(title.text, docobj, nlp)
        if matches:
            results.append(title.text)
    return results


def extract_legislative_origins(table, title_list):
    # Query the table for the item with the specified key
    for title in title_list:
        response = table.get_item(
            Key={
                'candidate_titles': title
            }
        )
        item = response['Item']

        legislative_origin = {
            "url": item["href"],
            "ref": item["ref"],
            "title": item["title"],
            "number": item["number"],
            "type": item["legType"],
            "division": item["legDivision"]
        }
        yield legislative_origin


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document']['document_uid']

    s3_client = boto3.client('s3')
    raw_text = download_text(s3_client=s3_client, document_uid=document_uid)

    nlp = NLPsetup()
    nlp_text = nlp(raw_text)

    dates_in_text = detect_year_span(nlp_text, nlp)

    # Set up the DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
    table = dynamodb.Table(TABLE_NAME)

    titles = query_titles_from_years(
        table=table,
        index_name=YEAR_INDEX_NAME,
        dates=dates_in_text)

    sentences = list(nlp_text.sents)
    for sentence in sentences:
        years_in_sentence = [
            year for year in dates_in_text if str(year) in sentence.text]
        relevant_titles = []
        for year in years_in_sentence:
            relevant_titles.extend(titles[str(year)])

        results = lookup_pipe(relevant_titles, nlp_text, nlp, exact_matcher)
        if results:
            break

    legislative_origins = extract_legislative_origins(table, results)

    handler_response = event
    handler_response['lambda'] = 'legislative_origin_extraction'
    handler_response['document']['legislative_origins'] = [
        *legislative_origins]

    return handler_response
