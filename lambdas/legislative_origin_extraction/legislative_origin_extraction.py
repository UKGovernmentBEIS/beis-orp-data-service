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
CUTOFF = 0.2


@Language.component('custom_sentencizer')
def custom_sentencizer(doc):
    '''Look for sentence start tokens by scanning for periods only.'''
    for i, token in enumerate(doc[:-2]):  # The last token cannot start a sentence
        if token.text == ".":
            doc[i + 1].is_sent_start = True
        else:
            # Tell the default sentencizer to ignore this token
            doc[i + 1].is_sent_start = False
    return doc


def NLPsetup():
    '''Initialises the Spacy models'''
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


def detect_year_span(nlp_text, nlp):
    '''Detects mentions of years in the text'''
    pattern = [{"SHAPE": "dddd"}]
    dmatcher = Matcher(nlp.vocab)
    dmatcher.add('date matcher', [pattern])
    dm = dmatcher(nlp_text)
    dates = [nlp_text[start:end].text for _, start, end in dm]
    dates = set([int(d) for d in dates if (len(d) == 4) & (d.isdigit())])
    return dates


def query_titles_from_years(table, index_name, dates):
    '''
    Finds the titles of all legislation from the years detected in the text
    Queries a separate index which only holds the year and the title
    '''
    all_titles = {}
    for date in dates:
        date = str(date)
        response = table.query(
            KeyConditionExpression=Key('year').eq(date),
            IndexName=index_name
        )

        candidate_titles = [i['candidate_titles'] for i in response['Items']]
        # Paginating through the results as each results set must be <1MB
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


def exact_matcher(title, nlp_text, nlp):
    '''
    Finds instances of text within a larger piece of text
    Used for finding references to legislation titles in document
    '''
    phrase_matcher = PhraseMatcher(nlp.vocab)
    phrase_list = [nlp(title)]
    phrase_matcher.add("Text Extractor", None, *phrase_list)

    matched_items = phrase_matcher(nlp_text)

    matched_text = []
    for _, start, end in matched_items:
        span = nlp_text[start: end]
        matched_text.append((span.text, start, end, 100))
    return matched_text


def find_legislation_in_text(nlp_text, nlp, titles, dates_in_text):
    '''
    Finds mentions of legislation titles in text
    Returns the first set of results as this is indicative of the legislative origin
    '''
    sentences = list(nlp_text.sents)
    results = []
    for sentence in sentences:
        years_in_sentence = [
            year for year in dates_in_text if str(year) in sentence.text]
        relevant_titles = []
        for year in years_in_sentence:
            relevant_titles.extend(titles[str(year)])

        # for every legislation title in the table
        for title in nlp.pipe(relevant_titles):
            # detect legislation in the judgement body
            matches = exact_matcher(title.text, nlp_text, nlp)
            if matches:
                results.append(title.text)

        if results:
            break
    return results


def extract_legislative_origins(table, title_list):
    '''
    Query the table for the matched legislation in text
    Returns relevant metadata of the legislation and attaches it to document metadata
    '''
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

    # Fetches the raw text of the document matching the UID above
    s3_client = boto3.client('s3')
    raw_text = download_text(s3_client=s3_client, document_uid=document_uid)

    # Retrieving the first fifth of the text - this reduces the query space
    # and improves performance
    doc_cutoff_point = int(len(raw_text) * CUTOFF)
    top_text = raw_text[:doc_cutoff_point]

    # Intitialising model and text
    nlp = NLPsetup()
    nlp_text = nlp(top_text)

    # Find years mentioned in text
    dates_in_text = detect_year_span(nlp_text, nlp)

    # Set up the DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
    table = dynamodb.Table(TABLE_NAME)

    # Querying table for all titles in matched years
    titles = query_titles_from_years(
        table=table,
        index_name=YEAR_INDEX_NAME,
        dates=dates_in_text
    )

    # Finding legislation referenced in text
    legislative_origins = find_legislation_in_text(
        nlp_text=nlp_text,
        nlp=nlp,
        titles=titles,
        dates_in_text=dates_in_text
    )

    # Querying table for metadata of referenced legislation
    legislative_origins_metadata = extract_legislative_origins(
        table, legislative_origins)

    # Unpacking and deduping the output of the above function
    unpacked_legislative_origins_metadata = [*legislative_origins_metadata]
    deduped_legislative_origins_metadata = list(
        {frozenset(d.items()): d for d in unpacked_legislative_origins_metadata}.values())

    handler_response = event
    handler_response['lambda'] = 'legislative_origin_extraction'
    handler_response['document']['data']['legislative_origins'] = deduped_legislative_origins_metadata

    return handler_response
