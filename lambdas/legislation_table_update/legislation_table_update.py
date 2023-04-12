import os
import json
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import boto3
from SPARQLWrapper import SPARQLWrapper, CSV
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']
TABLE_NAME = os.environ['TABLE_NAME']
DESTINATION_BUCKET = 'beis-dev-datalake'
TABLE_NAME = 'legislative-origin'


def get_secret(secret_name):
    '''Retrieves a secret from AWS Secrets Manager'''

    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response['SecretString'])

    return secret_value


def query_tna(username, password, date_cursor):
    sparql = SPARQLWrapper("https://www.legislation.gov.uk/sparql")
    sparql.setCredentials(user=username, passwd=password)
    sparql.setReturnFormat(CSV)
    sparql.setQuery("""
                prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix xsd: <http://www.w3.org/2001/XMLSchema#>
                prefix void: <http://rdfs.org/ns/void#>
                prefix dct: <http://purl.org/dc/terms/>
                prefix sd: <http://www.w3.org/ns/sparql-service-description#>
                prefix prov: <http://www.w3.org/ns/prov#>
                prefix leg: <http://www.legislation.gov.uk/def/legislation/>
                select distinct ?ref ?title ?href ?shorttitle ?citation ?acronymcitation ?year ?number
                where {
                   ?activity prov:endedAtTime ?actTime .
                   ?graph prov:wasInfluencedBy ?activity .
                   ?activity rdf:type <http://www.legislation.gov.uk/def/provenance/Addition> .
                   ?dataUnitDataSet sd:namedGraph ?graph .
                   <http://www.legislation.gov.uk/id/dataset/topic/core> void:subset ?dataUnitDataSet .
                   graph ?graph { ?ref a leg:Legislation;
                                        leg:title ?title ;
                                        leg:year ?year ;
                                        leg:interpretation ?href .
                                   OPTIONAL {?ref   leg:citation ?citation  } .
                                   OPTIONAL {?ref   leg:acronymCitation ?acronymcitation} .
                                   OPTIONAL {?href  leg:shortTitle ?shorttitle} .
                                   OPTIONAL {?ref   leg:number ?number  } .}
                   FILTER(str(?actTime) > "%s")
                }
                """ % date_cursor)

    results = sparql.query().convert()
    df = pd.read_csv(BytesIO(results))
    return df


def transform_results(df):
    df['divAbbv'] = df.ref.apply(lambda x: x.split('/')[4])
    candidate_titles = ['title', 'shorttitle', 'citation', 'acronymcitation']

    # Filters out non-NaN values from the candidate_titles
    def to_list(x): return list(filter(lambda y: pd.notna(y), x))
    df['candidate_titles'] = df[candidate_titles].apply(to_list, axis=1)

    LEG_DIV = './leg-division-list.csv'
    df_leg_type = pd.read_csv(LEG_DIV)
    df = df.merge(df_leg_type[['legDivision', 'legType', 'divAbbv']], how='left')
    df = df.explode('candidate_titles')
    df = df.drop_duplicates('candidate_titles')

    return df


def save_to_s3(df):
    curr_dt = datetime.now().strftime('%Y_%m_%d')
    s3_filename = f'legislation_data_{curr_dt}.csv'
    s3_key = f'legislative-origin/{s3_filename}'

    csv_buffer = pd.DataFrame.to_csv(df, index=False)
    s3 = boto3.client('s3')
    response = s3.put_object(Bucket=DESTINATION_BUCKET, Key=s3_key, Body=csv_buffer)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception('Failed to save CSV to S3')


def insert_results(df):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    for index, row in df.iterrows():
        item = row.dropna().to_dict()
        item['number'] = str(item['number'])
        item['year'] = str(item['year'])
        response = table.put_item(Item=item)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Failed to insert item into DynamoDB: {item}')
    return df.shape[0]


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    date_cursor = datetime.now() - timedelta(days=7)
    date_cursor_str = date_cursor.strftime('%Y-%m-%dT%H:%M:%S')

    credentials = get_secret(secret_name='tna_credentials')
    username = credentials['tna_username']
    password = credentials['tna_password']

    df = query_tna(username=username, password=password, date_cursor=date_cursor_str)
    df = transform_results(df=df)
    save_to_s3(df=df)
    rows_inserted = insert_results(df=df)

    return f'Inserted {rows_inserted} into DynamoDB'
