import os
import json
from io import BytesIO
import pandas as pd
import boto3
from SPARQLWrapper import SPARQLWrapper, CSV
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
TABLE_NAME = os.environ['TABLE_NAME']
YEAR_INDEX_NAME = os.environ['YEAR_INDEX_NAME']


def get_secret(secret_name):
    '''Retrieves a secret from AWS Secrets Manager'''

    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response['SecretString'])

    return secret_value


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    date = "2023-01-01T00:00:00"

    username = get_secret('tna_username')
    password = get_secret('tna_password')

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
                select distinct ?ref  ?title ?href ?shorttitle ?citation ?acronymcitation ?year ?number
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
                """ % date)

    results = sparql.query().convert()
    df = pd.read_csv(BytesIO(results))
    stitles = ['title', 'shorttitle', 'citation', 'acronymcitation']
    df['candidate_titles'] = df[stitles].apply(list, axis=1)
# ====
    df['divAbbv'] = df.ref.apply(lambda x: x.split('/')[4])
#     df = df.merge(dff[['legDivision', 'legType', 'divAbbv']], how='left')
# # ====
#     df = df.explode('candidate_titles')
#     df = df[~df['candidate_titles'].isna()].drop_duplicates('candidate_titles')
    # df.to_csv(savefile, index=None)
