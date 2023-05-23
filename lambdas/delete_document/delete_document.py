import os
# import json
# import boto3
from datetime import datetime
from typedb.client import TransactionType, SessionType, TypeDB
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


def match_delete(session, query):
    with session.transaction(TransactionType.WRITE) as transaction:
        logger.debug(f'Query:\n {query}')
        transaction.query().delete(query)
        transaction.commit()


def query_function(event, session):
    try:
        uid = event['uuid']
        regulator_id = event['regulator_id']
    except:
        return {
            "status_code": 400,
            "status_description": "Bad Request - Missing parameter(s)."
        }

    query = f'match $x isa entity, has document_uid "{uid}",'\
            f'has regulator_id "{regulator_id}";' \
            'delete $x isa entity;'
    match_delete(query=query, session=session)
    return {
            "status_code": 200,
            "status_description": "OK"
        }


def validate_env_variable(env_var_name):
    logger.debug(
        f"Getting the value of the environment variable: {env_var_name}")
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f"Please, set environment variable {env_var_name}")
    if not env_variable:
        raise Exception(f"Please, provide environment variable {env_var_name}")
    return env_variable


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get the JSON payload from the POST request
    payload = event

    logger.info(f'Received event with a payload: {payload}')
    # payload['time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')

    client = TypeDB.core_client(TYPEDB_IP + ':' + TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)
    return query_function(session=session, event=payload)
