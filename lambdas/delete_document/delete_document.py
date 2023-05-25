import os
import json
from typedb.client import TransactionType, SessionType, TypeDB
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


def getUniqueResult(results):
    res = [(i.get_type().get_label().name(), i.get_value())
           for a in results for i in a.concepts() if i.is_attribute()]
    return res

def match_query(query, session, group=True):
    with session.transaction(TransactionType.READ) as transaction:
        print("Query:\n %s" % query)
        iterator = transaction.query().match_group(
            query) if group else transaction.query().match(query)
        results = [ans for ans in iterator]
        return results
    
def match_delete(session, query):
    with session.transaction(TransactionType.WRITE) as transaction:
        logger.info(f'Query:\n {query}')
        transaction.query().delete(query)
        transaction.commit()

def get_query(uid, regulator_id, session):
    query = f'match $x isa entity, has document_uid "{uid}",'\
            f'has regulator_id "{regulator_id}",' \
            'has document_format $df, has URI $uri;'
    
    ans = match_query(query=query, session=session, group=False)
    results = dict(getUniqueResult(ans))
    return results

def delete_query(uid, regulator_id, session):
    query = f'match $x isa entity, has document_uid "{uid}",'\
            f'has regulator_id "{regulator_id}";' \
            'delete $x isa entity;'
    match_delete(query=query, session=session)
    logger.info('Finished deletion query')
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
    payload = json.loads(event['body'])

    logger.info(f'Received event with a payload: {payload}')

    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')

    client = TypeDB.core_client(TYPEDB_IP + ':' + TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)

    uid = payload['uuid']
    regulator_id = payload['regulator_id']

    attrs = get_query(uid, regulator_id, session)
    logger.info(f'Metadata of document about to be deleted: {attrs}')
    qstatus = delete_query(session=session, event=payload)
    qstatus['metadata'] = attrs
    return qstatus
