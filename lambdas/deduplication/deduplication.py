import os
import json
import boto3
import logging
import numpy as np
from utils import getHash
from typedb.client import *
from numpy.linalg import norm
from typedb.client import TransactionType, SessionType, TypeDB


LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))

search_keys = {"id", "status", 
               "regulatory_topic", "document_type"}

return_vals = ['regulator_id',
               'document_type', 'status',
               'date_published', 'version']


def validate_env_variable(env_var_name):
    LOGGER.debug(
        f"Getting the value of the environment variable: {env_var_name}")
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f"Please, set environment variable {env_var_name}")
    if not env_variable:
        raise Exception(f"Please, provide environment variable {env_var_name}")
    return env_variable


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    text = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'

    )['Body'].read().decode('utf-8')

    LOGGER.info('Downloaded text')

    return text


def create_hash_list(text):
    """
    param: text: Str
    returns: hash_list: list of hashes
    """
    hash_np = getHash(text)
    hash_list = hash_np.tolist()
    return hash_np, hash_list


def read_transaction(session, hash_list):

    ## Read the person using a READ only transaction
    with session.transaction(TransactionType.READ) as read_transaction:

        contains = " or ".join(['{$h contains "%s";}'%hash for hash in hash_list])

        query = f"""
        match 
            $u isa legalDocument, 
            has node_id $n,
            has regulator_id $r, 
            # has document_type $d,
            # has status $s,
            # has date_published $dp,
            # has version $v,
            has hash_text $h;
            {contains}; 
        get $u, $n, $r, $h;
        """

        answer_iterator = read_transaction.query().match(query)

        # Get matches on hash
        ans_list = [ans for ans in answer_iterator]

        matching_hash_list = [np.array(getattr(ans.get("h"), "_value").split("_"), dtype="uint64") for ans in ans_list]

        metadata_dict = {"regulator_id" : [ans.get("r") for ans in ans_list],
                            "node_id" : [ans.get("n") for ans in ans_list]}

        LOGGER.info("Number of returned hashes: " + str(len(matching_hash_list)))

        return matching_hash_list, metadata_dict


def get_similarity_score(hash_np, matching_hash_list):
    scores = []
    indeces = []
    for i,v in enumerate(matching_hash_list):
        cosine = np.dot(hash_np,v)/(norm(hash_np)*norm(v))
        scores.append(cosine)
        indeces.append(i)

    if (len(scores) > 0) and (max(scores) >= 0.95):
        if max(scores) == 1:
            LOGGER.info("Duplicate text detected")
            index = indeces[scores.index(max(scores))]
            return index
        else:
            LOGGER.info("Possible version detected, no current process for versioning.")
            return None
    else:
        LOGGER.info("New document")
        return None


def detect_duplicate(index, incoming_metadata, existing_metadata, return_vals=return_vals):

    incoming_dict = {k:incoming_metadata[k] for k in return_vals if k in incoming_metadata.keys()}
    existing_dict = {k:getattr(existing_metadata[k][index], "_value") for k in return_vals if k in existing_metadata.keys()}

    if incoming_dict == existing_dict:
        LOGGER.info("Duplicate document with identical metadata - discard incoming document")
        return False
    else:
        LOGGER.info("Different metadata detected")
        return True

        
def search_module(event, session, text, incoming_metadata):
    keyset = set(event.keys()) & search_keys

    if len(keyset) == 0:
        return {
            "status_code": 400,
            "status_description": "Bad Request - Unsupported search parameter(s)."
        }

    else:

        hash_np, hash_list = create_hash_list(text)

        matching_hash_list, metadata_dict = read_transaction(session, hash_list)

        index = get_similarity_score(hash_np=hash_np, matching_hash_list=matching_hash_list)

        if index != None:
            if detect_duplicate(index=index, incoming_metadata = incoming_metadata, existing_metadata=metadata_dict):
                return {
                    "status_code": 200,
                    "status_description": "OK"
                }
            else:
                return


def lambda_handler(ev):
    LOGGER.info("Received event: " + json.dumps(ev, indent=2))

    event = json.loads(ev['body'])
    document_uid = event['document']['document_uid']

    LOGGER.info("Event Body: ", event)
    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')
    SOURCE_BUCKET = validate_env_variable('SOURCE_BUCKET')

    client = TypeDB.core_client(TYPEDB_IP + ':'+TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)

    s3_client = boto3.client('s3')
    text = download_text(s3_client=s3_client, document_uid=document_uid)

    # Get incoming metadata
    incoming_metadata = event['document']['data']['dates']['date_published']

    result = search_module(event, session, text, incoming_metadata)

    handler_response = event
    handler_response['lambda'] = 'date_generation'
    handler_response['document']['data']['dates']['date_published'] = date

    session.close()
    client.close()

    return result