import os
import json
import boto3
import logging
import numpy as np
from email import get_email_address, send_email
from utils import getHash
from typedb.client import *
from numpy.linalg import norm
from typedb.client import TransactionType, SessionType, TypeDB


LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))

search_keys = {"id", "status", 
               "regulatory_topic", "document_type"}

return_vals = ["regulatory_topic", 'document_type', 'status']


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


def download_text(s3_client, document_uid, bucket):
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
    """
    params: session: TypeDB session opened
    params: hash_list: list of integers in hash list
        returns: matching_hash_list: list of hashes from the database that matched an integer in the hash_list
        metadata_dict: dictionary of the metadata of all the shortlisted documents
    """

    ## Read the person using a READ only transaction
    with session.transaction(TransactionType.READ) as read_transaction:

        contains = " or ".join(['{$h contains "%s";}'%hash for hash in hash_list])

        query = f"""
        match 
            $u isa legalDocument, 
            has node_id $n,
            has document_type $d,
            has status $s,
            has regulatory_topic $rt,
            has regulator_id $r,
            has date_published $dp,
            has title $t,
            has document_uid $uuid,
            has hash_text $h;
            {contains}; 
        """

        answer_iterator = read_transaction.query().match(query)

        # Get matches on hash
        ans_list = [ans for ans in answer_iterator]

        matching_hash_list = [np.array(getattr(ans.get("h"), "_value").split("_"), dtype="uint64") for ans in ans_list]

        metadata_dict = {"node_id" : [ans.get("n") for ans in ans_list],
                        "title" : [ans.get("t") for ans in ans_list],
                        "status" : [ans.get("s") for ans in ans_list],
                        "regulator_id" : [ans.get("r") for ans in ans_list],
                        "document_type" : [ans.get("dt") for ans in ans_list],
                        "regulatory_topic" : [ans.get("rt") for ans in ans_list],
                        "date_published" : [ans.get("dp") for ans in ans_list],
                        "document_uid" : [ans.get("uuid") for ans in ans_list]}

        LOGGER.info("Number of returned hashes: " + str(len(matching_hash_list)))

        return matching_hash_list, metadata_dict


def get_similarity_score(hash_np, matching_hash_list):
    """
    params: hash_np: numpy hash of the incoming document
    params: matching_hash_list: list of hashes from the database that matched an integer in the hash_list
    returns: index or None: index is returned if an exact duplicate is found, otherwise None is returned
    """
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


def is_duplicate(index, incoming_metadata, complete_existing_metadata, return_vals=return_vals):
    """
    params: index int:
    params: incoming_metadata dict:
    params: complete_existing_metadata dict:
    params: return_vals List: 
        returns: Boolean, Dictionary: (True / False) if (is / is not) a duplicate. 
        complete_existing_metadata: All associated metadata so that the user can be given details of the existing document
        existing_dict: A smaller selection of the metadata to be replaced if document is found to be a version
    """

    incoming_dict = {k:incoming_metadata[k] for k in return_vals if k in incoming_metadata.keys()}
    existing_dict = {k:getattr(complete_existing_metadata[k][index], "_value") for k in return_vals if k in complete_existing_metadata.keys()}

    if incoming_dict == existing_dict:
        LOGGER.info("Duplicate document with identical metadata - discard incoming document")
        return True, complete_existing_metadata
    else:
        LOGGER.info("Different metadata detected")
        return False, existing_dict

        
def search_module(event, session, text, incoming_metadata):
    keyset = set(event.keys()) & search_keys

    if len(keyset) == 0:
        return {
            "status_code": 400,
            "status_description": "Bad Request - Unsupported search parameter(s)."
        }

    else:
        hash_np, hash_list = create_hash_list(text)
        matching_hash_list, complete_existing_metadata = read_transaction(session, hash_list)
        index = get_similarity_score(hash_np=hash_np, matching_hash_list=matching_hash_list)

        # 1. If index != None, i.e. text with similarity > 0.95 found
        # 2. Test for exact duplicates and return the results
        # 3. Otherwise return false

        if index != None:
            is_duplicate_results = is_duplicate(index=index, incoming_metadata = incoming_metadata, 
                                            complete_existing_metadata=complete_existing_metadata)
            return is_duplicate_results

        # No index returned, hence there are no similar documents
        else:
            return False


def handler(ev):
    LOGGER.info("Received event: " + json.dumps(ev, indent=2))

    event = json.loads(ev['body'])
    document_uid = event['document']['document_uid']

    LOGGER.info("Event Body: ", event)
    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    SOURCE_BUCKET = validate_env_variable('SOURCE_BUCKET')   
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    COGNITO_USER_POOL = validate_env_variable('COGNITO_USER_POOL')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')
    SENDER_EMAIL_ADDRESS = validate_env_variable('SENDER_EMAIL_ADDRESS')


    client = TypeDB.core_client(TYPEDB_IP + ':'+TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)

    s3_client = boto3.client('s3')
    text = download_text(s3_client=s3_client, document_uid=document_uid, bucket=SOURCE_BUCKET)

    # Get incoming metadata
    incoming_metadata = dict(zip(return_vals, [event['document']['data'][idx] for idx in return_vals]))

    # If search module returns a True i.e. duplicate text with different metadata, then replace existing metadata
    # The returned dictionary is the existing document's metadata
    is_duplicate_results = search_module(event, session, text, incoming_metadata)

    # Close the session
    session.close()
    client.close()

    #========== 1. If it is not a duplicate, pass the document through as usual ========
    if is_duplicate_results == False:
        handler_response = event
        return handler_response

    #========== 2. If the document is a version (same text different metadata), update the metadata ========
    elif is_duplicate_results[0] == False:
        handler_response = event
        handler_response['lambda'] = 'deduplication'
        for i in range(0, len(incoming_metadata)):
            handler_response['document']['data'][*incoming_metadata][i] = [*incoming_metadata.values()][i]
            handler_response = handler_response
        return handler_response

    #========== 3. Else the document is a complete duplicate, and the user should be informed ========
    else:
        # Get the existing metadata of the matching document
        complete_existing_metadata = is_duplicate_results[1]

        email_address = get_email_address(COGNITO_USER_POOL, user_id)
        LOGGER.info(f'Pulled email from Cognito: {email_address}')

        if email_address:
            user_id = complete_existing_metadata['user_id']
            document_uid = complete_existing_metadata['document_uid']
            title = complete_existing_metadata['title']
            document_type = complete_existing_metadata['document_type']
            regulator_id = complete_existing_metadata['regulator_id']
            date_published = complete_existing_metadata['date_published']

            send_email(
                sender_email=SENDER_EMAIL_ADDRESS,
                recipient_email=email_address,
                subject='ORP Upload Rejected',
                body=f'''Your document (UUID: {document_uid}) has been flagged as a duplicate. 
                    The existing document can be viewed in the ORP at https://app.{ENVIRONMENT}.cannonband.com/document/view/{document_uid}?ingested=true
                    It is currently searchable.\n
                    You can search using the following criteria:\n
                    - Title: {title}\n
                    - Document Type: {document_type}\n
                    - Regulator: {regulator_id}\n
                    - Date Published: {date_published}\n
                    This is a system generated email, please do not reply.'''
            )

    return handler_response

