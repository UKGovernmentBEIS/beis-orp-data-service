import os
import boto3
import numpy as np
from numpy.linalg import norm
from botocore.client import Config
from utils import create_hash_list
from notification_email import send_email
from pandas import DataFrame
from bs4 import BeautifulSoup
from typedb.client import TransactionType, SessionType, TypeDB
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging.logger import Logger


logger = Logger()
search_keys = {'document_uid', 'status',
               'regulatory_topic', 'document_type', 'node_id'}
return_vals = ['regulatory_topic', 'document_type', 'status']
SIMILARITY_SCORE_CUTOFF = 0.95


def validate_env_variable(env_var_name):
    '''Check existence of environment variables'''
    logger.debug(
        f'Getting the value of the environment variable: {env_var_name}')
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f'Please, set environment variable {env_var_name}')
    return env_variable


def download_document(s3_client, document_uid, bucket):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.orpml'
    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return document


def extract_metadata(document: str) -> dict:
    soup = BeautifulSoup(document, features='xml')
    title = soup.metadata.dublinCore.title.text
    status = soup.metadata.orp.status.text
    regulatory_topic = soup.metadata.orp.regulatoryTopic.text.split(', ')
    document_type = soup.metadata.dublinCore.type.text
    date_created = soup.metadata.dublinCore.created.text
    return {
        'status': status,
        'document_type': document_type,
        'regulatory_topic': regulatory_topic,
        'date_created': date_created,
        'title': title
    }


def extract_text(document: str) -> str:
    soup = BeautifulSoup(document, features='xml')
    body_tag = soup.find('body')

    # Extract all the text content from inside the <body> tag
    text_content = body_tag.get_text(strip=True)
    return text_content


def group_attributes(attr):
    return DataFrame(attr).groupby(0)[1].apply(set).apply(
        lambda x: list(x) if len(x) > 1 else list(x)[0]).to_dict()


def getUniqueResult(results):
    res = [(i.get_type().get_label().name(), i.get_value())
           for a in results for i in a.concepts() if i.is_attribute()]
    return group_attributes(res)


def read_transaction(session, hash_list):
    '''
    params: session: TypeDB session opened
    params: hash_list: list of integers in hash list
        returns: matching_hash_list: list of hashes from the database that matched an integer in the hash_list
                metadata_dict: dictionary of the metadata of all the shortlisted documents
    '''
    logger.info(f'Incoming document hash: {"_".join(hash_list)}')
    window_size = 6
    hl = [hash_list[i:i + window_size]
          for i in range(0, len(hash_list) + 1, window_size)]
    contains = ' or '.join(['{$h contains \'%s\';}' % "_".join(hash) for hash in hl])

    query = f'''
    match
        $u isa regulatoryDocument,
        {''.join([f'has {i} ${i},' for i in search_keys])}
        has hash_text $h;
        not {{$u has status "archive";}};
        {contains}; group $u;
    '''

    logger.info(f"Query:\n {query}")

    # Read using a READ only transaction
    with session.transaction(TransactionType.READ) as read_transaction:
        answer_iterator = read_transaction.query().match_group(query)

        # Get matches on hash
        ans_list = [ans for ans in answer_iterator]
    metadata_dict = [getUniqueResult(results=a.concept_maps())
                     for a in ans_list]

    matching_hash_list = [
        np.array(
            hash['hash_text'].split('_'),
            dtype='uint64') for hash in metadata_dict]

    # Node IDs
    node_id_list = [node['node_id'] for node in metadata_dict]

    logger.info('Number of returned hashes: ' + str(len(matching_hash_list)))
    logger.info(matching_hash_list)
    return matching_hash_list, metadata_dict, node_id_list


def get_similarity_score(hash_np, matching_hash_list):
    '''
    params: hash_np: numpy hash of the incoming document
    params: matching_hash_list: list of hashes from the database that matched an integer in the hash_list
        returns: index or None: index is returned if an exact duplicate is found, otherwise None is returned
    '''
    scores = []
    for v in matching_hash_list:
        if v.shape == hash_np.shape:
            cosine = np.dot(hash_np, v) / (norm(hash_np) * norm(v))
            scores.append(cosine)
        else:
            logger.warn("Skipping: matching hash size doesn't match incoming hash.")

    max_score = max(scores)
    logger.info(f"Incoming hash: {hash_np}")
    logger.info(f"Max score: {max_score}")
    if (len(scores) > 0) and (max_score >= SIMILARITY_SCORE_CUTOFF):
        logger.info('Possible duplicate text detected')
        index = scores.index(max(scores))
        return index
    else:
        logger.info('New document')
        return None


def is_duplicate(index, incoming_metadata, complete_existing_metadata,
                 return_vals=return_vals):
    '''
    params: index int:
    params: incoming_metadata dict:
    params: complete_existing_metadata dict:
    params: return_vals List:
        returns: Boolean, Dictionary: (True / False) if (is / is not) a duplicate.
        complete_existing_metadata: All associated metadata so that the user can be given details of the existing
        document
        existing_dict: A smaller selection of the metadata to be replaced if document is found to be a version
    '''

    incoming_dict = {k: incoming_metadata[k]
                     for k in return_vals if k in incoming_metadata.keys()}
    existing_dict = {k: complete_existing_metadata[index][k]
                     for k in return_vals if k in complete_existing_metadata[index].keys()}

    if incoming_dict == existing_dict:
        logger.info(
            'Duplicate document with identical metadata - discard incoming document')
        return True, complete_existing_metadata
    else:
        logger.info('Different metadata detected')
        logger.info(
            f'Existing metadata {existing_dict} | Incoming metadata {incoming_dict}')
        return False, existing_dict


def search_module(session, hash_np, hash_list, incoming_metadata):
    '''
    params: session: TypeDB session
    params: hash_np: numpy hash of incoming document text
    params: hash_list: list of candidate matching hashes in the database
    params: incoming_metadata: metadata of the incoming document
        returns: is_duplicate_results / False: if is_duplicate_results is returned, the incoming document is a
        version or duplicate of the incoming document. Otherwise, the document is new.
    '''
    matching_hash_list, complete_existing_metadata, node_id_list = read_transaction(
        session, hash_list)
    index = get_similarity_score(
        hash_np=hash_np,
        matching_hash_list=matching_hash_list) if matching_hash_list else None

    # 1. If index != None, i.e. text with similarity > 0.95 found
    # 2. Test for exact duplicates and return the results
    # 3. Otherwise return false

    if index is not None:
        logger.info(f"Index returned: {index}")
        is_duplicate_results = is_duplicate(index=index, incoming_metadata=incoming_metadata,
                                            complete_existing_metadata=complete_existing_metadata)
        return is_duplicate_results + (node_id_list[index],)

    # No index returned, hence there are no similar documents
    else:
        logger.info("No index returned - no similar documents")
        return False


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    key_metadata = event

    document_uid = key_metadata['document_uid']

    logger.info('Event Body: ', event)
    SOURCE_BUCKET = validate_env_variable('SOURCE_BUCKET')
    TYPEDB_SERVER_IP = validate_env_variable('TYPEDB_SERVER_IP')
    SOURCE_BUCKET = validate_env_variable('SOURCE_BUCKET')
    TYPEDB_SERVER_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')
    COGNITO_USER_POOL = validate_env_variable('COGNITO_USER_POOL')
    SENDER_EMAIL_ADDRESS = validate_env_variable('SENDER_EMAIL_ADDRESS')

    # Open TypeDB session
    client = TypeDB.core_client(TYPEDB_SERVER_IP + ':' + TYPEDB_SERVER_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)

    # Call S3 and download processed text
    config = Config(connect_timeout=5, retries={'max_attempts': 0})
    s3_client = boto3.client('s3', config=config)
    document = download_document(
        s3_client=s3_client,
        document_uid=document_uid,
        bucket=SOURCE_BUCKET
    )

    doc_metadata = extract_metadata(document=document)
    metadata = {**doc_metadata, **key_metadata}
    text = extract_text(document=document)

    # Get incoming metadata
    incoming_metadata = dict(
        zip(return_vals, [metadata[val] for val in return_vals]))
    user_id = metadata['user_id']

    # If search module returns a True i.e. duplicate text with different metadata, then replace existing metadata
    # The returned dictionary is the existing document's metadata
    hash_np, hash_list = create_hash_list(text)
    is_duplicate_results = search_module(session, hash_np, hash_list, incoming_metadata)

    # Close TypeDB session and define handler response
    session.close()
    client.close()

    handler_response = metadata
    handler_response['text'] = text

    # ========== 1. If it is not a duplicate, insert hash and pass the document
    handler_response['hash_text'] = '_'.join(map(str, hash_np.tolist()))
    if is_duplicate_results is False:
        logger.info('New document!')
        return handler_response

    # ========== 2. If the document is a version (similar text, different metadata) return node ID
    elif is_duplicate_results[0] is False:
        node_id = is_duplicate_results[2]
        handler_response['node_id'] = node_id
        logger.info(
            f"Similar document exists! Node_id of existing version to be changed {node_id}")
        return handler_response

    # ========== 3. Else the document is a complete duplicate, and the user is informed
    else:
        # Get the existing metadata of the matching document
        complete_existing_metadata = is_duplicate_results[1]
        send_email(
            COGNITO_USER_POOL,
            SENDER_EMAIL_ADDRESS,
            user_id,
            complete_existing_metadata
        )
        return handler_response
