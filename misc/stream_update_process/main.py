# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 15:53:34 2022

@author: imane.hafnaoui
"""
import json
from typedb.client import TypeDB, SessionType
from extract_graph_elems import extractElements
from record_handler import process_record
from utils.tdb_query_helpers import batch_insert, batch_match_insert
from queue_wrapper import *


import logging

LOGFILE = "logs/orp-pbeta-stream.log"
logger = logging.getLogger('ORP_Stream_KG_Ingestion')
logging.basicConfig(filename=LOGFILE,
                    filemode='a+',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

logging.getLogger().addHandler(logging.StreamHandler())

def message_handler(message, attr_type_dict, dict_thing_attrs, session):
    msg_body = message.body
    logger.info(f"Message Body:\n{msg_body}")
    try:
        data = json.loads(msg_body)
    except Exception as e:
        logger.exception(f"Invalid JSON - \n {e} \n {msg_body}")
        return
    try:
        logger.info(f"Document UID: {data.get('document_uid')}")
        # process record into graph elems
        gdata = extractElements(data, dict_thing_attrs)
        # process elems into db queries
        queries, mqueries = process_record(gdata, attr_type_dict, session)
        queries = list(filter(None, queries))
        mqueries = list(filter(None, mqueries))

        logger.info(f"Number of queries to [INSERT] [{len(queries)}]")
        logger.info(f"Number of queries to [MATCH-INSERT] [{len(mqueries)}]")
       

    except Exception as e:
        logger.exception(f"ERROR: Query Transform - \n {e}")
        return

    try:
        # batch ingest into tdb
        batch_insert(session, queries)
        batch_match_insert(session, mqueries)
        
        logger.info(f"--- Deleting the message ---")
        message.delete()

    except Exception as e:
        logger.exception(f"ERROR: DB Ingestion - \n {e}")

if __name__ == "__main__":

    # ==== VARS ====
    SCHEMA_FILE = "schema/orp-gdb-schema.json"
    TYPEDB_DATABASE_NAME = os.environ['TYPEDB_DATABASE_NAME']
    TYPEDB_DOCU_SQS_NAME = os.environ['TYPEDB_DOCU_SQS_NAME']

    # ======
    client = TypeDB.core_client('localhost:1729') 
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)


    schema = json.loads(open(SCHEMA_FILE).read())
    dict_thing_attrs = {thing:v['attr'] for i in schema.values() for thing, v in i.items()}
    attr_type_dict = {k:v['value'] for k,v in schema['attribute'].items()}

    # poll message from sqs
    queue = get_queue(TYPEDB_DOCU_SQS_NAME)
    msg_cnt = 1
    queue_messages = get_queue_messages(queue)

    while len(queue_messages)>0:
        for message in queue_messages:
            logger.info(f"=== Started processing message [{msg_cnt}] ===")
            message_handler(message, attr_type_dict, dict_thing_attrs, session)
            msg_cnt+=1
        queue_messages = get_queue_messages(queue)