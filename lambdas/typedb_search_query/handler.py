#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 10:15:34 2022

@author: imane.hafnaoui
"""

import json
import logging
import os
from typedb.client import SessionType, TypeDB
from search_functions import *

# list of params accepted in a search event
search_keys = {"id", "keyword", "title", "date_published",
               "regulator_id", "status", "regulatory_topic", "document_type",
               "legislation_href"}

RET_SIZE = 10

LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


###########################################
# HELPER FUNCTIONS
###########################################

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


############################################
# LAMBDA HANDLER
############################################


def search_module(event, session):
    
        keyset = set(event.keys()) & search_keys
        page_size = int(event.get('page_size', RET_SIZE))
        page = int(event.get('page', 0)) * page_size
        order = event.get('order', 'desc')

        if len(keyset) == 0:
            return {
                "status_code": 400,
                "status_description": "Bad Request - Unsupported search parameter(s)."
            }

        else:
            # Build TQL query from search params
            query = query_builder(event)

            # Query the graph database for reg. documents
            ans = matchquery(query, session)
            num_ret = len(ans)

            LOGGER.info(f"Ret -> {num_ret}")
            if num_ret == 0:
                docs = []
            else:
                # leg <> reg.doc link search
                if event.get('legislation_href'):
                    docs = search_reg_docs(ans, page_size)
                # search by UID
                elif event.get('id'):
                    docs = format_doc_results(ans, session, id_search=True)
                # search + filters
                else:                    
                    docs = format_doc_results(ans, session, page=page, page_size=page_size, asc=order=='asc')

            LOGGER.info(f"Results: {docs}")
            return {
                "status_code": 200,
                "status_description": "OK",
                "total_search_results": num_ret,
                "documents": docs
            }

def lambda_handler(ev, context):
    LOGGER.info("Received event: " + json.dumps(ev, indent=2))

    event = json.loads(ev['body'])

    LOGGER.info(f"Event Body: {event}")
    TYPEDB_IP = validate_env_variable('TYPEDB_SERVER_IP')
    TYPEDB_PORT = validate_env_variable('TYPEDB_SERVER_PORT')
    TYPEDB_DATABASE_NAME = validate_env_variable('TYPEDB_DATABASE_NAME')

    client = TypeDB.core_client(TYPEDB_IP + ':' + TYPEDB_PORT)
    session = client.session(TYPEDB_DATABASE_NAME, SessionType.DATA)
    result = search_module(event, session)
    return result
