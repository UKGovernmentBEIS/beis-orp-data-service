import argparse
from vars_orp_pbeta import *

def migrator_parser():
    parser = argparse.ArgumentParser(
        description="Define database schema and insert data."
    )
    parser.add_argument(
        "-a",
        "--host",
        help=f"Server host address (default: {DB_IP})",
        default=DB_IP,
    )
    parser.add_argument(
        "-p", "--port", help="Server port (default: 1729)", default="1729"
    )
    parser.add_argument(
        "-n",
        "--num_threads",
        type=int,
        help="Number of threads to enable multiprocessing (default: 4)",
        default=4,
    )
    parser.add_argument(
        "-c",
        "--batch_size",
        type=int,
        help="Sets the number of queries made per commit (default: 500)",
        default=500,
    )
    parser.add_argument(
        "-d",
        "--database",
        help=f"Database name (default: {DB_NAME}",
        default=DB_NAME,
    )
    parser.add_argument(
        "-e",
        "--existing",
        action="store_true",
        help="Write to database by this name even if it already exists (default: False)",
        default=False,
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="If a database by this name already exists, delete and overwrite it (default: False)",
        default=False,
    )

    return parser


from timeit import default_timer as timer

from migration_helpers.migrate_helpers import *
from migration_helpers.typedb_helpers import insert_data_bulk
from migration_helpers.utils import (getSchemaAttrType, getThingAttr,
                                     getThingRole)

from typedb.client import SessionType, TransactionType, TypeDB
import pandas as pd

logger.info("- BULK DATA INGESTION STARTED -")
if __name__ == "__main__":

    start = timer()

    # get cmd line arguments
    parser = migrator_parser()
    args = parser.parse_args()

    # 0. all attribute value types as a dict through a query
    client = TypeDB.core_client(address=f"{args.host}:{args.port}")
    # checking whether database already exists; if not, create it
    # databases = [db.name() for db in client.databases().all()]
    if args.force:
        try:
            client.databases().get(args.database).delete()
        except Exception:
            pass
    logger.info(f"Creating Database [{args.database}]")
    if client.databases().contains(args.database):
        if not args.existing:
            raise UserWarning(
                f"database {args.database} already exists. Use --existing to write into existing database or --force to delete it and start anew."
            )
    else:
        client.databases().create(args.database)
    query_define = open(SCHEMA_TQL, "r").read()

    logger.info(f"Importing schema [{SCHEMA_TQL}]")
    # define schema
    session = client.session(args.database, SessionType.SCHEMA)
    # with  as session:
    with session.transaction(TransactionType.WRITE) as write_transaction:
        write_transaction.query().define(query_define)
        write_transaction.commit()

    logger.info("Defining database elements")
    # get all attributes and their valuetypes
    dict_attr_valuetype = getSchemaAttrType(session)

    # get thing roles
    dict_roles = getThingRole(session)

    # get thing attributes
    dict_thing_attrs = getThingAttr(session)
    session.close()

    # ENTITIES
    # thingType = "nodes"
    for thingType in ["nodes", "links"]:
        fpath = f"{DIR_PATH}/{thingType}.p"
        logger.info(f"Started bulk {thingType} ingestion")
        df = pd.read_pickle(fpath)
        logger.info(f"Preparing insert queries [{df.shape[0]}]")
        is_insert = thingType == "nodes"
        prep_method = prep_entity_insert_queries if is_insert else prep_relation_insert_queries
        queries = prep_method(df, attr_type_mapping=dict_attr_valuetype)
        with client.session(args.database, SessionType.DATA) as session:
            logger.info(f" Ingesting -> {df.shape[0]} records")
            insert_data_bulk(
                session,
                queries,
                num_threads=args.num_threads,
                batch_size=args.batch_size,
                typedb_options=None,
                is_insert=is_insert
            )
        logger.info(f"+++ Done inserting {thingType} +++")

    thingType = "attributes"

    logger.info(f"Preparing {thingType} insert queries")
    fpath = f"{DIR_PATH}/{thingType}.p"
    df = pd.read_pickle(fpath)
    logger.info(f" Preparing insert queries")
    queries = prep_attribute_insert_queries(
        df,
        dict_attr_valuetype,
    )
    with client.session(args.database, SessionType.DATA) as session:
        logger.info(f"Ingesting -> {df.shape[0]} records")
        insert_data_bulk(
            session,
            queries,
            num_threads=args.num_threads,
            batch_size=args.batch_size,
            typedb_options=None,
            is_insert=True,
        )
    logger.info(f"+++ Done inserting {thingType} +++")

   
    end = timer()
    time_in_sec = end - start
    logger.info("Elapsed time: " + str(time_in_sec) + " seconds.")
