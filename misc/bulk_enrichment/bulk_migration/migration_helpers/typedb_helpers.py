#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 10:58:18 2022

@author: imane.hafnaoui
"""
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
# from joblib import Parallel, delayed
from typedb.client import TransactionType, TypeDBOptions


import logging

handler = logging.FileHandler('logs/bulk_migration_batch_default.log')        
handler.setFormatter(logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s"))

loggertdb = logging.getLogger("Bulk_Migrator_batch_default")
loggertdb.setLevel(logging.INFO)
loggertdb.addHandler(handler)

def batch_insert(session, batch):
    ind, qbatch = batch
    # try:
    with session.transaction(TransactionType.WRITE) as transaction:
        if qbatch:
            typeql_insert_query = "insert " + " ".join(qbatch)
            transaction.query().insert(typeql_insert_query)
            transaction.commit()
    loggertdb.debug(f"++> QUERY\n{qbatch}")
    loggertdb.info(f"==> Finished inserting batch {ind} [{len(qbatch)}]")
    # except Exception as e:
    #     loggertdb.exception(f"==>! ERROR --> BATCH [{ind}]\n {e}")


def batch_match_insert(session, batch, inserttype=True):
    ind, qbatch = batch
    try:
        with session.transaction(TransactionType.WRITE) as transaction:
            for qm in qbatch:
                transaction.query().insert(
                    qm
                ) if inserttype else transaction.query().delete(qm)
            transaction.commit()
        loggertdb.info(f"==> Finished match-inserting batch {ind} [{len(qbatch)}]")
    except Exception as e:
        loggertdb.exception(f"==>! ERROR --> BATCH [{ind}]\n {e}")


def chunker(iterable, chunksize):
    return (
        iterable[pos : pos + chunksize] for pos in range(0, len(iterable), chunksize)
    )


def multi_thread_write_query_batches(
    session,
    query_batches,
    method,
    num_threads=4
    ):
    pool = ThreadPool(num_threads)
    pool.map(partial(method, session), query_batches)
    pool.close()
    pool.join()


def insert_data_bulk(
    session,
    queries,
    num_threads = 4,
    batch_size = 100,
    is_insert=True,
    typedb_options=None
    ):

    if not typedb_options:
        typedb_options = TypeDBOptions.core()
    batches = enumerate(chunker(queries, chunksize=batch_size))
    method = batch_insert if is_insert else batch_match_insert
    multi_thread_write_query_batches(session, batches, method, num_threads)
