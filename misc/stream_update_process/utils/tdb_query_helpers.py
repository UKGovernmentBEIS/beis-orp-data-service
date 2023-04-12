# -*- coding: utf-8 -*-
"""
Created on Tue Sep 6 11:12:34 2022

@author: imane.hafnaoui
"""
from typedb.client import TransactionType
from pandas import to_datetime
# from vars_orp_stream import logger
from datetime import timezone, datetime
import logging
logger = logging.getLogger('ORP_Stream_KG_Ingestion')

# def convertDateUTC(date):
#     return datetime.strftime(date.astimezone(timezone.utc), "%Y-%m-%dT%H:%M:%S")

# ==== TDB QUERY HELPERS ===
def getResults(ans):
    results = [list(zip(a.keys(), [(i.get_type().get_label().name(), i.get_value())  for i in a.values() if i.is_attribute()])) for a in ans ]
    # results = [(k,convertDateUTC(v) if type(v)==datetime else v) for k,v in results]
    return results

def getUniqueResult(results):
    results = [(i.get_type().get_label().name(), i.get_value()) for a in results for i in a.concepts() if i.is_attribute()]
    # results = [(k,convertDateUTC(v) if type(v)==datetime else v) for k,v in results]
    return results

def matchquery(query, session):
    with session.transaction(TransactionType.READ) as transaction:
        logger.debug("Query:\n %s"%query)
        iterator = transaction.query().match(query)
        results = [ans for ans in iterator]
        return results

def matchgroupquery(query, session):
    with session.transaction(TransactionType.READ) as transaction:
        logger.debug("Query:\n %s"%query)
        iterator = transaction.query().match_group(query)
        results = [ans for ans in iterator]
        return results 
    
def deleteEntityQuery(etype, identifiers, attr_type_dict):
    q = "match "
    q += f"$x isa {etype} {''.join([match(k,v,attr_type_dict[k]) for k,v in identifiers])};"
    q += f"delete $x isa {etype};"
    return q

def deleteRelationQuery(rtype, nodes, identifiers, attr_type_dict): 
    # nodes = [(etype, ids)]
    q = "match "
    q += ' '.join([f"$x{i} isa {etype} {formatAttrDB(ids , attr_type_dict)};" for i, (etype, ids) in enumerate(nodes)]) 
    q += f"$rel "
    if nodes: q+= f"({','.join([f'$x{i}' for i in range(len(nodes))])})"
    q += f"isa {rtype} {''.join([match(k,v,attr_type_dict[k]) for k,v in identifiers if type(v)!=list])};"
    q += f"delete $rel isa {rtype};"
    return q


def getEntityDB(etype, identifier, attr_type_dict, session):
    logger.debug(f"? ==> Checking entity {etype} exists" )
    query = f"match $x isa {etype} {formatAttrDB(identifier, attr_type_dict)}, has attribute $a; get $a;"
    out = getUniqueResult(matchquery(query, session))
    return out

def getRelationDB(rtype, ids, nodes, attr_type_dict, session, date_indicator="PublishedOn", check=False):

    query = "match" +  "".join([f" $x{i} isa {etype} \
        {formatAttrDB(eID, attr_type_dict)};" for i, (etype, eID, _) in enumerate(nodes)])

    #     {''.join([match(k,v, attr_type_dict[k]) for k,v in resolveID(eID)])} ;" for i, (etype, eID, _) in enumerate(nodes)])
    query += f"$p({','.join([f'$x{i}' for i in range(len(nodes))])}) isa {rtype}"
    query += formatAttrDB(ids, attr_type_dict)
    if not check: 
        query += ", has attribute $rattr; get $rattr, $p; group $p;"
        # grab latest one
        links = [getUniqueResult(i.concept_maps()) for i in matchgroupquery(query, session)]
        if links:
            max_date = max([dict(l).get(date_indicator, 0) for l in links])
            latest = [l for l in links if dict(l).get(date_indicator)==max_date][0] if max_date!=0 else links[0]
            return latest
    else:
        query += "; get $p; group $p;"
        links = [getUniqueResult(i.concept_maps()) for i in matchgroupquery(query, session)]
        if links: return True

def deleteAttrOwn(etype, attrs, attr_type_dict):
    query = f"match $x isa {etype}, "\
        f'{", ".join([f"has {k} $attr{i} " for i, (k, _) in enumerate(attrs)]) };' \
        f'{"; ".join([f"$attr{i} {format_attr(v, attr_type_dict[k])}" for i, (k, v) in enumerate(attrs)]) };' \
        f'delete $x {", ".join([f"has $attr{i} " for i in range(len((attrs)))]) };'
    return query

def match_insert_ent(etype, ids, attrs, attr_type_dict):
    logger.info(f"==> updating entity {etype}")
    logger.debug(f"<- \n{attrs}")
    query = f"match $x isa {etype} {formatAttrDB(ids, attr_type_dict)}; insert $x "
    query += formatAttrDB(attrs, attr_type_dict)[1:]
    query += ";"
    return query
    
# ==== TDB INGESTION ====
# TODO change logging

import re

# === VARS
spc = r'\\|"|,|;'

#======== BASIC
def rmspc(string): return re.sub(spc, ' ', string)

def format_attr(v, atype):
    if (atype=='string'): return f'"{re.sub(spc, " ", str(v))}"'
    elif (atype=='datetime'): return to_datetime(v).strftime('%Y-%m-%dT%H:%M:%S')
    elif (atype=='long'): return int(float(v))
    else: return v

def match(k, v, atype):
    if v:
        try:
            if (type(v)==list) : return ''.join([match(k, i, atype) for i in v])
            else : return f', has {k} {format_attr(v, atype)}'
        except Exception as e:
            print(f'ERROR: Unexpected at ({k,v,atype})\n {e}')
    else: return ''

def formatAttrDB(attrs, attr_type_dict, delimiter=""):
    return f"{delimiter}".join([match(attr, value, attr_type_dict[attr]) for attr, value in attrs])

# =====ingestion

def batch_insert(session , qbatch):
    if qbatch:
        s='\n\n'.join(qbatch)
        logger.debug(f'Queries:\n {s}')
        with session.transaction(TransactionType.WRITE) as transaction:
               typeql_insert_query = 'insert ' + ' '.join(qbatch)
               transaction.query().insert(typeql_insert_query)
               transaction.commit()
        logger.info(f'=> Finished inserting batch [{len(qbatch)}]')
                   
def batch_match_insert(session, qbatch, inserttype = True):
        s='\n\n'.join(qbatch)
        logger.debug(f'Queries:\n {s}')
        with session.transaction(TransactionType.WRITE) as transaction:
            for qm in qbatch:
                transaction.query().insert(qm) if inserttype else transaction.query().delete(qm)
            transaction.commit()
        logger.info(f'=> Finished m-inserting batch [{len(qbatch)}]')


def chunker(iterable, chunksize):
        return (iterable[pos: pos + chunksize] for pos in range(0, len(iterable), chunksize))
