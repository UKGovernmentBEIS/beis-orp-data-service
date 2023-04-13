# -*- coding: utf-8 -*-
"""
Created on Tue Sep 6 11:12:34 2022

@author: imane.hafnaoui
"""
from typedb.client import TransactionType
from pandas import to_datetime
from itertools import groupby
# from vars_orp_stream import logger

import logging
logger = logging.getLogger('ORP_Stream_KG_Ingestion')

# ==== TDB QUERY HELPERS ===

def group_attributes(attr):
    results = []
    for k, gp in groupby(attr, lambda x: x[0]):
        gpl = list(gp)
        results.append((k, [i[1] for i in gpl] if len(gpl) > 1 else gpl[0][1]))
    return dict(results)


def getUniqueResult(results):
    results = [(i.get_type().get_label().name(), i.get_value()) 
            for a in results for i in a.concepts() if i.is_attribute()]
    return group_attributes(results)

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

def deleteAttrOwn(etype, identifier, attrs, attr_type_dict, in_attrs=None):
    dat = ''
    q1= ''
    q2=''
    query = f'match $x isa {etype}, has Identifier "{identifier}"'
    for i, (k, v) in enumerate(attrs):
        if type(v)==list:
            q1 += "".join([f", has {k} $attr{i}{j} " for j,_ in enumerate(v)])
            q2 += f'{"".join([f"; $attr{i}{j} {format_attr(vv, attr_type_dict[k])}" for j, vv in enumerate(v)]) }'
            dat += "".join([f", has $attr{i}{j} " for j in range(len((v)))])
        else:
            q1 += f', has {k} $attr{i}'
            q2 += f'; $attr{i} {format_attr(v, attr_type_dict[k])}' 
            dat += f", has $attr{i}"
    query += f'{q1}{q2}; delete $x {dat[1:]};'
    if in_attrs:
        query += f'insert $x' \
        f'{", ".join([f"has {k} {format_attr(v, attr_type_dict[k])}" for k, v in in_attrs]) };' 
    return query


def match_insert_ent(etype, ids, attrs, attr_type_dict):
    logger.info(f"==> updating entity {etype}")
    logger.debug(f"<- \n{attrs}")
    query = f"match $x isa {etype} {formatAttrDB(ids, attr_type_dict)}; insert $x "
    query += formatAttrDB(attrs, attr_type_dict)[1:]
    query += ";"
    return query
    
# ==== TDB INGESTION ====


import re
# === VARS
spc = r'\\|"|,|;'

# ======== BASIC
def clean_text(string):
    return re.sub(spc, " ", string).encode("ascii", "ignore").decode().strip()


def convertDateUTC(date):
    try:
        return to_datetime(date).tz_localize('Europe/Lisbon').tz_convert('utc').isoformat(timespec='seconds').replace("+00:00", "")
    except:
        return to_datetime(date).isoformat(timespec='seconds').replace("+00:00", "")


def format_attr(v, atype):
    # [tbd] wrap the attribute into an allowed format
    if atype == "datetime":
        return convertDateUTC(v)
        # return to_datetime(v, infer_datetime_format=True).strftime("%Y-%m-%dT%H:%M:%S")
    elif atype == "long":
        return int(float(v))
    elif atype == "double":
        return float(v)
    elif atype == "boolean":
        return str(bool(v)).lower()
    else:
        return  f'"{clean_text(str(v))}"'


def match(k, v, atype):
    # [tbd] format query for attribute ownership
    if v:
        try:
            if type(v) == list:
                return "".join([match(k, i, atype) for i in v])
            else:
                return f", has {k} {format_attr(v, atype)}"
        except Exception as e:
            print(f"ERROR: Unexpected at ({k,v,atype})\n {e}")
    else:
        return ""
    
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
