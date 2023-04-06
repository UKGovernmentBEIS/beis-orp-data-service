# -*- coding: utf-8 -*-
"""
Created on Mon Feb  7 16:58:33 2022

@author: imane.hafnaoui
"""


import re
# from datetime import datetime, timezone
from itertools import groupby
from pandas import  to_datetime
from typedb.client import TransactionType

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

# ==== Migration


def getSchemaAttrType(session):
    with session.transaction(TransactionType.READ) as read_transaction:
        iterator_conceptMap = read_transaction.query().match(
            "match $x sub attribute; not {$x type attribute;}; "
        )
        list_concept = [conceptMap.get("x") for conceptMap in iterator_conceptMap]
        attr_type_mapping = {
            concept.get_label().name(): concept.get_value_type().name.lower()
            for concept in list_concept
        }
        return attr_type_mapping


def getThingAttr(session):
    with session.transaction(TransactionType.READ) as read_transaction:
        iterator_conceptMap = read_transaction.query().match(
            "match $x sub thing, owns $a;  not {$x sub attribute;};"
        )
        list_concept = [
            (
                conceptMap.get("x").get_label().name(),
                conceptMap.get("a").get_label().name(),
            )
            for conceptMap in iterator_conceptMap
        ]
        thing_attrs = {
            m: [i[1] for i in g] for m, g in groupby(list_concept, key=lambda x: x[0])
        }
        return thing_attrs


def getThingRole(session):
    with session.transaction(TransactionType.READ) as read_transaction:
        iterator_conceptMap = read_transaction.query().match(
            "match $x sub thing, plays $a;  not {$x sub attribute;};"
        )
        list_concept = [
            (
                conceptMap.get("x").get_label().name(),
                conceptMap.get("a").get_label().name(),
            )
            for conceptMap in iterator_conceptMap
        ]
        thing_roles = {
            m: [i[1] for i in g] for m, g in groupby(list_concept, key=lambda x: x[0])
        }
        return thing_roles


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
    # [tdb] format multiple attribute ownership
    return f"{delimiter}".join(
        [match(attr, value, attr_type_dict[attr]) for attr, value in attrs]
    )
