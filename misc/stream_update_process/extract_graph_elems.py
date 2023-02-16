from utils.functions import *
from datetime import datetime
from json_flatten import flatten

colmap = {
    'data.dates.date_uploaded': 'date_uploaded',
    'data.dates.date_published': 'date_published',
    'data.dates.date_changed': 'date_changed'
}


def extractElements(js: dict, dict_thing_attrs: dict):
    nodes = []
    links = []
    doc = flatten(js)
    doc = key_remapper(doc, colmap)
    doc['keyword'] = js.get('subject_keywords', [])
    doc['regulatory_topic'] = js.get('regulatory_topic', [])
    doc['document_type'] = js.get('document_type', 'regulatory document')
    # insert regDoc node
    regID = [('node_id', hashID([doc.get(i) for i in ['uri', 'title']]))]

    nodes.append([
        "regulatoryDocument",
        regID,
        getElements(doc, dict_thing_attrs["regulatoryDocument"]) + regID
    ])
    # insert regulator node

    doc['regulator_id'] = doc.get('regulator_id', 'reg_test')
    reguID = [('node_id', hashID([doc['regulator_id']]))]
    nodes.append([
        "regulator",
        reguID,
        getElements(doc, dict_thing_attrs["regulator"]) + reguID
    ])
    # insert regDoc < pub > regulator
    # doc["date_changed"] = doc.get("date_changed", datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S"))
    links.append([
        'publication',
        'check',
        [
            ("regulatoryDocument", regID, "issued"),
            ("regulator", reguID, "issuedBy"),
        ],
        getElements(doc, dict_thing_attrs['publication'])
    ])

    # insert leg.org. node
    legs = js.get('data', {}).get('legislative_origins', [])
    for leg in legs:
        leg['leg_division'] = leg.pop('division')
        leg['leg_type'] = leg.pop('type')
        leg_etype = "primaryLegislation" if leg['leg_type'] == "Primary" else "secondaryLegislation"
        legID = [('node_id', hashID([leg['url']]))]
        nodes.append([
            leg_etype,
            legID,
            getElements(leg, dict_thing_attrs[leg_etype]) + legID
        ])

        # insert regDoc < pub > leg.org
        links = [[rtype, ids, nds + [(leg_etype, legID, "issuedFor")], attrs]
                 for rtype, ids, nds, attrs in links]

    return {'entities': nodes, 'links': links}
