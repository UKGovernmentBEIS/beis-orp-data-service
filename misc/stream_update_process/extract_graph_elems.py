from utils.functions import *
from datetime import datetime
from json_flatten import flatten


leg_types = {
'Primary':'primaryLegislation',
'Secondary':'secondaryLegislation',
}

colmap = {
  'data.dates.date_uploaded':'date_uploaded',
 'data.dates.date_published':'date_published',
 'data.dates.date_modified':'date_modified'
}
STATIC_MD=['status','regulator_id','document_type', 'title']

def extractElements(js:dict, dict_thing_attrs:dict):
    nodes = []
    links = []
    doc = flatten(js)
    doc = key_remapper(doc, colmap)
    doc['keyword'] = js.get('subject_keywords', [])
    reg_topic = js.get('regulatory_topic', [])
    doc['regulatory_topic'] = reg_topic
    if reg_topic: 
        doc['assigned_orp_topic'] = max(reg_topic, key=len)
    doc['document_type'] = js.get('document_type', 'NA')
    
    # insert regDoc node
    node_id = doc.get('node_id', hashID([doc.get(i) for i in STATIC_MD]))
    regID = [('node_id',node_id)]
    nodes.append([
        "regulatoryDocument", 
        regID,
        getElements(doc, dict_thing_attrs["regulatoryDocument"]) + regID
    ])
    # insert regulator node

    doc['regulator_id'] = doc.get('regulator_id', 'reg_default')
    reguID = [('node_id', hashID([doc['regulator_id']]))]
    nodes.append([
        "regulator", 
        reguID,
        getElements(doc, dict_thing_attrs["regulator"]) + reguID
    ])

    # insert regulator agent node

    doc['user_id'] = doc.get('user_id', 'user_default')
    userID = [('node_id', hashID([doc['user_id']]))]
    nodes.append([
        "regulatoryAgent",
        userID,
        getElements(doc, dict_thing_attrs["regulatoryAgent"]) + userID
    ])

    # insert regDoc < pub > regulator
    doc["date_modified"] = doc.get("date_uploaded", datetime.now().isoformat())
    links.append([
        'publication',
        True,
        [
            ( "regulatoryDocument", regID, "issued"),
            ("regulator", reguID, "issuedBy"),
            ( "regulatoryAgent", userID, "uploader"),
        ],
        getElements(doc,dict_thing_attrs['publication']) 
    ])

    # insert leg.org. node
    legs = js['data'].get('legislative_origins', [])
    for leg in legs:
        leg['leg_division'] = leg.pop('division')
        leg['leg_type'] = leg.pop('type')
        leg['leg_number'] = leg.pop('number')
        leg_etype = leg_types.get(leg['leg_type'], 'legislation')
        legID = [('node_id', hashID([leg['href']]))]
        nodes.append([
            leg_etype, 
            legID,
            getElements(leg, dict_thing_attrs[leg_etype]) + legID
        ])

        # insert regDoc < pub > leg.org
        links =[[rtype,ids, nds+[(leg_etype, legID, "issuedFor")], attrs] for rtype, ids, nds, attrs in links]
       
     # insert regulatorAgent < partOf > regulator
    links.append([
        'partOf',
        True,
        [
            ( "regulatoryAgent", userID, "agent"),
            ("regulator", reguID, "agency"),
        ],
        getElements(doc.to_dict(),dict_thing_attrs['partOf'])
    ])

    return {'entities': nodes, 'links':links}