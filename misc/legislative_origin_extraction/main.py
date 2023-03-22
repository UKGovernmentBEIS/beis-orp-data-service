import os
import pandas as pd
import json
from helpers import NLPsetup
from legislation_origin_matcher import leg_pipeline

ROOTDIR = "."
OUTPUT_FILE = "leg_tuples2.jsonl"
cutoff = 0.1  # Top section of document to look into

# pull list of existing legislation
LEGISLATION_DATA_PATH = "legislation_data_ALL.p"
leg_titles = pd.read_pickle(LEGISLATION_DATA_PATH)
leg_titles = leg_titles[leg_titles.legType.isin(['Primary', 'Secondary'])]

# setup nlp module with custom sententiser
nlp = NLPsetup()

tuple_file = open(OUTPUT_FILE, "w+")
flist = [fl for fl in os.listdir(ROOTDIR) if fl.endswith('.txt')]
# for each txt of rugulatory document
for i, file in enumerate(flist):
    file_path = os.path.join(ROOTDIR, file)
    with open(file_path, "r", encoding="utf-8") as file_in:
        reg_doc_text = file_in.read()
        print(f'{i}/{len(flist)} => "{file_path}" [{len(reg_doc_text)}]')
        # get top section of text
        TOP_DOC_CUTOFF = int(len(reg_doc_text) * cutoff)
        doc = nlp(reg_doc_text[:TOP_DOC_CUTOFF])

    # run pipeline to detect legislation references
    LEG_ORGs = leg_pipeline(leg_titles, nlp, doc)

    # append metadata to found legislations
    for leg in LEG_ORGs:
        LEG_ORG = {}
        ref = leg_titles[leg_titles.candidate_titles == leg]
        LEG_ORG['filename'] = file
        if not ref.empty:
            ref = ref.iloc[0]
            LEG_ORG['title'] = ref.title
            LEG_ORG['ref'] = ref.ref
            LEG_ORG['href'] = ref.href
            LEG_ORG['legNumber'] = ref.number
            LEG_ORG['legDivision'] = ref.legDivision
            LEG_ORG['legType'] = ref.legType
        tuple_file.write(json.dumps(LEG_ORG) + "\n")
tuple_file.close()
# close_connection(db_conn)
