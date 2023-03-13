import pandas as pd
from legislative_origin.helpers import NLPsetup
from legislative_origin.legislation_origin_matcher import leg_pipeline

CUTOFF = 0.1  # Top section of document to look into

def lo_extraction(text):
    # pull list of existing legislation
    LEGISLATION_DATA_PATH = "/Users/imane.hafnaoui/gitpod/mxt-pod/beis-orp/legislation_extraction/legislation_data_ALL.p"
    leg_titles = pd.read_pickle(LEGISLATION_DATA_PATH)
    leg_titles = leg_titles[leg_titles.legType.isin(['Primary', 'Secondary'])]

    # setup nlp module with custom sententiser 
    nlp = NLPsetup()

       
    TOP_DOC_CUTOFF = int(len(text) * CUTOFF)
    doc = nlp(text[:TOP_DOC_CUTOFF])
    print('lengths: ', leg_titles.shape[0], len(text), len(text[:TOP_DOC_CUTOFF]), len(doc))
    # run pipeline to detect legislation references
    LEG_ORGs = leg_pipeline(leg_titles, nlp, doc)

    # append metadata to found legislations
    LEGS = []
    for leg in LEG_ORGs:
        LEG_ORG = {}
        ref = leg_titles[leg_titles.candidate_titles == leg]
        if not ref.empty:
            ref = ref.iloc[0]
        LEG_ORG['title'] = ref.title
        LEG_ORG['ref'] = ref.ref
        LEG_ORG['href'] = ref.href
        LEG_ORG['legNumber'] = int(ref.number)
        LEG_ORG['legDivision'] = ref.legDivision
        LEG_ORG['legType'] = ref.legType
        LEGS.append(LEG_ORG)
    return LEGS