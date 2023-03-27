import pandas as pd
from legislative_origin.helpers import NLPsetup
from legislative_origin.legislation_origin_matcher import leg_pipeline
from pyspark import SparkFiles

CUTOFF = 0.1  # Top section of document to look into

def lo_extraction(text):

    path = SparkFiles.get('resources/legislation_data_2023_03_12.csv')
    
    leg_titles = pd.read_csv(path)
    leg_titles = leg_titles[leg_titles.legType.isin(['Primary', 'Secondary'])]
    # setup nlp module with custom sententiser 
    nlp = NLPsetup()

       
    TOP_DOC_CUTOFF = max(2000, int(len(text) * CUTOFF))
    doc = nlp(text[:TOP_DOC_CUTOFF])
    print('lengths: ', leg_titles.shape[0], len(text), len(text[:TOP_DOC_CUTOFF]), len(doc))
    # run pipeline to detect legislation references
    LEG_ORGs = leg_pipeline(leg_titles, nlp, doc)

    # append metadata to found legislations
    LEGS = []
    for leg in LEG_ORGs:
        ref = leg_titles[leg_titles.candidate_titles == leg]
        if not ref.empty:
            ref = ref.iloc[0]
            LEGS.append([ref.title,
                         ref.ref,
                         ref.href,
                         str(ref.number),
                         ref.legDivision,
                         ref.legType])
    return LEGS