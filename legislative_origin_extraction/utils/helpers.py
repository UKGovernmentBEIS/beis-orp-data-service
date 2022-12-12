from spacy.language import Language
import spacy

@Language.component('custom_sentencizer')
def custom_sentencizer(doc):
    ''' Look for sentence start tokens by scanning for periods only. '''
    for i, token in enumerate(doc[:-2]):  # The last token cannot start a sentence
        if token.text == ".":
            doc[i+1].is_sent_start = True
        else:
            doc[i+1].is_sent_start = False  # Tell the default sentencizer to ignore this token
    return doc

def NLPsetup():
    nlp = spacy.load("en_core_web_sm", exclude=['tok2vec','senter', 'attribute_ruler', 'lemmatizer', 'ner'])
    nlp.max_length = 500000
    nlp.add_pipe('custom_sentencizer', before="parser")
    return nlp