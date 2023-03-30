import re
import string
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

def remove_excess_punctuation(text) -> str:
    '''
    param: text: Str document text
    returns: text: Str cleaned document text
        Returns text without excess punctuation
    '''
    # Clean punctuation spacing
    text = text.replace(' .', '')
    for punc in string.punctuation:
        text = text.replace(punc + punc, '')
    return text


def clean_text(text):
    '''Clean the text by removing illegal characters and excess whitespace'''
    pattern = re.compile(r'\s+')

    text = str(text).replace('\n', ' ')
    text = text.replace(' .', '. ')
    text = re.sub('(\\d+(\\.\\d+)?)', r' \1 .', text)
    text = re.sub(pattern, ' ', text)
    text = remove_excess_punctuation(text)
    text = re.sub(ILLEGAL_CHARACTERS_RE, ' ', text)

    # Space out merged words by adding a space before a capital letter
    # if it appears after a lowercase letter
    text = re.sub(
        r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))',
        r'\1 ',
        text
    )

    text = text.strip()
    text = text.replace('\t', ' ')
    text = text.replace('_x000c_', '')
    text = text.encode('ascii', 'ignore').decode("utf-8")
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\\.{4,}', '.', text)

    return text