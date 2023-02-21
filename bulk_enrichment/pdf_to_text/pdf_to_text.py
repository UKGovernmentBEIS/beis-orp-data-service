import io
import re
import pikepdf
import fitz
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

import logging
logger = logging.getLogger("Bulk_processing").addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def extract_title(doc_bytes_io):
    '''Extracts title from PDF streaming input'''

    pdf = pikepdf.Pdf.open(doc_bytes_io)
    meta = pdf.open_metadata()
    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')

    return str(title)


def extract_text(doc_bytes_io):
    '''Extracts text from PDF streaming input'''

    with fitz.open(stream=doc_bytes_io) as doc:
        text = ''
        for page in doc:
            text += page.get_text()

    return text


def clean_text(text):
    '''Clean the text by removing illegal characters and excess whitespace'''

    text = re.sub('\n', ' ', text)
    text = re.sub(ILLEGAL_CHARACTERS_RE, ' ', text)

    # Space out merged words by adding a space before a capital letter
    # if it appears after a lowercase letter
    text = re.sub(
        r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))',
        r'\1 ',
        text
    )

    text = text.strip()
    text = text.lower()
    text = text.replace('\t', ' ')
    text = text.replace('_x000c_', '')
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\\.{4,}', '.')

    return text


def cut_title(title):
    '''Cuts title length down to 25 tokens'''

    title = re.sub('Figure 1', '', title)
    title = re.sub(r'[^\w\s]', '', title)

    if len(str(title).split(' ')) > 25:
        title = ' '.join(title.split(' ')[0:25]) + '...'

    return title


def write_text(text, document_uid, destination_bucket):
    '''Write the extracted text to a .txt file in the staging bucket'''

    open(f'{destination_bucket}/{document_uid}.txt', 'w+').write(text)
    logger.debug(f'Saved text to {destination_bucket}')

def pdf_converter(file_path, document_uid, save_path):
    doc_bytes_io = io.BytesIO(open(file_path))

    title = extract_title(doc_bytes_io=doc_bytes_io)
    text = extract_text(doc_bytes_io=doc_bytes_io)
    logger.debug(f'Extracted title: {title}'
                f'UUID obtained is: {document_uid}')


    write_text(text=text, document_uid=document_uid, destination_bucket=save_path)

    return text, title
