import re
import string
import pandas as pd
import pikepdf
import fitz
from PyPDF2 import PdfReader
from pdfminer.high_level import extract_text
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


def extract_title_and_date(doc_bytes_io):
    '''Extracts title from PDF streaming input'''

    pdf = pikepdf.Pdf.open(doc_bytes_io)
    meta = pdf.open_metadata()
    docinfo = pdf.docinfo
    dict_docinfo = dict(docinfo.items())

    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')

    # Get date
    if '/ModDate' in dict_docinfo:
        mod_date = re.search(r'\d{8}', str(docinfo['/ModDate'])).group()
    elif '/CreationDate' in dict_docinfo:
        mod_date = re.search(r'\d{8}', str(docinfo['/CreationDate'])).group()

    date_published = pd.to_datetime(
        mod_date).isoformat()

    return str(title), date_published


def extract_text_from_pdf(doc_bytes_io):
    '''Extracts text from PDF streaming input'''

    text = extract_text(doc_bytes_io)
    if text == "" or text is None:
        try:

            # creating a pdf reader object
            reader = PdfReader(doc_bytes_io)

            # printing number of pages in pdf file
            # print(len(reader.pages))

            totalPages = PdfReader.numPages

            # getting a specific page from the pdf file
            text = []
            for page in range(0, totalPages):
                page = reader.pages[page]
                # extracting text from page
                txt = page.extract_text()
                text.append(txt)

            text = " ".join(text)

        except Exception:
            stream = bytearray(open(doc_bytes_io, "rb").read())
            with fitz.open(doc_bytes_io, stream) as doc:
                text = ''
                for page in doc:
                    text += page.get_text()
        return text

    else:
        return text


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



def pdf_converter(doc_bytes_io):
    title, date_published = extract_title_and_date(doc_bytes_io=doc_bytes_io)
    text = clean_text(extract_text_from_pdf(doc_bytes_io=doc_bytes_io))
    return text, title, date_published
