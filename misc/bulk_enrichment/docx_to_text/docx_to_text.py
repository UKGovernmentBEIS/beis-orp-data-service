import io
import docx
import zipfile
import xml.etree.ElementTree as ET

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bulk_processing")

# Defining elements from openxml schema
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'



def get_docx_text(path):
    """
    param: path Str: Take the path of a docx file as argument
    returns: paragraphs Str: text in unicode.
        Function from stackoverflow to pull text from a docx file
    """
    document = zipfile.ZipFile(path)
    xml_content = document.read('word/document.xml')
    document.close()
    tree = ET.XML(xml_content)

    paragraphs = []
    for paragraph in tree.iter(PARA):
        texts = [node.text
                 for node in paragraph.iter(TEXT)
                 if node.text]
        if texts:
            paragraphs.append(''.join(texts))

    return '\n\n'.join(paragraphs)


def getMetaData(doc):
    """
    param: doc docx
    returns: metadata
        Function from stackoverflow to get metadata from docx
    """

    prop = doc.core_properties
    metadata = {
        "author": prop.author,
        "category": prop.category,
        "comments": prop.comments,
        "content_status": prop.content_status,
        "created": prop.created,
        "identifier": prop.identifier,
        "keywords": prop.keywords,
        "last_modified_by": prop.last_modified_by,
        "language": prop.language,
        "modified": prop.modified,
        "subject": prop.subject,
        "title": prop.title,
        "version": prop.version
    }

    return metadata


def write_text(text, document_uid, destination_bucket):
    '''Write the extracted text to a .txt file in the staging bucket'''

    open(f'{destination_bucket}/{document_uid}.txt', 'w+').write(text)
    logger.debug(f'Saved text to {destination_bucket}')


def docx_converter(file_path, document_uid, save_path):
   
    logger.info('--- Calling DOCX converter')
    docx_file = io.BytesIO(open(file_path).read())
    doc = docx.Document(docx_file)
    metadata = getMetaData(doc=doc)

    # Get title and date published
    title = metadata["title"]
    date_published = metadata["created"]

    # Get text 
    text = get_docx_text(path=docx_file)
    write_text(text=text, document_uid=document_uid, destination_bucket=save_path)

    logger.debug(f"All data extracted. E.g. Title extracted: {title}")


    return text, title, date_published
