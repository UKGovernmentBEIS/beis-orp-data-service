import io
import re
import zipfile
import datetime
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


import logging
logger = logging.getLogger("Bulk_processing").addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def convert2xml(odf):
    """
    params: odf
    returns: content, metadata: content xml and metadata xml of the odf
    """
    myfile = zipfile.ZipFile(odf)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
            bh = myfile.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            content = ET.tostring(element, encoding='unicode')
        elif s.orig_filename == 'meta.xml':
            bh = myfile.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            metadataXML = ET.tostring(element, encoding='unicode')

    return content, metadataXML


def metadata_title_date_extraction(metadataXML):
    """
    param: metadataXML: metadata of odf file
    returns: modification date of odf
    """
    soup = BeautifulSoup(metadataXML, "lxml")
    metadata = soup.find("ns0:meta")
    title = metadata.find("dc:title").get_text()
    date = datetime.datetime.strptime(
        metadata.find("dc:date").get_text()[
            :10], '%Y-%m-%d')
    return title, date


def xml2text(xml):
    """
    params: xml
    returns: text
    """
    soup = BeautifulSoup(xml, "lxml")
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\\s+", " ", text)


def write_text(text, document_uid, destination_bucket):
    '''Write the extracted text to a .txt file in the staging bucket'''

    open(f'{destination_bucket}/{document_uid}.txt', 'w+').write(text)
    logger.debug(f'Saved text to {destination_bucket}')

def odf_converter(file_path, document_uid, save_path):
    doc_bytes_io = io.BytesIO(open(file_path))

    # Extract the content and metadata xml
    contentXML, metadataXML = convert2xml(odf=doc_bytes_io)
    text = xml2text(xml=contentXML)

    # Extract the publishing date
    title, date_published = metadata_title_date_extraction(metadataXML=metadataXML)

    logger.debug(f'Extracted title: {title}'
                f'Publishing date: {date_published}'
                f'UUID obtained is: {document_uid}')

    write_text(text=text, document_uid=document_uid, destination_bucket=save_path)
    
    return text, title, date_published
