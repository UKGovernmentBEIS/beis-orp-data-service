import io
import re
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bulk_processing")

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
    date_published = pd.to_datetime(
        metadata.find("dc:date").get_text()).isoformat()

    return title, date_published


def xml2text(xml):
    """
    params: xml
    returns: text
    """
    soup = BeautifulSoup(xml, "lxml")
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\\s+", " ", text)


def odf_converter(doc_bytes_io):

    # Extract the content and metadata xml
    contentXML, metadataXML = convert2xml(odf=doc_bytes_io)
    text = xml2text(xml=contentXML)

    # Extract the publishing date
    title, date_published = metadata_title_date_extraction(metadataXML=metadataXML)
    return text, title, date_published
