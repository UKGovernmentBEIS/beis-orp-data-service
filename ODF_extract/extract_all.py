from odf import text, teletype
from odf.opendocument import load
import datefinder
import os
import sys
import zipfile
import xml.dom.minidom
import lxml.etree as etree
import xml.etree.ElementTree as ET

path = "/Users/thomas/Documents/BEIS/input_data/ODF/OpenDocument-v1.2-os.odt"
sample_ODF = load(path)
elements = sample_ODF.getElementsByType(text.P)

def title_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: title Str: the title of the document where the attribute value
        is equal to "Title"
    """
    for element in elements:
        el_attributes = element.attributes
        if list(el_attributes.values())[0] == "Title":
            title = teletype.extractText(element)
            return title

def publishing_date_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: match Str: date found in the footer of the document
    """
    for element in elements:
        if list(element.values())[0] == "Footer":
            text = teletype.extractText(element)
            matches = datefinder.find_dates(text, strict=True) # Set strict to True to collect well formed dates
            # If length of matches is greater than 1, return the last strict date format found
            if len(matches) > 1:
                return matches[-1]
            # Else return the date found
            else:
                for match in matches:
                    return str(match)

def text_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: texts Str: all text in the document
    """
    texts = []
    for element in elements:
        text = teletype.extractText(element)
        texts.append(text)
    return "\n".join(texts)


def convert2xml(path):

    myfile = zipfile.ZipFile(path)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
                fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/output.xml",'w')
                bh = myfile.read(s.orig_filename)
                # x = etree.parse(bh)
                # fd.write(etree.tostring(x, pretty_print = True, encoding='unicode'))
                # fd.close()
                element = ET.XML(bh)
                ET.indent(element)
                fd.write(ET.tostring(element, encoding='unicode'))

