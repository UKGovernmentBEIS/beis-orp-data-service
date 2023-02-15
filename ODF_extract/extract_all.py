from odf import text, teletype
from odf.opendocument import load
import datefinder
import os
import sys
import zipfile
import xml.dom.minidom
import lxml.etree as etree
import xml.etree.ElementTree as ET

path1 = "/Users/thomas/Documents/BEIS/input_data/ODF/OpenDocument-v1.2-os.odt"
path2 = "/Users/thomas/Documents/BEIS/input_data/ODF/Consultation technically competent manager attendance consultation document.odt"


def title_extraction(path):
    """
    params: elements: odf.element.Element
    returns: title Str: the title of the document where the attribute value
        is equal to "Title"
    """
    ODF = load(path)
    elements = ODF.getElementsByType(text.P)
    titles = []
    for element in elements:
        el_attributes = element.attributes
        if "title" in str(list(el_attributes.values())[0]).lower() and "sub" not in str(list(el_attributes.values())[0]).lower():
            title = teletype.extractText(element)
            titles.append(title)
    return titles[0]


def publishing_date_extraction(path):
    """
    params: elements: odf.element.Element
    returns: match Str: date found in the footer of the document
    """
    ODF = load(path)
    elements = ODF.getElementsByType(text.P)
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
    text_list = []
    for element in elements:
        text = teletype.extractText(element)
        text_list.append(text)
    return "\n".join(text_list)


def convert2xml(path, output):

    myfile = zipfile.ZipFile(path)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
                fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + output,'w')
                bh = myfile.read(s.orig_filename)
                element = ET.XML(bh)
                ET.indent(element)
                fd.write(ET.tostring(element, encoding='unicode'))

    return ET.tostring(element, encoding='unicode')


from bs4 import BeautifulSoup
import re

def xml2text(xml):
    soup = BeautifulSoup(xml, "lxml")   
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\s+", " ", text)


def date_extraction(path):
    """

    """
    ODF = load(path)
    elements = ODF.getElementsByType(text.P)
    dates = []
    for element in elements:
        el_attributes = element.attributes
        if "subtitle" in str(list(el_attributes.values())[0]).lower():
            date = str(teletype.extractText(element))
            dates.append(date)
    print(" ".join(dates))
    matches = datefinder.find_dates(" ".join(dates))
    date_matches = [str(date) for date in matches]
    return date_matches[0]


if __name__ == "__main__":
    # convert2xml(path1, "output.xml")
    # print(date_extraction(path2))
    # print(title_extraction(path2))

    # ODF = load(path2)
    # elements = ODF.getElementsByType(text.P)
    # text = text_extraction(elements)
    # fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + "text_output.txt",'w')
    # fd.write(str(text))

    xml = convert2xml(path2, "EAoutput.xml")
    text = xml2text(xml)
    fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + "text_output.txt",'w')
    fd.write(str(text))