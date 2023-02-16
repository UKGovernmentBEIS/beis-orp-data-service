from odf import text, teletype
from odf.opendocument import load
import datefinder
import os
import sys
import zipfile
import xml.dom.minidom
import lxml.etree as etree
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import re
from htmldate import find_date

path1 = "/Users/thomas/Documents/BEIS/input_data/ODF/OpenDocument-v1.2-os.odt"
path2 = "/Users/thomas/Documents/BEIS/input_data/ODF/Consultation technically competent manager attendance consultation document.odt"

ODF = load(path1)
elements = ODF.getElementsByType(text.P)

def title_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: title Str: the title of the document where the attribute value
        is equal to "Title"
    """
    titles = []
    for element in elements:
        el_attributes = element.attributes
        if "title" in str(list(el_attributes.values())[0]).lower() and "sub" not in str(list(el_attributes.values())[0]).lower():
            title = teletype.extractText(element)
            titles.append(title)
    if len(titles) > 0:
        return titles[0]
    else:
        return None


# def publishing_date_extraction(path):
#     """
#     params: elements: odf.element.Element
#     returns: match Str: date found in the footer of the document
#     """
#     ODF = load(path)
#     elements = ODF.getElementsByType(text.P)
#     for element in elements:
#         if list(element.values())[0] == "Footer":
#             text = teletype.extractText(element)
#             matches = datefinder.find_dates(text, strict=True) # Set strict to True to collect well formed dates
#             # If length of matches is greater than 1, return the last strict date format found
#             if len(matches) > 1:
#                 return matches[-1]
#             # Else return the date found
#             else:
#                 for match in matches:
#                     return str(match)


def convert2xml(path):

    myfile = zipfile.ZipFile(path)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
                # fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + f"{output}_content.xml",'w')
                bh = myfile.read(s.orig_filename)
                element = ET.XML(bh)
                ET.indent(element)
                # fd.write(ET.tostring(element, encoding='unicode'))
                content = ET.tostring(element, encoding='unicode')
        elif s.orig_filename == 'meta.xml':
                # fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + f"{output}_meta.xml",'w')
                bh = myfile.read(s.orig_filename)
                element = ET.XML(bh)
                ET.indent(element)
                # fd.write(ET.tostring(element, encoding='unicode'))
                metadata = ET.tostring(element, encoding='unicode')

    return content, metadata


def xml2text(xml):
    soup = BeautifulSoup(xml, "lxml")   
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\s+", " ", text)


import datetime
def metadata_title_date_extraction(metadataXML):
    soup = BeautifulSoup(metadataXML, "lxml")   
    metadata = soup.find("ns0:meta")
    title= metadata.find("dc:title").get_text()
    date = datetime.datetime.strptime(re.sub(r'[a-zA-Z]', r' ', metadata.find("dc:date").get_text()).strip(), '%Y-%m-%d %H:%M:%S')
    return title, date


if __name__ == "__main__":

    content, metadataXML = convert2xml(path2)
    print(metadata_title_date_extraction(metadataXML=metadataXML))
    # text = xml2text(xml)
    # fd = open("/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/" + "odf_text_output.txt",'w')
    # fd.write(str(text))


    # title = title_extraction(elements)
    # print(title)
    # ti_c = os.path.getctime(path1)
    # ti_m = os.path.getmtime(path1)
    # print(time.ctime(ti_m))
