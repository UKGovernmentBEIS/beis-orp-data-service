# Import modules
import zipfile
import xml.etree.ElementTree as ET
import docx

# Defining elements from openxml schema
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'

# Function from stackoverflow to pull text from a docx file
def get_docx_text(path):
    """
    param: path Str: Take the path of a docx file as argument
    returns: paragraphs Str: text in unicode.
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

# Function from stackoverflow to get metadata from docx
def getMetaData(doc):
    """
    param: doc docx
    returns: metadata
    """
    metadata = {}
    prop = doc.core_properties
    metadata["author"] = prop.author
    metadata["category"] = prop.category
    metadata["comments"] = prop.comments
    metadata["content_status"] = prop.content_status
    metadata["created"] = prop.created
    metadata["identifier"] = prop.identifier
    metadata["keywords"] = prop.keywords
    metadata["last_modified_by"] = prop.last_modified_by
    metadata["language"] = prop.language
    metadata["modified"] = prop.modified
    metadata["subject"] = prop.subject
    metadata["title"] = prop.title
    metadata["version"] = prop.version
    return metadata

# Get text
text = get_docx_text("/Users/thomas/Documents/BEIS/scraper/ONR/onr_documents/Nuclear safety_NS-TAST-GD-003 (Issue 9.2)_September 2024_Safety Systems.docx")

# Get metadata
doc = docx.Document("/Users/thomas/Documents/BEIS/scraper/ONR/onr_documents/Nuclear safety_NS-TAST-GD-003 (Issue 9.2)_September 2024_Safety Systems.docx")
metadata = getMetaData(doc)

# Get publishing date from metadata
publishing_date = metadata["modified"]