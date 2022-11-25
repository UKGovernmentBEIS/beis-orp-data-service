from pdfminer.layout import LTTextContainer, LTChar, LTTextLine
from pdfminer.high_level import extract_pages
# from PyPDF2 import PdfReader, PdfFileReader
import pandas as pd
import os
import pdfplumber
import unidecode
# from PyPDF4 import PdfFileReader
from PyPDF2 import PdfReader, PdfFileReader
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser 
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTChar, LTFigure, LTTextBox, LTTextLine
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


import logging
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

# Define regulators
regulator_name_list = ["Health and Safety Executive", "Ofgem", "Environmental Agency"]


def make_parsing_state(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('ParsingState', (), enums)
CHAR_PARSING_STATE = make_parsing_state('INIT_X', 'INIT_D', 'INSIDE_WORD')

def log(text):
    if IS_LOG_ON:
        print('--- ' + text)
IS_LOG_ON = False

MIN_CHARS = 6
MAX_WORDS = 20
MAX_CHARS = MAX_WORDS * 10
TOLERANCE = 1e-06

def empty_str(s):
    return len(s.strip()) == 0

def is_close(a, b, relative_tolerance=TOLERANCE):
    return abs(a-b) <= relative_tolerance * max(abs(a), abs(b))

def update_largest_text(line, y0, size, largest_text):
    log('update size: ' + str(size))
    log('largest_text size: ' + str(largest_text['size']))

    # Sometimes font size is not correctly read, so we
    # fallback to text y0 (not even height may be calculated).
    # In this case, we consider the first line of text to be a title.
    if ((size == largest_text['size'] == 0) and (y0 - largest_text['y0'] < -TOLERANCE)):
        return largest_text

    # If it is a split line, it may contain a new line at the end
    line = re.sub(r'\n$', ' ', line)

    if (size - largest_text['size'] > TOLERANCE):
        largest_text = {
            'contents': line,
            'y0': y0,
            'size': size
        }
    # Title spans multiple lines
    elif is_close(size, largest_text['size']):
        largest_text['contents'] = largest_text['contents'] + line
        largest_text['y0'] = y0

    return largest_text

def extract_largest_text(obj, largest_text):
    # Skip first letter of line when calculating size, as articles
    # may enlarge it enough to be bigger then the title size.
    # Also skip other elements such as `LTAnno`.
    for i, child in enumerate(obj):
        if isinstance(child, LTTextLine):
            log('lt_obj child line: ' + str(child))
            for j, child2 in enumerate(child):
                if j > 1 and isinstance(child2, LTChar):
                    largest_text = update_largest_text(child.get_text(), child2.y0, child2.size, largest_text)
                    # Only need to parse size of one char
                    break
        elif i > 1 and isinstance(child, LTChar):
            log('lt_obj child char: ' + str(child))
            largest_text = update_largest_text(obj.get_text(), child.y0, child.size, largest_text)
            # Only need to parse size of one char
            break
    return largest_text
    
def extract_figure_text(lt_obj, largest_text):
    """
    Extract text contained in a `LTFigure`.
    Since text is encoded in `LTChar` elements, we detect separate lines
    by keeping track of changes in font size.
    """
    text = ''
    line = ''
    y0 = 0
    size = 0
    char_distance = 0
    char_previous_x1 = 0
    state = CHAR_PARSING_STATE.INIT_X
    for child in lt_obj:
        log('child: ' + str(child))

        # Ignore other elements
        if not isinstance (child, LTChar):
            continue

        char_y0 = child.y0
        char_size = child.size
        char_text = child.get_text()
        decoded_char_text = unidecode.unidecode(char_text.encode('utf-8').decode('utf-8'))
        log('char: ' + str(char_size) + ' ' + str(decoded_char_text))

        # A new line was detected
        if char_size != size:
            log('new line')
            largest_text = update_largest_text(line, y0, size, largest_text)
            text += line + '\n'
            line = char_text
            y0 = char_y0
            size = char_size

            char_previous_x1 = child.x1
            state = CHAR_PARSING_STATE.INIT_D
        else:
            # Spaces may not be present as `LTChar` elements,
            # so we manually add them.
            # NOTE: A word starting with lowercase can't be
            # distinguished from the current word.
            char_current_distance = abs(child.x0 - char_previous_x1)
            log('char_current_distance: ' + str(char_current_distance))
            log('char_distance: ' + str(char_distance))
            log('state: ' + str(state))

            # Initialization
            if state == CHAR_PARSING_STATE.INIT_X:
                char_previous_x1 = child.x1
                state = CHAR_PARSING_STATE.INIT_D
            elif state == CHAR_PARSING_STATE.INIT_D:
                # Update distance only if no space is detected
                if (char_distance > 0) and (char_current_distance < char_distance * 2.5):
                    char_distance = char_current_distance
                if (char_distance < 0.1):
                    char_distance = 0.1
                state = CHAR_PARSING_STATE.INSIDE_WORD
            # If the x-position decreased, then it's a new line
            if (state == CHAR_PARSING_STATE.INSIDE_WORD) and (child.x1 < char_previous_x1):
                log('x-position decreased')
                line += ' '
                char_previous_x1 = child.x1
                state = CHAR_PARSING_STATE.INIT_D
            # Large enough distance: it's a space
            elif (state == CHAR_PARSING_STATE.INSIDE_WORD) and (char_current_distance > char_distance * 8.5):
                log('space detected')
                log('char_current_distance: ' + str(char_current_distance))
                log('char_distance: ' + str(char_distance))
                line += ' '
                char_previous_x1 = child.x1
            # When larger distance is detected between chars, use it to
            # improve our heuristic
            elif (state == CHAR_PARSING_STATE.INSIDE_WORD) and (char_current_distance > char_distance) and (char_current_distance < char_distance * 2.5):
                char_distance = char_current_distance
                char_previous_x1 = child.x1
            # Chars are sequential
            else:
                char_previous_x1 = child.x1
            child_text = child.get_text()
            if not empty_str(child_text):
                line += child_text
    return (largest_text, text)

def pdf_text(filename):
    fp = open(filename, 'rb')
    parser = PDFParser(fp)
    doc = PDFDocument(parser, '')
    parser.set_document(doc)
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    text = ''
    largest_text = {
        'contents': '',
        'y0': 0,
        'size': 0
    }
    for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)
        layout = device.get_result()
        for lt_obj in layout:
            log('lt_obj: ' + str(lt_obj))
            if isinstance(lt_obj, LTFigure):
                (largest_text, figure_text) = extract_figure_text(lt_obj, largest_text)
                text += figure_text
            elif isinstance(lt_obj, (LTTextBox, LTTextLine)):
                # Ignore body text blocks
                stripped_to_chars = re.sub(r'[ \t\n]', '', lt_obj.get_text().strip())
                if (len(stripped_to_chars) > MAX_CHARS * 2):
                    continue

                largest_text = extract_largest_text(lt_obj, largest_text)
                text += lt_obj.get_text() + '\n'

        # Remove unprocessed CID text
        largest_text['contents'] = re.sub(r'(\(cid:[0-9 \t-]*\))*', '', largest_text['contents'])

        # Clean title
        largest_text['contents'] = clean_title(largest_text['contents'])

        # Only parse the first page
        return largest_text['contents'] 


def select_page_with_text(pdf):
    """
    Open and read a pdf
    Extract the number of pages
    Extract from text from each page
    The first page with length of text greater than 10 is returned
    """
    try:
        # Find number of pages in pdf
        with open(pdf, "rb") as f:
            number_of_pages = PdfReader(f).numPages
            f.close()
            pdf = pdfplumber.open(pdf) 
            for page_number in range(0, number_of_pages):
                text = pdf.pages[page_number].extract_text()
                if len(text) > 10:
                    return page_number
                else:
                    continue
    except AttributeError:
        logger.info("PDF is not machine readable")


def clean_title(title):
    """
    Clean the title by removing 
    illegal characters and adding / removing spaces
    """
    # Remove illegal characters
    title = re.sub(ILLEGAL_CHARACTERS_RE, " ", title)
    # Space out merged words by adding a space before a capital letter if it appears after a lowercase letter
    title = re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1 ', title)
    # Take empty space off beginning and end of string
    title = title.strip()
    # Take out excess spaces
    title = re.sub('\s+'," ",title)
    return title


def get_bold_text_from_pdf(pdf, page_number):
    """
    Extract text in bold text
    If the text in bold is empty
    Extract and return text from the page given
    """
    with pdfplumber.open(pdf) as opened_pdf: 
        text = opened_pdf.pages[page_number]
        clean_text = text.filter(lambda obj: obj["object_type"] == "char" and "Bold" in obj["fontname"])
        title = clean_text.extract_text()
        # If length of title is nothing, try take extracted words from the page read as the title
        if len(title) == 0:
            text_from_page = " ".join(clean_text.extract_text().split(" ")[0 : 25]) + "..."
            # If unable to extract text with pdfplumber, try with PyPDF2
            if text_from_page == "...":
                with open(pdf, "rb") as opened_pdf:
                    reader = PdfReader(opened_pdf) 
                    # Pages to read
                    page_to_be_read = reader.pages[page_number]
                    # Find dates from pages
                    text_from_page = page_to_be_read.extract_text()
                    # Clean title
                    text_from_page = clean_title(text_from_page)
                    return text_from_page
            else:
                return clean_title(text_from_page)
        else:
            return clean_title(title)


def get_title_from_metadata(pdf):
    """
    Extract title from metadata of the pdf
    """
    with open(pdf, "rb") as f:
        pdf_reader = PdfFileReader(f) 
        title = pdf_reader.getDocumentInfo().title  
        return title


def get_titles(pdf):
    """
    This function brings together all previous functions
    and applies heuristics for when to apply each function 
    """
    # Get page number
    page_number = select_page_with_text(pdf)
    logger.info(page_number)
    # Try get title from metadata first
    title = str(get_title_from_metadata(pdf))
    junk_titles = ["Date", "Microsoft Word", "email", "Enter your title here", "Email:", "To:", "Dear"]
    # If title is either none, too short, contains junk title keywords, is entirely numeric, then get bold text from pdf
    if (title == "None" or len(title.split(" ")) < 3) or any(item in " ".join(title.split(" ")[0:10]) for item in junk_titles) or (re.sub(" ","", re.sub(r'[^\w\s]',"",title)).isnumeric()):
        title = pdf_text(pdf)
        # If the title text is still too short, is only numeric, or is only regulator name
        if len(title.split(" ")) < 3 or (re.sub(" ","", re.sub(r'[^\w\s]',"",title)).isnumeric()) or any(regulator_name == title for regulator_name in regulator_name_list):
            title = get_bold_text_from_pdf(pdf, page_number)
            return title
        else:
            return title
    else:
        return clean_title(title)


def cut_title(title):
    """
    Cuts title length down to 25 tokens
    """
    if len(title.split(" ")) > 25:
        title = " ".join(title.split(" ")[0 : 25]) + "..."
        return title
    else:
        return title


####################### Iterate through pdfs in all_pdfs ########################


pdfs = []
title_list = []

for pdf in os.listdir(f"/Users/thomas/Documents/BEIS/input_data/all_pdfs/"):
    if pdf == ".DS_Store":
        continue
    else:
        logger.info(pdf)
        pdfs.append(pdf)
        title_found = get_titles(f"/Users/thomas/Documents/BEIS/input_data/all_pdfs/{pdf}")
        title = cut_title(title_found)
        title_list.append(title)

logger.info(len(pdfs))
logger.info(len(title_list))

# Output to excel
pd.DataFrame({"PDFs" : pdfs, "Title" : title_list}).to_excel("titles_from_pdfsv4.xlsx", engine = "openpyxl")