import boto3
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTChar, LTFigure, LTTextBox, LTTextLine
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import logging
import io

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

MIN_CHARS = 6
MAX_WORDS = 20
MAX_CHARS = MAX_WORDS * 10
TOLERANCE = 1e-06


def make_parsing_state(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type("ParsingState", (), enums)


CHAR_PARSING_STATE = make_parsing_state(
    "INIT_X", "INIT_D", "INSIDE_WORD")


def empty_str(s):
    return len(s.strip()) == 0


def is_close(a, b, relative_tolerance=TOLERANCE):
    return abs(a - b) <= relative_tolerance * max(abs(a), abs(b))


def update_largest_text(line, y0, size, largest_text):
    # Sometimes font size is not correctly read, so we
    # fallback to text y0 (not even height may be calculated).
    # In this case, we consider the first line of text to be a title.
    if (size == largest_text["size"] == 0) and (
            y0 - largest_text["y0"] < -TOLERANCE):
        return largest_text

    # If it is a split line, it may contain a new line at the end
    line = re.sub(r"\n$", " ", line)

    if size - largest_text["size"] > TOLERANCE:
        largest_text = {"contents": line, "y0": y0, "size": size}
    # Title spans multiple lines
    elif is_close(size, largest_text["size"]):
        largest_text["contents"] = largest_text["contents"] + line
        largest_text["y0"] = y0

    return largest_text


def extract_largest_text(obj, largest_text):
    # Skip first letter of line when calculating size, as articles
    # may enlarge it enough to be bigger then the title size.
    # Also skip other elements such as `LTAnno`.
    for i, child in enumerate(obj):
        if isinstance(child, LTTextLine):
            for j, child2 in enumerate(child):
                if j > 1 and isinstance(child2, LTChar):
                    largest_text = update_largest_text(
                        child.get_text(), child2.y0, child2.size, largest_text
                    )
                    # Only need to parse size of one char
                    break
        elif i > 1 and isinstance(child, LTChar):
            largest_text = update_largest_text(
                obj.get_text(), child.y0, child.size, largest_text
            )
            # Only need to parse size of one char
            break
    return largest_text


def extract_figure_text(lt_obj, largest_text):
    """
    Extract text contained in a `LTFigure`.
    Since text is encoded in `LTChar` elements, we detect separate lines
    by keeping track of changes in font size.
    """
    text = ""
    line = ""
    y0 = 0
    size = 0
    char_distance = 0
    char_previous_x1 = 0
    state = CHAR_PARSING_STATE.INIT_X
    for child in lt_obj:

        # Ignore other elements
        if not isinstance(child, LTChar):
            continue

        char_y0 = child.y0
        char_size = child.size
        char_text = child.get_text()

        # A new line was detected
        if char_size != size:
            largest_text = update_largest_text(
                line, y0, size, largest_text)
            text += line + "\n"
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

            # Initialization
            if state == CHAR_PARSING_STATE.INIT_X:
                char_previous_x1 = child.x1
                state = CHAR_PARSING_STATE.INIT_D
            elif state == CHAR_PARSING_STATE.INIT_D:
                # Update distance only if no space is detected
                if (char_distance > 0) and (
                    char_current_distance < char_distance * 2.5
                ):
                    char_distance = char_current_distance
                if char_distance < 0.1:
                    char_distance = 0.1
                state = CHAR_PARSING_STATE.INSIDE_WORD
            # If the x-position decreased, then it's a new line
            if (state == CHAR_PARSING_STATE.INSIDE_WORD) and (
                child.x1 < char_previous_x1
            ):
                line += " "
                char_previous_x1 = child.x1
                state = CHAR_PARSING_STATE.INIT_D
            # Large enough distance: it's a space
            elif (state == CHAR_PARSING_STATE.INSIDE_WORD) and (
                char_current_distance > char_distance * 8.5
            ):
                line += " "
                char_previous_x1 = child.x1
            # When larger distance is detected between chars, use it to
            # improve our heuristic
            elif (
                (state == CHAR_PARSING_STATE.INSIDE_WORD)
                and (char_current_distance > char_distance)
                and (char_current_distance < char_distance * 2.5)
            ):
                char_distance = char_current_distance
                char_previous_x1 = child.x1
            # Chars are sequential
            else:
                char_previous_x1 = child.x1
            child_text = child.get_text()
            if not empty_str(child_text):
                line += child_text
    return (largest_text, text)


def clean_text(text):
    """
    Clean the title by removing
    illegal characters and adding / removing spaces
    """
    # Remove paragraphs
    text = re.sub("\n", " ", text)
    # Remove illegal characters
    text = re.sub(ILLEGAL_CHARACTERS_RE, " ", text)
    # Space out merged words by adding a space before a capital letter
    # if it appears after a lowercase letter
    text = re.sub(
        r"([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))",
        r"\1 ",
        text)
    # Take empty space off beginning and end of string
    text = text.strip()
    # Take out excess spaces
    text = re.sub("\\s+", " ", text)
    return text


def extract_title_and_text_from_all_pages(doc_bytes):
    parser = PDFParser(doc_bytes)
    doc = PDFDocument(parser, "")
    parser.set_document(doc)
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    text = ""
    largest_text = {"contents": "", "y0": 0, "size": 0}
    largest_text_per_page = []
    for page in PDFPage.get_pages(doc_bytes):
        # for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)
        layout = device.get_result()
        for lt_obj in layout:
            if isinstance(lt_obj, LTFigure):
                (largest_text, figure_text) = extract_figure_text(
                    lt_obj, largest_text)
                text += figure_text
            elif isinstance(lt_obj, (LTTextBox, LTTextLine)):
                # Ignore body text blocks
                stripped_to_chars = re.sub(
                    r"[ \t\n]", "", lt_obj.get_text().strip())
                if len(stripped_to_chars) > MAX_CHARS * 2:
                    continue
                largest_text = extract_largest_text(
                    lt_obj, largest_text)
                text += lt_obj.get_text() + "\n"
            largest_text_per_page.append(largest_text)

    cleaned_text = clean_text(text)

    title = largest_text_per_page[0]

    # Remove unprocessed CID text
    title["contents"] = re.sub(
        r"(\(cid:[0-9 \t-]*\))*",
        "",
        title["contents"])

    # Clean title
    title["contents"] = clean_text(title["contents"])

    return title["contents"], cleaned_text


def handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    object_size = event["Records"][0]["s3"]["object"]["size"]
    print(f"New document in {bucket_name}: {object_key}, with size: {object_size}")

    s3_client = boto3.client('s3')
    doc_stream = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )['Body']

    doc_bytes = doc_stream.read()
    title, text = extract_title_and_text_from_all_pages(io.BytesIO(doc_bytes))

    print(f"Title of document: {title} \n\n")
    print(f"Document text: \n {text}")
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
