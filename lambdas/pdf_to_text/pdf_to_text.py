import boto3
import pdfplumber
import difflib
from PyPDF2 import PdfFileReader, PdfReader
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTChar, LTFigure, LTTextBox, LTTextLine
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import io
import pymongo


MIN_CHARS = 6
MAX_WORDS = 20
MAX_CHARS = MAX_WORDS * 10
TOLERANCE = 1e-06

DESTINATION_BUCKET_NAME = "beis-orp-dev-datalake"

regulator_name_list = [
    "Health and Safety Executive",
    "HSE",
    "Ofgem",
    "Environmental Agency",
    "EA",
]


def make_parsing_state(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type("ParsingState", (), enums)


CHAR_PARSING_STATE = make_parsing_state("INIT_X", "INIT_D", "INSIDE_WORD")


def is_close(a, b, relative_tolerance=TOLERANCE):
    return abs(a - b) <= relative_tolerance * max(abs(a), abs(b))


def update_largest_text(line, y0, size, largest_text):
    # Sometimes font size is not correctly read, so we
    # fallback to text y0 (not even height may be calculated).
    # In this case, we consider the first line of text to be a title.
    if (size == largest_text["size"] == 0) and (y0 - largest_text["y0"] < -TOLERANCE):
        return largest_text

    # If it is a split line, it may contain a new line at the end
    line = re.sub(r"\n$", " ", line)

    if size - largest_text["size"] > TOLERANCE:
        largest_text["contents"] = line
        largest_text["y0"] = y0
        largest_text["size"] = size
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
    # Extract text contained in a `LTFigure`.
    # Since text is encoded in `LTChar` elements, we detect separate lines
    # by keeping track of changes in font size.
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
            largest_text = update_largest_text(line, y0, size, largest_text)
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
            if not len(child_text.strip()) == 0:
                line += child_text
    return (largest_text, text)


def clean_text(text):
    # Clean the title by removing
    # illegal characters and adding / removing spaces

    text = re.sub("\n", " ", text)
    text = re.sub(ILLEGAL_CHARACTERS_RE, " ", text)

    # Space out merged words by adding a space before a capital letter
    # if it appears after a lowercase letter
    text = re.sub(
        r"([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))",
        r"\1 ",
        text
    )
    text = text.strip()
    text = re.sub("\\s+", " ", text)
    text = text.lower()
    text = text.replace("\t", " ")
    text = re.sub("<.*?>", "", text)
    text = text.replace("_x000c_", "")
    text = re.sub("\\s+", " ", text)

    return text


def extract_title_and_text_from_all_pages(doc_bytes_io):
    # pdf_reader = PdfFileReader(doc_bytes_io)
    # title = pdf_reader.getDocumentInfo().title

    parser = PDFParser(doc_bytes_io)
    doc = PDFDocument(parser, "")
    parser.set_document(doc)
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    text = ""
    largest_text = {"contents": "", "y0": 0, "size": 0}
    largest_text_per_page = []

    for page in PDFPage.get_pages(doc_bytes_io):
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
                stripped_to_chars = re.sub(r"[ \t\n]", "", lt_obj.get_text().strip())
                if len(stripped_to_chars) > MAX_CHARS * 2:
                    continue

                largest_text = extract_largest_text(lt_obj, largest_text)
                text += lt_obj.get_text() + "\n"

            largest_text_per_page.append(largest_text)

    cleaned_text = clean_text(text)

    title = largest_text_per_page[0]

    title["contents"] = re.sub(
        r"(\(cid:[0-9 \t-]*\))*",
        "",
        title["contents"])

    # Clean title
    cleaned_title = clean_text(title["contents"])

    return cleaned_title, cleaned_text


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
        print("PDF is not machine readable")


def get_bold_text_from_pdf(pdf, page_number):
    """
    Extract text in bold text
    If the text in bold is empty
    Extract and return text from the page given
    """
    with pdfplumber.open(pdf) as opened_pdf:
        text = opened_pdf.pages[page_number]
        bold_text = text.filter(
            lambda obj: obj["object_type"] == "char" and "Bold" in obj["fontname"]
        )
        title = bold_text.extract_text()
        # If length of title is nothing, try take extracted words from
        # the page read as the title
        if (
            len(clean_text(title).split(" ")) <= 3
            or len(
                [
                    1
                    for regulator_name in regulator_name_list
                    if clean_text(title) == regulator_name
                ]
            )
            > 0
        ):
            return ""
        else:
            return clean_text(title)


def get_title_from_metadata(pdf):
    """
    Extract title from metadata of the pdf
    """
    with open(pdf, "rb") as f:
        pdf_reader = PdfFileReader(f)
        title = pdf_reader.getDocumentInfo().title
        return title


def metadata_title_similarity_score(meta_title, text):
    large_string = re.sub(r"[^\w\s]", "", text[0:300])
    query_string = re.sub(r"[^\w\s]", "", meta_title)
    for reg_name in regulator_name_list:
        query_string = re.sub(reg_name, "", query_string)
    s = difflib.SequenceMatcher(None, large_string, query_string)
    similarity_of_title_to_first_few_lines = sum(
        n for i, j, n in s.get_matching_blocks()
    ) / float(len(query_string) + 1)
    return similarity_of_title_to_first_few_lines


def get_title_and_text(pdf):
    """
    This function brings together all previous functions
    and applies heuristics for when to apply each function
    """
    # Get page number
    page_number = select_page_with_text(pdf)
    # Try get title from metadata first
    meta_title = str(get_title_from_metadata(pdf))
    clean_meta_title = clean_text(meta_title)
    # Get title and text from text
    title, text = extract_title_and_text_from_all_pages(pdf)
    clean_title = clean_text(title)
    # Define junk titles
    junk_titles = [
        "Date",
        "Microsoft Word",
        "email",
        "Enter your title here",
        "Email:",
        "To:",
        "Dear",
        "@",
    ]
    # Get similarity score
    similarity_score = metadata_title_similarity_score(
        meta_title, text)
    # If title is either none, too short, contains junk title
    # keywords, is entirely numeric, then get bold text from pdf
    if (
        (similarity_score < 0.7)
        or (clean_meta_title == "None")
        or (len(clean_meta_title.split(" ")) <= 3)
        or any(
            item in " ".join(clean_meta_title.split(" ")[0:10]) for item in junk_titles
        )
        or (re.sub(" ", "", re.sub(r"[^\w\s]", "", clean_meta_title)).isnumeric())
    ):
        # If the title text is still too short, is only numeric, or is
        # only regulator name
        if (
            (len(clean_title.split(" ")) <= 3)
            or (re.sub(" ", "", re.sub(r"[^\w\s]", "", clean_title)).isnumeric())
            or any(regulator_name == title for regulator_name in regulator_name_list)
        ):
            bold_title = get_bold_text_from_pdf(pdf, page_number)
            if bold_title == "":
                return " ".join(text.split(" ")[0:25]) + "...", text
            else:
                return bold_title, text
        else:
            return title, text
    else:
        return clean_text(meta_title), text


def cut_title(title):
    """
    Cuts title length down to 25 tokens
    """
    title = re.sub("Figure 1", "", title)
    title = re.sub(r"[^\w\s]", "", title)
    if len(str(title).split(" ")) > 25:
        title = " ".join(title.split(" ")[0:25]) + "..."
        return title
    else:
        return title


def extract_summary(text, title):
    """
    Define function to create a summary of the document
    i.e first 80 characters of the document
    """
    summary = ' '.join(re.sub(title, "", clean_text(text)).split(' ')[:80])
    return summary


def handler(event, context):
    source_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    s3_client = boto3.client("s3")

    doc_stream = s3_client.get_object(
        Bucket=source_bucket_name,
        Key=object_key
    )["Body"]

    metadata = s3_client.head_object(
        Bucket=source_bucket_name,
        Key=object_key
    )["Metadata"]

    doc_bytes = doc_stream.read()
    doc_bytes_io = io.BytesIO(doc_bytes)

    title, text = extract_title_and_text_from_all_pages(doc_bytes_io)
    title = cut_title(title)
    summary = extract_summary(text, title)

    uuid = metadata["uuid"]

    print(
        f"New document in {source_bucket_name}: {object_key}")
    print(f"Title of document: {title}")
    print(f"UUID obtained is: {uuid}")

    # Create a MongoDB client and open a connection to Amazon DocumentDB
    print("Connecting to DocumentDB")
    db_client = pymongo.MongoClient(
        ("mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc."
         "eu-west-2.docdb.amazonaws.com:27017/?directConnection=true"),
        tls=True,
        tlsCAFile="./rds-combined-ca-bundle.pem"
    )
    print("Connected to DocumentDB")

    db = db_client.bre_orp
    collection = db.documents

    doc = {
        "title": title,
        "document_uid": uuid,
        "summary": summary,
        "uri": f"s3://{source_bucket_name}/{object_key}",
        "object_key": object_key
    }

    # Insert document to DB if it doesn't already exist
    if not collection.find_one(doc):
        collection.insert_one(doc)
        print("Inserted document to DocumentDB")

    # Test query and print the result to the screen
    print(f"Document inserted: {collection.find_one(doc)}")
    db_client.close()

    print("Saving text to data lake")
    s3_client.put_object(
        Body=text,
        Bucket=DESTINATION_BUCKET_NAME,
        Key=f"processed/{uuid}.txt",
        Metadata={
            "uuid": uuid
        }
    )
    print("Saved text to data lake")

    return {
        "statusCode": 200,
        "document_uid": uuid
    }
