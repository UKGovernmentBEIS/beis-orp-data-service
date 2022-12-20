import boto3
from PyPDF2 import PdfFileReader
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
    pdf_reader = PdfFileReader(doc_bytes_io)
    title = pdf_reader.getDocumentInfo().title

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

    # Remove unprocessed CID text
    title = re.sub(
        r"(\(cid:[0-9 \t-]*\))*",
        "",
        title
    )

    # Clean title
    cleaned_title = clean_text(title)

    return cleaned_title, cleaned_text


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
        "documentUUID": uuid
    }
