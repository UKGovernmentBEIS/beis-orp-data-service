import re
import string
import datetime
import datefinder
from add_patterns import initialise_matcher
from dateutil.relativedelta import relativedelta


import logging
logger = logging.getLogger("Bulk_processing").addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


# Initalise the matcher
nlp, matcher = initialise_matcher()


def preprocess_text(text):
    """
    param: text: str
    returns: clean_text: text more easily read by matcher
    """
    txt = text.lower()
    # Add spaces between digits and str characters
    txt = re.sub(r"(?i)(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)", " ", txt)
    clean_words = ''.join(' / ' if c in string.punctuation else c for c in txt)
    clean_text = re.sub(r"\s+", ' ', clean_words)
    return clean_text


def standardise_date(date):
    """
    param date: datetime
    returns date_matches: List of dates found
    """
    matches = datefinder.find_dates(date)
    date_matches = [str(date) for date in matches]
    return date_matches

# TODO  check if this is unnecessary 
def clean_date(candidate_dates):
    """
    param: candidate_dates: List of dates found from text
    returns: date_list: cleaned List of dates found from text
    """
    if len(candidate_dates) == 0:
        return None
    else:
        date_list = []
        for date in candidate_dates:
            if re.search('[a-zA-Z]', date):
                date_list.append(standardise_date(date))
            elif len(date.split(" / ")[-1]) < 4:
                if date.split(
                        " / ")[-1][0] == "9" or date.split(" / ")[-1][0] == "8" or date.split(" / ")[-1][0] == "7":
                    date = "".join(date.split(
                        " / ")[:-1]) + " / " + "19" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
                else:
                    date = "".join(date.split(
                        " / ")[:-1]) + " / " + "20" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
        return date_list


def find_date(clean_text):
    """
    param: clean_text: text from preprocess_text function
    returns: date_list: list of dates found from text
    """
    doc = nlp(clean_text)
    matches = matcher(doc)

    candidate_dates = []
    for _, start, end in matches:
        # string_id = nlp.vocab.strings[match_id]  # Get string representation
        span = doc[start:end]  # The matched span
        candidate_dates.append(str(span).title())

    date_list = clean_date(candidate_dates)
    return date_list


def check_metadata_date_in_doc(metadata_date, date_list):
    """
    param: metadata_date: date pulled from document's metadata
    param: date_list: list of dates from the cleaned text
    returns: date / metadata_date: either date from text or metadata date
        If any date extracted from the text is within 3 months of the metadata date, return this date
    """
    margin = relativedelta(months=3)

    datetime_obj = datetime.datetime.strptime(metadata_date, '%Y-%m-%d %H:%M:%S')
    upper_date = datetime_obj + margin
    lower_date = datetime_obj - margin

    for date in date_list:
        date = datetime.datetime.strptime(date[0], '%Y-%m-%d %H:%M:%S')
        if upper_date >= date >= lower_date:
            return date
        else:
            return metadata_date

def data_generation(text, metadata_date):
    clean_text = preprocess_text(text=text)

    # Extract date from text
    date_list = find_date(clean_text=clean_text)

    # Check if metadata date appears near dates found
    date = check_metadata_date_in_doc(metadata_date=metadata_date, date_list=date_list)
    return date
