import re
import spacy
import string
import datetime
import datefinder
from spacy.matcher import Matcher
from dateutil.relativedelta import relativedelta


nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)

# Add match ID "HelloWorld" with no callback and one pattern
month_list = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
shorthand_month_list = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sept", "oct", "nov", "dec"]
digit_day_list = ["01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16", "17","18","19","20","21","22","23","24","25","26","27","27","28","29","30","31"]
digit_month_list =  ["01","02","03","04","05","06","07","08","09","10","11","12"]
# No zero
digit_day_list_nozero = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16", "17","18","19","20","21","22","23","24","25","26","27","27","28","29","30","31"]
digit_month_list_nozero =  ["1","2","3","4","5","6","7","8","9","10","11","12"]

# Long hand date patterns
pattern1 = [{"IS_DIGIT": True, "OP": "?"}, {"TEXT": {"IN" :month_list}}, {"IS_DIGIT": True}]
# Long hand date patterns w/ punctuation
pattern2 = [{"IS_DIGIT": True}, {"IS_PUNCT" : True, "OP": "?"}, {"TEXT": {"IN" :month_list}}, {"IS_PUNCT" : True, "OP": "?"}, {"IS_DIGIT": True}]
# Short hand date patterns
pattern3 = [{"IS_DIGIT": True}, {"IS_PUNCT" : True, "OP": "?"}, {"TEXT": {"IN" :shorthand_month_list}}, {"IS_PUNCT" : True, "OP": "?"}, {"IS_DIGIT": True}]
# All numbers
pattern4 = [{"TEXT": {"IN" :digit_day_list}, "OP": "?"}, {"IS_PUNCT" : True, "OP": "?"}, {"TEXT": {"IN" :digit_month_list}}, {"IS_PUNCT" : True}, {"IS_DIGIT": True}]
# All numbers - no zero
pattern5 = [{"TEXT": {"IN" :digit_day_list_nozero}, "OP": "?"}, {"IS_PUNCT" : True, "OP": "?"}, {"TEXT": {"IN" :digit_month_list_nozero}}, {"IS_PUNCT" : True}, {"IS_DIGIT": True}]


matcher.add("date", [pattern1])
matcher.add("date", [pattern2])
matcher.add("date", [pattern3])
matcher.add("date", [pattern4])
matcher.add("date", [pattern5])


def addSpace(text):
    return re.sub(r'(?<=([a-z])|\d)(?=(?(1)\d|[a-z]))', ' ', text)

ODF = "/Users/thomas/Documents/BEIS/repo/beis-orp-data-service/ODF_extract/text_output.txt"
other_txt = "/Users/thomas/Documents/BEIS/input_data/uuid_txt/1c45ffda181c44a1aaa7c087132551ae.txt"

with open(other_txt) as f:
    textObject = f.read()
    txt = str(textObject).lower()
    # Add spaces between digits and str characters
    txt = re.sub(r"(?i)(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)", " ", txt)
    clean_words = ''.join(' / ' if c in string.punctuation else c for c in txt)
    clean = re.sub(r"\s+", ' ', clean_words) 


def standardise_date(date):
    matches = datefinder.find_dates(date)
    date_matches = [str(date) for date in matches]
    return date_matches


def clean_date(candidate_dates):
    if len(candidate_dates) == 0:
        return None
    else:
        date_list = []
        for date in candidate_dates:
            if re.search('[a-zA-Z]', date):
                date_list.append(standardise_date(date))
            elif len(date.split(" / ")[-1]) < 4:
                if date.split(" / ")[-1][0] == "9" or date.split(" / ")[-1][0] == "8" or date.split(" / ")[-1][0] == "7":
                    date = "".join(date.split(" / ")[:-1]) + " / " + "19" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
                else: 
                    date = "".join(date.split(" / ")[:-1]) + " / " + "20" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
        return date_list


def find_date(clean):
    doc = nlp(clean)
    matches = matcher(doc)

    candidate_dates = []
    for match_id, start, end in matches:
        string_id = nlp.vocab.strings[match_id]  # Get string representation
        span = doc[start:end]  # The matched span
        candidate_dates.append(str(span).title())

    date_list = clean_date(candidate_dates)
    return date_list


def check_metadata_date_in_doc(metadata_date, date_list):
    margin = relativedelta(months = 3)

    datetime_obj = datetime.datetime.strptime(metadata_date[0], '%Y-%m-%d %H:%M:%S')
    upper_date = datetime_obj + margin
    lower_date = datetime_obj - margin

    for date in date_list:
        date =  datetime.datetime.strptime(date[0], '%Y-%m-%d %H:%M:%S')
        if  upper_date > date > lower_date:
            return date
        else: 
            return metadata_date


date_list = find_date(clean)
print(date_list)
print(check_metadata_date_in_doc(standardise_date("2015-02-16"), date_list = date_list))

# if find_date(clean) < standardise_date("19/12/2020"):
#     print("yes")
