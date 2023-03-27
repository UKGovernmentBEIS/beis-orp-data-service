import spacy
from spacy.matcher import Matcher


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

from pyspark import SparkFiles
def initialise_matcher():
    """
    Initalises nlp and matcher from Spacy
    """
    nlp = spacy.load("en_core_web_sm")
    matcher = Matcher(nlp.vocab)
    matcher.add("date", [pattern1])
    matcher.add("date", [pattern2])
    matcher.add("date", [pattern3])
    matcher.add("date", [pattern4])
    matcher.add("date", [pattern5])
    return nlp, matcher