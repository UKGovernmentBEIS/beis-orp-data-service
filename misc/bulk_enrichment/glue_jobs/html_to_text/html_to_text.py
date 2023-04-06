import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from htmldate import find_date
from html_to_text.govuk_extraction import get_content

def get_title_and_text(URL):
    '''
    params: req: request URL
    returns: title, text: Str
    '''
    req = requests.get(URL)
    soup = BeautifulSoup(req.text, 'html.parser')

    title = str(soup.head.title.get_text())
    text = re.sub(
        "\\s+", " ", str(soup.body.find(id="contentContainer").get_text()).replace("\n", " "))

    return title, text


def get_publication_modification_date(URL):
    '''
    params: URL: Str
    returns: publication_date: Str
    '''
    # Initally disable extensive search
    publication_date = str(
        find_date(URL, original_date=True, extensive_search=False))
    modification_date = str(find_date(URL, extensive_search=False))

    # If no concrete date is found, do extensive search
    if (publication_date == 'None') and (modification_date == 'None'):
        publication_date = str(find_date(URL, original_date=True))

    publication_date = pd.to_datetime(publication_date).isoformat()

    return publication_date


def html_converter(url):

    if "https://www.gov.uk/" in url:
        text, title, date_published = get_content(url)

    else:
        title, text = get_title_and_text(url)
        date_published = get_publication_modification_date(url)

    return text, title, date_published