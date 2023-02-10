from bs4 import BeautifulSoup
import requests
from htmldate import find_date
import re


URL = "https://www.hse.gov.uk/simple-health-safety/gettinghelp/index.htm"

HSE_URL = "https://www.hse.gov.uk/simple-health-safety/gettinghelp/index.htm"

EA_URL = "https://www.gov.uk/check-flooding"

req = requests.get(HSE_URL)


def get_title_text(req):
    """
    params: req: request URL
    returns: title, text: Str
    """
    soup = BeautifulSoup(req.text, "html.parser")

    title = str(soup.head.title.get_text())
    text = re.sub("\\s+", " ", str(soup.get_text()).replace("\n", " "))

    return title, text


def get_publication_modification_date(URL):
    """
    params: URL: Str
    returns: publication_date, modification_date: Str
    """
    # Initally disable extensive search
    publication_date = str(find_date(URL, original_date=True, extensive_search=False))
    modification_date = str(find_date(URL, extensive_search=False))

    # If no concrete date is found, do extensive search
    if publication_date == "None":
        publication_date = "Inferred " + str(find_date(URL, original_date=True))

    if modification_date == "None":
        modification_date = "Inferred " + str(find_date(URL))

    return publication_date, modification_date


# Get title and text
title, text = get_title_text(req)

# Get publication dates
publication_date, modification_date = get_publication_modification_date(URL)


print(f"Title: {title}")
print(f"Publication Date: {publication_date}")
print(f"Modification Date: {modification_date}")
print(f"Text: {text}")