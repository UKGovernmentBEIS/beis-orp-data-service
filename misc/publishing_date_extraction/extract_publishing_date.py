import PyPDF2
from dateparser.search import search_dates
from datetime import datetime

import logging
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

# Extract pdf
filename = "PDF TO BE PASSED TO SCRIPT"
uploaded_pdf = open(filename, mode="rb")
pdfdoc = PyPDF2.PdfFileReader(uploaded_pdf, strict=False)

# Function to get creation date from pdfs


def get_document_date_info(pdfdoc):
    """
    Extract date from metadata and format date
    To get the month and year
    """
    # Define date format
    format_yyyymm = "%Y/%m"
    # Get creation date and modification date
    creation_date = pdfdoc.documentInfo["/CreationDate"]
    # Check if in date format already
    if search_dates(creation_date) is None:
        # Add dates to a list
        digits = []
        for char in creation_date:
            if char.isdigit():
                digits.append(char)
        month_and_year = "".join(digits[0: 4]) + "/" + "".join(digits[4: 6])
        # Format date
        date = datetime.strptime(month_and_year, format_yyyymm)
        # Return the mod date and creation date
        return date
    else:
        return creation_date
