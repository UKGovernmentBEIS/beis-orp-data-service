import string
import wordninja
from typing import List

# regulator_name_list = [
#     "Charity Commission for England and Wales",
#     "Charity Commission for Northern Ireland",
#     "Office of the Scottish Charity Regulator",
#     "The General Teaching Councils for Scotland, Wales and Northern Ireland",
#     "Ofqual – Office of Qualifications and Examinations Regulation",
#     "Ofsted – Office for Standards in Education, Children's Services and Skills",
#     "Office for Students (OfS)",
#     "Environment Agency (EA)",
#     "Marine Management Organisation (MMO)",
#     "Natural Resources Wales (NRW)",
#     "Northern Ireland Environment Agency (NIEA)",
#     "Scottish Environment Protection Agency (SEPA)",
#     "Financial Conduct Authority (FCA)",
#     "The Office for Professional Body Anti-Money Laundering Supervision (OPBAS)",
#     "Financial Reporting Council, expected to be replaced by the Audit, Reporting and Governance Authority in 2023",
#     "Institute of Chartered Accountants in England and Wales",
#     "Office of the Regulator of Community Interest Companies (ORCIC)",
#     "Payment Systems Regulator (PSR)",
#     "Pensions Regulator",
#     "Prudential Regulation Authority (PRA)",
#     "Care Quality Commission (CQC)",
#     "Complementary and Natural Healthcare Council (CNHC)",
#     "General Chiropractic Council (GCC)",
#     "General Dental Council (GDC)",
#     "General Medical Council (GMC)",
#     "General Optical Council (GOC)",
#     "General Osteopathic Council (GOsC)",
#     "General Pharmaceutical Council (GPhC)",
#     "Health and Care Professions Council (HCPC)",
#     "Health and Safety Executive",
#     "Healthcare Inspectorate Wales (HIW)",
#     "Healthcare Safety Investigation Branch (HSIB)",
#     "Human Fertilisation and Embryology Authority",
#     "Human Tissue Authority (HTA)",
#     "Medicines and Healthcare products Regulatory Agency (MHRA)",
#     "NHS Improvement (NHSI)",
#     "Nursing and Midwifery Council (NMC)",
#     "Pharmaceutical Society of Northern Ireland (PSNI)",
#     "Professional Standards Authority for Health and Social Care",
#     "Royal College of Veterinary Surgeons (RCVS)",
#     "UK Health Security Agency (UKHSA)",
#     "Regulator of Social Housing",
#     "Scottish Housing Regulator",
#     "Authorised Conveyancing Practitioners Board",
#     "Bar Standards Board",
#     "CILEx Regulation",
#     "Faculty of Advocates",
#     "Law Society of Northern Ireland",
#     "Law Society of Scotland",
#     "Master of the Faculties",
#     "Office of the Immigration Services Commissioner",
#     "Solicitors Regulation Authority",
#     "Costs Lawyer Standards Board[3]",
#     "Council for Licensed Conveyancers",
#     "Scottish Care Inspectorate",
#     "Care Council for Wales (CCW)",
#     "Social Work England[4]",
#     "Northern Ireland Social Care Council (NISCC)",
#     "Scottish Social Services Council (SSSC)",
#     "Civil Aviation Authority (CAA)",
#     "Office of Rail and Road (ORR)",
#     "Ofcom – independent regulator and competition authority for the UK communications industries",
#     "Phone-paid Services Authority – regulator for phone-paid services in the UK, part of Ofcom, replaces ICSTIS, PhonepayPlus",
#     "Office for Nuclear Regulation (ONR)",
#     "Ofgem – the Office of the Gas and Electricity Markets",
#     "Ofwat – the Water Services Regulation Authority",
#     "The Utility Regulator – regulating electricity, gas, water and sewerage industries in Northern Ireland",
#     "Water Industry Commission for Scotland",
#     "Accreditation Service",
#     "Advertising Standards Authority",
#     "British Board of Film Classification",
#     "Chartered Institute for the Management of Sport and Physical Activity",
#     "Competition and Markets Authority",
#     "Council for Registered Gas Installers",
#     "Direct Marketing Authority",
#     "Engineering Council – the regulatory body for the engineering profession",
#     "Equality and Human Rights Commission",
#     "Food Standards Agency",
#     "Forensic Science Regulator",
#     "Fundraising Regulator",
#     "Gambling Commission",
#     "Gangmasters and Labour Abuse Authority",
#     "HM Revenue and Customs",
#     "IMPRESS",
#     "Independent Press Standards Organisation",
#     "Information Commissioner's Office",
#     "North Sea Transition Authority",
#     "Planning Inspectorate",
#     "Independent Office for Police Conduct",
#     "Security Industry Authority"
# ]

with open("regulator_name_list.txt", "r") as f:
    fileObject = f.read()
    regulator_name_list = fileObject.split("\n")


def removing_regulator_names(text : str, regulator_name_list = regulator_name_list) -> str:
    """
    param: text: Str document text
    param: regulator_name_list: List list of regulator names to remove from text
    returns: text: Str cleaned document text
        Removal of regulator names from text to clean the text before the title is predicted
    """
    for reg in regulator_name_list:
        text = text.replace(reg, "")

    return text


def delete_single_characters(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Removal of single characters at the start of text
    """
    # If first 5 tokens are == len(1) it is a sign the text is malformed
    short_char_counter = 0
    for char in text.strip().split(" ")[:5]:
        if len(char) == 1:
            short_char_counter +=1
    if short_char_counter == 5:        
        text = wordninja.split("".join(text).replace(" ", ""))
        return " ".join(text)
    else:
        return text


def remove_excess_punctuation(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Returns text without excess punctuation
    """
    # Clean punctuation spacing
    text = text.replace(" .", "")
    for punc in string.punctuation:
        text = text.replace(punc + punc, "")
    return text


def preprocess(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Function that fully preprocesses text
    """
    text = removing_regulator_names(text, regulator_name_list)
    text = delete_single_characters(text)
    text = remove_excess_punctuation(text)
    return text