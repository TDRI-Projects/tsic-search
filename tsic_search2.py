import re
import urllib
import requests
import pandas as pd
import numpy as np
from thefuzz import fuzz
from pythainlp.soundex import lk82
from lingua import Language, LanguageDetectorBuilder

languages = [Language.ENGLISH, Language.THAI]
detector = LanguageDetectorBuilder.from_languages(*languages).build()

df = pd.read_csv("./tsic_search/dbd_data_2024_cleaned_and_soundex.csv") # DBD Database


keywords = ["สาขา", "สำหรับ", 'จังหวัด', 'บริษัท', 'จำกัด', 'หจก',
               'มหาชน', '-', '.', '/', ',', '\\', 'company', 'corporation',
               'group', 'co', 'ltd', 'limited', 'pcl']


tsic_mapping = dict()
df2 = pd.read_excel("./tsic_search/TSIC_mapping.xlsx") #TSIC Mapping
for index, row in df2.iterrows():
    tsic_mapping[row['TSIC_CODE']] = row['TSIC_GROUP']
search_url = "https://www.dataforthai.com/api/company"

# Common headers and cookies
AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15'
headers = {
    'User-Agent': AGENT,
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,th;q=0.8',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}
cookies = {
    'PHPSESSID': '0mubrseten3kulptp22ae0d72l'
}

def clean_search_term(search_term):
    global th_keywords
    search_term = search_term.lower()
    # handle (สำนักงานใหญ่ สาขา) etc
    search_term = re.sub(r'\([^)]*\)', '', search_term)
    search_term = search_term.strip('"() ')
    for forbidden_word in keywords:
        search_term = search_term.replace(forbidden_word, "")
    return search_term.strip()

def check_lang_and_clean(search_term):
    """
    Detects the language of a string and returns a tuple with 
    the language and the string with only the dominant language left
    >>>print(check_lang_and_clean('ไก่ไข่ banana milk bread monkey turtle'))
    'banana milk bread monkey turtle'
    """
    search_term = clean_search_term(search_term)
    lang = detector.detect_language_of(search_term).name
    if lang == 'THAI':
        for result in detector.detect_multiple_languages_of(search_term):
            if result.language.name == 'ENGLISH':
                search_term.replace(search_term[result.start_index:result.end_index], '')
        return (search_term.strip(), lang)
    else:
        for result in detector.detect_multiple_languages_of(search_term):
            if result.language.name == 'THAI':
                search_term.replace(search_term[result.start_index:result.end_index], '')
        return (search_term.strip(), lang)
    
    # for chunk in results:
        
    # result.language.name == 'THAI'

def scrape_dataforthai(search_term):
    search_term = clean_search_term(search_term)
    global tsic_mapping
    # search_term = clean_search_term(search_term)
    with requests.Session() as session:
        encoded_company_name = urllib.parse.quote(search_term)
        payload = f"mode=search_comp&data%5Bsearchtext%5D={
            encoded_company_name}"
        # try:
        response = session.post(
            search_url, headers=headers, data=payload, cookies=cookies)
        # except:
        #     print('dataforthai broke')
        #     return None
        try:
            json_response = response.json()
        except:
            print(response)
            return None
        # Check if the company is found
        if json_response['status'] == '1' and len(json_response['data']) > 0:
            company_id = json_response['data'][0]['jp_no']
            return company_id

        else:
            return None


def scrape_dbd(company_id):
    detail_url_template = "https://openapi.dbd.go.th/api/v1/juristic_person/{}"
    detail_url = detail_url_template.format(company_id)
    with requests.Session() as session:
        detail_headers = {
            'Cookie': 'incap_ses_1841_3107248=HSLyLViaTnN3ear004uMGWx0emYAAAAAC00AipHWPZIUka1cnqBUZg==; visid_incap_3107248=BXyfzpjhQRSG7gs32+X1HWt0emYAAAAAQUIPAAAAAAABc6yyhKhmnZi29Exe+7Sx'
        }
        try:
            detail_response = session.get(
                detail_url, headers=detail_headers, allow_redirects=False)
        except:
            print('timed out')
            return None
        print(detail_response)
        try:
            detail_json = detail_response.json()
            # Extracting desired information from the detail response
            print(detail_json)
            juristic_code = detail_json['data'][0]["cd:OrganizationJuristicPerson"][
                'cd:OrganizationJuristicObjective']['td:JuristicObjective']['td:JuristicObjectiveCode']
            if juristic_code:
                juristic_code = int(juristic_code[:2])
            else:
                return None
            return tsic_mapping[juristic_code]
        except:
            print('weve been IP banned')
            return None




def exact_match(search_term):
    result = df[df['company_name'] == search_term]

    if result.empty:
        return None
    return result['tsic_code'].iloc[0]


def exact_match_id(id):
    id = int(id)
    result = df[df['company_id'] == id]
    if result.empty:
        return None
    return result['tsic_group'].iloc[0]


def cleaned_match(search_term: str):
    global df
    term, lang = check_lang_and_clean(search_term)
    if lang == 'THAI':  # is in thai
        result = df[df['cleaned_th_name'].str.contains(
            term, regex=False)]
        result['score'] = result['cleaned_th_name'].apply(
            lambda x: fuzz.ratio(x, term))
    else: # is in english
        result = df[df['cleaned_en_name'].str.contains(
            term, regex=False)]
        result['score'] = result['cleaned_en_name'].apply(
            lambda x: fuzz.ratio(x, term))
        
    result = result.sort_values(by='score', ascending=False)
    if result.empty:
        return None
    # print(result)
    return result['tsic_group'].iloc[0]


def soundex_match(search_term): # unused
    # clean the search term
    search_term = get_soundex(search_term)
    result = df[df['soundex'] == search_term]
    if result.empty:
        return None
    else:
        return result.iloc[0]


def fuzzy_match(search_term):
    result = df
    term, lang = check_lang_and_clean(search_term)
    if lang == 'THAI':  # is thai
        result['score'] = df['cleaned_th_name'].apply(
            lambda x: fuzz.ratio(x, term))
    else:  # english
        result['score'] = df['cleaned_en_name'].apply(
            lambda x: fuzz.ratio(x, term))
    result = result[result['score'] >= 70]
    result = result.sort_values(by='score', ascending=False)
    if not result.empty:
        return result['tsic_group'].iloc[0]
    return None


def get_soundex(company_name): #unused
    thai_keywords2 = ["สาขา", "สำหรับ", 'จังหวัด',
                      'บริษัท', 'จำกัด', 'มหาชน', '-', '.']
    for forbidden_word in thai_keywords2:
        company_name = company_name.replace(forbidden_word, "")
    soundex = [lk82(word) for word in company_name.split()]
    soundex = ''.join(word for word in soundex)
    return soundex


def search(search_term):
    search_term = clean_search_term(search_term)
    result = cleaned_match(search_term)
    if not result:
        result = fuzzy_match(search_term)
        if not result:
            return None
        else:
            return result
    else:
        return result


# df3 = pd.read_csv("./tsic_search/q2_combined(working).csv")
# print(df3.head())
# i = 0
# for index, row in df3.iterrows():
#     if i > 100:
#         break
#     if isinstance(row['tsic_code'], float):
#         company_name = row['company']
#         company_id = scrape_dataforthai(company_name)

#         if company_id:
#             result = exact_match_id(company_id)
#             if result:
#                 print('searching for:' + company_name)
#                 print('found ' + result)
#                 df3._set_value(index, 'tsic_code', result)
#                 df3._set_value(index, 'method', 'dataforthai')
#                 df3._set_value(index, 'registration_number', company_id)
#                 print(i)
#                 i += 1
#             else:
#                 print('not found in database')
#         else:
#             print('cant find ' + company_name)
#     else:
#         continue
# df3.to_csv('q2_lastpass.csv')
