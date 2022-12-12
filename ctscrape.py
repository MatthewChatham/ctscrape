import requests
from datetime import datetime
import os
import re
import urllib
import pandas as pd
import argparse

DATE_FORMAT = '%m/%d/%Y'
API_URL = 'https://www.clinicaltrials.gov/api/query/full_studies'

drugs = '(MDMA OR psilocybin OR ketamine OR ayahuasca OR kratom OR DMT OR psilocin OR ibogaine OR iboga OR 2C-B OR mescaline OR peyote OR salvia divinorum)'
QUERY_EXPR = drugs #+ 'AND AREA[StudyFirstPostDate]RANGE[{}, {}]TILT[StudyFirstPostDate]'


def get_epoch():
    """
    Time since epoch in milliseconds.
    """
    since_epoch_ms = str(datetime.now().timestamp() * 1000)
    return since_epoch_ms.split('.')[0]

def norm_col(c):
    """
    Helper for converting query results to DF. Uses JSON leaf as column name.
    """
    return c.split('.')[-1]


def query_api(qparams: dict):
    """
    Uses the `requests` module to query the clinicaltrials.gov API.
    """

    url = API_URL + '?' + urllib.parse.urlencode(qparams)

    try:
        r = requests.get(url)
    except Exception:
        raise Exception(f'Error running API query with parameters {qparams}')

    resp = r.json()['FullStudiesResponse']
    
    return resp


def query_helper(qparams, res):
    """
    Helper for hitting the API with some added logic for looping.
    """
    print(qparams)
    resp = query_api(qparams)
    res.extend(resp['FullStudies'])
    qparams['min_rnk'] += 100
    qparams['max_rnk'] += 100
    return resp


def quoter(s, *args):
    return s


def ctscrape(begin=None, end=None, pth='./data', filename='ctscrape'):
    """
    Given a `begin_date` and `end_date`, query the ClinicalTrials.gov API
    and save trials published during that timeframe (inclusive) to 
    `filename` as a CSV located at `pth`.
    
    `begin`: str, date in format mm/dd/yyyy
    `end`: str, date in format mm/dd/yyyy
    `pth`: str, the directory where you'd like to store the file
    `filename`: str, filename to use
    
    Requirements:
    * Do not overwrite data -- append timestamp to `filename`.
    """
    #
    # Argument checking
    #
    if begin is None or end is None:
        raise Error('Enter `begin` and `end`.')

    try:
        datetime.strptime(begin, DATE_FORMAT)
        datetime.strptime(end, DATE_FORMAT)
    except ValueError:
        raise ValueError('Enter date in mm/dd/yyyy format.')

    if not os.path.isdir(pth):
        raise ValueError('`pth` is not a directory')

    pat = re.compile(r'[A-Za-z0-9_]+')
    if not re.fullmatch(pat, filename):
        msg = '`filename` must be alphanumeric or underscore. \
        Will be appended with timestamp and .csv'
        raise ValueError(msg)
    
    res = []
    
    #
    # Construct query parameters
    #
    qparams = dict(
        expr = QUERY_EXPR.format(begin, end),
        min_rnk = 1,
        max_rnk = 100,
        fmt = 'json'
    )
    
    #
    # Loop through results
    #
    print('getting first respondse')
    resp = query_helper(qparams, res)
    n_studies = int(resp['NStudiesFound'])
    print(n_studies)
    while qparams['min_rnk'] < n_studies:
        print(qparams['min_rnk'])
        query_helper(qparams, res)
            
    # Convert to DF
    studies_df = pd.json_normalize(res)
    studies_df.columns = [norm_col(c) for c in studies_df.columns]
    
    # Write to file
    epoch = get_epoch()
    filename = filename + f'_{epoch}' + '.csv'
    out = os.path.join(pth, filename)
    studies_df.to_csv(out, index=False)
    
    return studies_df

def main():
    
    desc = 'Query the ClinicalTrials.gov API.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('begin')
    parser.add_argument('end')
    args = parser.parse_args()
    
    ctscrape(args.begin, args.end)

if __name__ == '__main__':
    main()