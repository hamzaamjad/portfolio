#!/usr/bin/env python3
"""Objects that provide an interface to various Public APIs.
"""

# -- Imports --------------------------------------------------------------------------------
import sys, os, logging
from datetime import datetime
import json

import pandas as pd

from probitaspy.ppy_web import create_session

# -- API Objects ----------------------------------------------------------------------------

# -- FRED ---------------------------------------------------------------------------------
class FRED():
    def __init__(self, api_key: str = '', debug = False):
        
        self.api_key = api_key
        
        self.file_type = 'json'
        self.url_base = 'https://api.stlouisfed.org/fred/'
        self.base_params = {'api_key' : self.api_key, 'file_type' : self.file_type}
        self.url_options = {
            'series' : ['categories', 'observation', 'release', 'search']
            }
        
        self.configure_session()
        
        self.debug = debug
        if self.debug:
            print("debugger | debugging enabled")

    def configure_session(self):
        self.r = create_session(timeout = 15)
    
    def query_url(self, endpoint_url: str, params: dict):
        payload = {**params, **self.base_params}
        url = self.url_base + endpoint_url
        try:
            response = self.r.get(url, params = payload)
            if self.debug:
                print(f"debugger | {endpoint_url} | {response.url}")
            response = response.json()
            return(response)
        except:
            ("Error.", {'url':url,'params':params})

    def series_search(self, search_text: str):
        url = 'series/search'
        response = self.query_url(url, {'search_text':search_text})
        return(response)
    
    def series_observations(self, search_text: str, scope: str = 'national'):
        url = 'series/observations'
        response = self.query_url(url, {'series_id':search_text})
        try:
            response = response['observations']
            response = pd.DataFrame(response)
            if scope == 'national':
                response['variable'] = search_text
                response['geo_scope'] = 'National'
                response['geo_abbreviation'] = 'US'
            elif scope == 'state':
                response['variable'] = search_text[2:]
                response['geo_scope'] = 'State'
                response['geo_abbreviation'] = search_text[0:2]
            else:
                response['variable'] = search_text
            return(response)
        except:
            print(f'Error in series_obsevations func | {search_text}')
            print(sys.exc_info())
            print(response)
            
    def generate_fred_codes(self, abbreviation_state):
        '''
        Provided a state abbreviation_stateabbreviation_state, generates the state's initial claims FRED Series ID 
        Example usage -- generate_iclaims('AZ')

        Response --
        {
            'claims': ['AZICLAIMS', 'AZCCLAIMS'],
            'cemploy': ['AZCEMPLOY']
            }
        '''
        response = {
            'claims' : [
                f'{abbreviation_state}ICLAIMS',
                f'{abbreviation_state}CCLAIMS'
            ],
            'cemploy' : [
                f'{abbreviation_state}CEMPLOY'
            ],
            'iur' : [
                f'{abbreviation_state}INSUREDUR'
            ]
        }

        return(response)
    
# -- BLS ----------------------------------------------------------------------------------
class BLS():
    def __init__(self, api_key: str = ''):
        self.r = create_session()
        self.headers = {'Content-type': 'application/json'}
        self.api_key = api_key

    def generate_laus(self, fips_state):
        '''
        Provided a state FIPS code, generates the state's LAUS Series IDs 
        >>> generate_laus('06')
        {'laus': ['LASST060000000000003',
            'LASST060000000000004',
            'LASST060000000000005',
            'LASST060000000000006',
            'LASST060000000000007',
            'LASST060000000000008']}
        '''
        fips_state = str(fips_state)
        if len(fips_state) < 2:
            fips_state = '0' + fips_state
        else:
            fips_state = fips_state # just in case...

        response = {
            'laus' : [
                f'LASST{fips_state}0000000000003', # Unemployment Rate
                f'LASST{fips_state}0000000000004', # Unemployment
                f'LASST{fips_state}0000000000005', # Employment
                f'LASST{fips_state}0000000000006', # Labor Force
                f'LASST{fips_state}0000000000007', # Employment-Populatio Ratio
                f'LASST{fips_state}0000000000008', # Labor Force Participation Rate
            ]
        }
        
        return(response)

    def generate_ces(self, fips_state):
        '''
        Provided a state FIPS code, generates the state's CES Series IDs 
        >>> generate_ces('06')
        {'ces': ['LASST060000000000003',
            'LASST060000000000004'
            ]}
        '''
        fips_state = str(fips_state)
        if len(fips_state) < 2:
            fips_state = '0' + fips_state

        response = {
            'ces' : [
                f'SMS{fips_state}000000000000001', # Total Nonfarm, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000000500000001', # Total Private, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000000600000001', # Goods Producing, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000000700000001', # Service-Providing, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000000800000001', # Private Service Providing, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000001000000001', # Mining/Logging, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000001500000001', # Mining/Logging/Construction, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000002000000001', # Construction, All Employees, In Thousands, Seasonally Adjusted
                f'SMS{fips_state}000001500000001', # Mining/Logging/Construction, All Employees, In Thousands, Seasonally Adjusted
            ]
        }
        
        return(response)

    def get_series(self, series_list, start_year = str(int(datetime.strftime(datetime.now(),"%Y"))-10), end_year = datetime.strftime(datetime.now(),"%Y")):
        '''
        Code originally from: https://www.bls.gov/developers/api_python.htm#python2
        Modified by: Hamza Amjad

        series_list -- list of series IDs
        start_year -- start year for data pull, defaults to ten years in the past
        end_year -- end year for data pull, defaults to today's year

        >>> get_series(generate_laus('04')['laus'])
        '''
        
        data = json.dumps({
            "seriesid": series_list,
            "startyear": start_year, "endyear": end_year,
            "registrationkey" :  '302aea9fa5ad4faa9ec5114111b6add1' # Your key here...
            })
        p = self.r.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=self.headers)
        try:
            json_data = json.loads(p.text)
            if json_data['status'] == 'REQUEST_NOT_PROCESSED':
                print(json_data['status'])
                print(json_data['message'])
                return(json_data)
            else:
                series_data = {k['seriesID']:'' for k in json_data['Results']['series']}
                for series in json_data['Results']['series']:
                    data = list()
                    series_id = series['seriesID']
                    for item in series['data']:
                        year = item['year']
                        period = item['period']
                        value = item['value']
                        footnotes = ""
                        for footnote in item['footnotes']:
                            if footnote:
                                footnotes = footnotes + footnote['text'] + ','
                        if 'M01' <= period <= 'M12':
                            data.append({'series_id' : series_id, 'year' : year, 'period' : period, 'value' : value, 'footnotes' : footnotes[0:-1]})
                    data = pd.DataFrame.from_dict(data)
                    series_data[series_id] = data
        except:
            series_data = p.text
        return(series_data)