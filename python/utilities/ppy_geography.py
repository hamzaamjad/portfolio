#!/usr/bin/env python3
"""Various Geographic Tools
"""

# -- Imports --------------------------------------------------------------------------------
import logging, re, time
from datetime import datetime

import pandas as pd

import sys
from probitaspy.ppy_web import create_session

# -- Geo ------------------------------------------------------------------------------------

class Geographies():
    def __init__(self, with_vars: bool = True, with_session: bool = False, api_keys: dict = {}):
        if with_vars:
            self.states = [
                ['name_state', 'abbreviation_state', 'fips_state'],
                ['Alabama', 'AL', '01'],['Alaska', 'AK', '02'],['Arizona', 'AZ', '04'],['Arkansas', 'AR', '05'],['California', 'CA', '06'],
                ['Colorado', 'CO', '08'],['Connecticut', 'CT', '09'],['Delaware', 'DE', '10'],['Florida', 'FL', '12'],['Georgia', 'GA', '13'],
                ['Hawaii', 'HI', '15'],['Idaho', 'ID', '16'],['Illinois', 'IL', '17'],['Indiana', 'IN', '18'],['Iowa', 'IA', '19'],['Kansas', 'KS', '20'],
                ['Kentucky', 'KY', '21'],['Louisiana', 'LA', '22'],['Maine', 'ME', '23'],['Maryland', 'MD', '24'],['Massachusetts', 'MA', '25'],
                ['Michigan', 'MI', '26'],['Minnesota', 'MN', '27'],['Mississippi', 'MS', '28'],['Missouri', 'MO', '29'],['Montana', 'MT', '30'],
                ['Nebraska', 'NE', '31'],['Nevada', 'NV', '32'],['New Hampshire', 'NH', '33'],['New Jersey', 'NJ', '34'],['New Mexico', 'NM', '35'],
                ['New York', 'NY', '36'],['North Carolina', 'NC', '37'],['North Dakota', 'ND', '38'],['Ohio', 'OH', '39'],['Oklahoma', 'OK', '40'],
                ['Oregon', 'OR', '41'],['Pennsylvania', 'PA', '42'],['Rhode Island', 'RI', '44'],['South Carolina', 'SC', '45'],
                ['South Dakota', 'SD', '46'],
                ['Tennessee', 'TN', '47'],['Texas', 'TX', '48'],['Utah', 'UT', '49'],['Vermont', 'VT', '50'],['Virginia', 'VA', '51'],
                ['Washington', 'WA', '53'],['West Virginia', 'WV', '54'],['Wisconsin', 'WI', '55'],['Wyoming', 'WY', '56'],
                ['American Samoa', 'AS', '60'],['Guam', 'GU', '66'],['Northern Mariana Islands', 'MP', '69'],
                ['Puerto Rico', 'PR', '72'],['Virgin Islands', 'VI', '78']]
            self.states = pd.DataFrame.from_records(self.states[1:],columns = self.states[0])
        
        if with_session:
            self.r = create_session(timeout = 10)

        self.api_keys = api_keys
        self.today = datetime.now().date().strftime('%Y-%m-%d')
        
        # Generate logger
        self.logger = logging.getLogger(__name__)
        # set level
        self.logger.setLevel(logging.DEBUG)
        # create file handler that logs debug and higher level messages
        self.fh = logging.FileHandler(f'geographies_{self.today}.log')
        self.fh.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        # add the handlers to logger
        self.logger.addHandler(self.fh)

    def standardize_address(self, address: str):
        standardization = {
            r'(?P<before>\S)&(?P<after>\S)' : r'\g<before> & \g<after>', # Non whitespace char, followed by & and another non space char
            r'(?P<before>\s)&(?P<after>\S)' : r' & \g<after>', # Whitespace followed by &, followed by non space char
            r'(?P<before>\S)&(?P<after>\s)' : r'\g<before> & ',  # Non whitespace char followed by &, followed by another whitespace char
            r',$' : '' # Remove comma at the end of a string
        }
        
        if isinstance(address, pd._libs.missing.NAType):
            address = ''
            
        for k,v in standardization.items():
            p = re.compile(k)
            address = p.sub(v, address)
            
        address = address.strip()
        
        return(address)
    
    def standardize_zipcode(self, zipcode):
        zipcode = str(zipcode)
        
        
        standardization = {
            r'=' : r'',
            r'\"' : r''
        }
        
        if isinstance(zipcode, pd._libs.missing.NAType) or zipcode == 'nan' or zipcode == '0':
            zipcode = ''
            
        for k,v in standardization.items():
            p = re.compile(k)
            zipcode = p.sub(v, zipcode)
        
        zip_len = len(zipcode)
        if zip_len == 9:
            zipcode = zipcode[0:5]
        elif zip_len == 8:
            zipcode = '0' + zipcode
            zipcode = zipcode[0:5]
        elif zip_len == 4:
            zipcode = '0' + zipcode
        
        return(zipcode)
    
    # References Google's Geocoding API
    # https://developers.google.com/maps/documentation/geocoding/start
    # Uses f-strings, Python 3.6+
    def geocode_address(self, address: str, key = '', debug = False, parts = False):
        
        replacements = {'&' : 'and', '#' : 'Suite'}
        original = address
        
        # If the value provided for address is a pandas NA type, convert to a blank string and return.
        # May want to return pandas NA type here in the future, for consistency
        if isinstance(address, pd._libs.missing.NAType):
            output = {'key' : key, 'address' : address, 'location' : {}}
            return(output)
        
        address = [x.replace(" ", "+") for x in address.split(",")]
        for k,v in replacements.items():
            address = [x.replace(k, v) for x in address]
        
        address = ','.join(address)
        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={self.api_keys['geocoding']}"
        # url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address[0]},{address[1]},{address[2]}&key={api_key}'
        
        if debug:
            print(f"key: {key} | url: {url} | address: {address}")
        
        time.sleep(0.2)
        response = self.r.get(url)
        response = response.json()
        
        output = {
            'key' : key,
            'address' : address,
            'location' : {}
        }
        
        try:
            if response['status'] == 'REQUEST_DENIED' or response['status'] == 'ZERO_RESULTS':
                self.logger.error(f"geocode_address: {response['status']} {response['error_message']}")
                return(output)
            elif response['status'] == 'OK':
                result = response['results'][0]
                if parts:
                    output = {
                        'key' : key,
                        'address' : result['formatted_address'],
                        'location' : result['geometry']['location'],
                        'address_parts' : {
                            'street' : address_parts[0],
                            'city' : address_parts[1],
                            'state' : address_parts[2][:2],
                            'zip_code' : address_parts[2][-5:],
                            'country' : address_parts[3]
                        }
                    }
                else:
                    output = {
                        'key' : key,
                        'address' : result['formatted_address'],
                        'location' : result['geometry']['location']
                    }
            return(output)
        except:
            self.logger.error(f'geocode_address: {url}')
            return(output)

    # Expects an 'address' dictionary output by geocode_address
    def address_parts(self, key, address, location):
        try:
            address_parts = [x.strip() for x in address.split(",")]
            output = {'key' : key, 'address' : address, 'location' : location,
                      'address_parts' : {'street' : address_parts[0], 'city' : address_parts[1],
                                         'state' : address_parts[2][:2], 'zip_code' : address_parts[2][-5:], 'country' : address_parts[3]}}
        except:
            output = {'key' : key, 'address' : address, 'location' : location,
                      'address_parts' : {'street' : '', 'city' : '',
                                         'state' : '', 'zip_code' : '', 'country' : ''}}            
        return(output)

    # Expects an 'address' dictionary output by geocode_address, with further modification by address_parts
    def address_census_keys(self, key, address, location, address_parts, benchmark = 'Public_AR_Current', vintage = 'ACS2019_Current', i = 0):
        url = 'https://geocoding.geo.census.gov/geocoder/geographies/coordinates'
        #layers = ['2010 Census Blocks', 'Secondary School Districts', '2019 State Legislative Districts - Upper', 'County Subdivisions',
        #'Elementary School Districts', 'Metropolitan Statistical Areas', 'Counties', '2019 State Legislative Districts - Lower', 'Census Block Groups',
        #'Combined Statistical Areas', '2010 Census ZIP Code Tabulation Areas', 'Census Tracts']
        layers = ['Counties', 'Metropolitan Statistical Areas', 'Metropolitan Divisions','Combined Statistical Areas',
                  'Census Tracts', 'Census Block Groups',  '2010 Census ZIP Code Tabulation Areas', '2010 Census Blocks']
        try:
            url_params = {
                'x' : location['lng'],
                'y' : location['lat'],
                'benchmark' : benchmark,
                'vintage' : vintage,
                'layers' : layers,
                'format' : 'json'
            }
            response = self.r.get(url, params = url_params)
            result = response.json()['result']

            geographies = {k1: {k2: v2 for
                                k2, v2 in next(iter(v1 or []), dict()).items() if (k2 in ['GEOID', 'CENTLAT', 'BASENAME', 'NAME', 'CENTLON'])} for
                           k1, v1 in result['geographies'].items()}
            output = {
                'key' : key,
                'address' : address,
                'location' : location,
                'address_parts' : address_parts,
                'geographies' : geographies
            }

            if output.get('geographies','').get('Metropolitan Statistical Areas','').get('BASENAME','') != '' or i > 5:
                return(output)
            else:
                i += 1
                return self.address_census_keys(key, address, location, address_parts, i)
        except:
            output = {
                'key' : key,
                'address' : address,
                'location' : location,
                'address_parts' : address_parts,
                'geographies' : {}
            }
            return(output)
        
    def address_geographies(self, address: str, key = '', debug = False, parts = False):
        geographies = self.geocode_address(address, key)
        try:
            geographies = self.address_parts(**geographies)
            geographies = self.address_census_keys(**geographies)
            geographies = {
                'key': key,
                'geo_full_address' : geographies.get('address',''),
                'geo_latitude' : geographies.get('location',{}).get('lat',''),
                'geo_longitude' : geographies.get('location',{}).get('lng',''),
                'geo_county' : geographies.get('geographies',{}).get('Counties',{}).get('BASENAME',''),
                'geo_metropolitan_statistical_area' : geographies.get('geographies',{}).get('Metropolitan Statistical Areas',{}).get('BASENAME',''),
                'geo_combined_statistical_area' : geographies.get('geographies',{}).get('Combined Statistical Areas',{}).get('BASENAME',''),
                'geo_census_tracts' : geographies.get('geographies',{}).get('Census Tracts',{}).get('BASENAME',''),
                'geo_census_block_groups' : geographies.get('geographies',{}).get('Census Block Groups',{}).get('BASENAME','')
                }
            return(geographies)
        except:
            print(geographies)