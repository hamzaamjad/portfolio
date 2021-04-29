#!/usr/bin/env python3
"""Placeholder
"""

# -- Imports --------------------------------------------------------------------------------
import prefect
from prefect import task, Flow
from prefect.engine import signals
from prefect.schedules import IntervalSchedule
from prefect import Parameter

from datetime import timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

@task(name="Create Static Variables")
def create_static_variables(run_style, bls_key):
    
    global sys, json, np, pd, datetime, ppy, ppy_auth, ppy_api, ppy_sql, ppy_geo, ppy_box, ppy_web

    import sys, os, ntpath
    
    import json
    import numpy as np
    import pandas as pd
    from datetime import datetime
    
    sys.path.append(os.path.join(os.getcwd(),'probitaspy'))
    
    import ppy
    import ppy_auth
    import ppy_api
    import ppy_sql
    import ppy_geography as ppy_geo
    import ppy_box
    import ppy_web
    
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    
    global auth, box, geo, testing, bls, fips, us_series, laus_series_names, fips_state_names, year_breakpoints
    
    if 'c' in run_style:
        auth = ppy_auth.Auth()
        box = ppy_box.ProbitasBox()
        geo = ppy_geo.Geographies()

        testing = False

        try:
            bls = ppy_api.BLS(api_key = auth.get_secret('dev/api/bls').get(bls_key))
            logger.info(str(bls_key))

            fips = ppy_geo.Geographies().states
            if testing:
                fips = fips.iloc[0:2,:]
            fips = fips.to_dict(orient = 'records')

            us_series = {
                'LNS11000000': 'Labor Force',
                'LNS12000000': 'Employment',
                'LNS13000000': 'Unemployment',
                'LNS14000000': 'Unemployment Rate'}

            laus_series_names = {
                8 : 'Labor Force Participation Rate',
                7 : 'Employment-Population Ratio',
                6 : 'Labor Force',
                5 : 'Employment',
                4 : 'Unemployment',
                3 : 'Unemployment Rate'}

            year_breakpoints = [
                {'start' : '1990', 'stop' : '2000'},
                {'start' : '2001', 'stop' : '2010'},
                {'start' : '2011', 'stop' : '2020'}]

            fips_state_names = dict(zip(ppy.findkeys(fips, 'fips_state'), ppy.findkeys(fips, 'name_state')))

            logger.info("Static variables configured.")
        except:
            logger.error("Failed to load static variables.")
            logger.error(json.dumps(sys.exc_info()[0]))
            raise signals.FAIL()
    else:
        logger.info("C - Skipped")

@task(name="Extract BLS Data")
def extract_bls(run_style):
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    
    if 'e' in run_style:
        logger.info("Retrieving U.S. Data")

        global pd, np, json, sys, auth, box, geo, ppy, ppy_web, ppy_geo, testing, bls, fips, us_series, laus_series_names, fips_state_names, year_breakpoints, us_employment, state_employment, msa_employment

        try:  
            # -- US -----------------------------------------------
            us_employment = []
            try:
                for year_range in year_breakpoints:
                    data_partial = bls.get_series(list(us_series.keys()), start_year = year_range['start'], end_year = year_range['stop'])
                    data_partial = pd.concat(data_partial).reset_index(drop = True)
                    us_employment.append(data_partial)
                us_employment = pd.concat(us_employment).reset_index(drop = True)
            except:
                logger.info(str(bls.get_series(list(us_series.keys()), start_year = year_range['start'], end_year = year_range['stop'])))

            us_employment['geo_scope'] = 'National'
            us_employment['fips_state'] = '99'
            us_employment['name_state'] = 'United States'

            us_employment['period'] = us_employment['period'].str[1:]
            us_employment = us_employment.rename(columns = {'period' : 'month'})

            us_employment['laus_series_name'] = us_employment['series_id'].map(us_series)

            us_employment = us_employment.loc[:,['series_id', 'fips_state', 'name_state', 'laus_series_name', 'year', 'month', 'value', 'footnotes', 'geo_scope']]
            logger.info(f"U.S. Data - {us_employment.shape}")

            # -- State -----------------------------------------------        
            for i in range(0,len(fips)):
                fips[i]['codes'] = bls.generate_laus(fips[i]['fips_state'])
            if testing:
                print("In Testing Mode.")
                print(fips)
            else:
                next(iter(fips))

            for i in range(0,len(fips)):
                for k,v in fips[i]['codes'].items():
                    try:
                        data_key = str(k) + '_data'
                        data = []
                        for year_range in year_breakpoints:
                            data_partial = bls.get_series(v, start_year = year_range['start'], end_year = year_range['stop'])
                            data_partial = pd.concat(data_partial, ignore_index = True).reset_index(drop = True)
                            data.append(data_partial)
                        data = pd.concat(data, ignore_index = True).reset_index(drop = True)
                        fips[i][data_key] = data
                    except:
                        logger.info(f"{str(fips[i])} - {v} | {year_range['start']} | {year_range['stop']}")

            state_employment = list(ppy.findkeys(fips, 'laus_data'))
            state_employment = pd.concat(state_employment, ignore_index = True)
            state_employment['fips_state'] = state_employment['series_id'].str[5:7]
            state_employment['laus_series_id'] = state_employment['series_id'].str[-1:].astype('int64')

            data_year_range = [int(x) for x in state_employment['year'].unique()]

            logger.info("Start Year of State Data |", min(data_year_range))
            logger.info("End Year of State Data   |", max(data_year_range))

            state_employment['laus_series_name'] = state_employment['laus_series_id'].map(laus_series_names)
            state_employment['name_state'] = state_employment['fips_state'].map(fips_state_names)
            state_employment['geo_scope'] = 'State'

            state_employment['period'] = state_employment['period'].str[1:]
            state_employment = state_employment.rename(columns = {'period' : 'month'})
            state_employment = state_employment.loc[:,['series_id', 'laus_series_id', 'fips_state', 'name_state', 'laus_series_name', 'year', 'month', 'value', 'footnotes', 'geo_scope']]

            logger.info(f"State Data - {state_employment.shape}")

            msa_employment_url = 'https://www.bls.gov/web/metro/ssamatab1.zip'
            msa_employment_file_name = 'ssamatab1.xlsx'

            import zipfile, io
            r = ppy_web.create_session()
            response = r.get(msa_employment_url, stream = True)
            z = zipfile.ZipFile(io.BytesIO(response.content))

            with z as msazip:
                msa_employment = pd.read_excel(io.BytesIO(z.read(msa_employment_file_name)),
                skiprows = 2)

            # Skip blank row between header & data
            msa_employment = msa_employment.iloc[1:,]
            # Remove empty rows & descriptive text at the end of the file
            msa_employment = msa_employment.loc[~((pd.isnull(msa_employment['LAUS Code'])) | (pd.isnull(msa_employment['Area'])))]

            rename_cols = {'LAUS Code' : 'series_id', 'State FIPS Code' : 'fips_state', 'Area FIPS Code' : 'fips_area', 'Area' : 'name_area',
                           'Year' : 'year', 'Month' : 'month', 'Civilian Labor Force' : 'Labor Force'}

            msa_employment = msa_employment.rename(columns = rename_cols)

            msa_employment['series_id'] = 'LAS' + msa_employment['series_id'].astype('str')
            msa_employment['fips_state'] = msa_employment['fips_state'].astype('str').str[:-2]
            msa_employment.loc[msa_employment['fips_state'].str.len() == 1,'fips_state'] = '0' + msa_employment.loc[msa_employment['fips_state'].str.len() == 1,'fips_state']
            msa_employment['fips_area'] = msa_employment['fips_area'].astype('str').str[:-2]
            msa_employment['year'] = msa_employment['year'].astype('str').str[:-2]
            msa_employment['month'] = msa_employment['month'].astype('str').str[:-2]

            msa_employment = pd.melt(
                msa_employment,
                id_vars = ['series_id', 'fips_state', 'fips_area', 'name_area', 'year', 'month'],
                value_vars = ['Labor Force', 'Employment', 'Unemployment', 'Unemployment Rate']
            )

            msa_employment = msa_employment.rename(columns = {'variable' : 'laus_series_name'})

            msa_employment['laus_series_id'] = msa_employment['laus_series_name'].map({v:k for (k,v) in laus_series_names.items()})
            msa_employment['series_id'] = msa_employment['series_id'] + '0' + msa_employment['laus_series_id'].astype('str')
            msa_employment['name_state'] = msa_employment['fips_state'].map(fips_state_names)
            msa_employment['type_area'] = msa_employment['name_area'].str.rsplit(' ',1,expand = True)[1]
            msa_employment['type_area'] = msa_employment['type_area'].str.replace('NECTA', 'Met NECTA')
            msa_employment['name_area'] = msa_employment['name_area'].str.replace(' MSA','')
            msa_employment['name_area'] = msa_employment['name_area'].str.replace(' Met NECTA','')
            msa_employment['geo_scope'] = 'Metropolitan Area'
            msa_employment['flags'] = ''

            msa_employment = msa_employment.loc[:,['series_id', 'laus_series_id', 'fips_state', 'fips_area', 'name_state', 'name_area', 'type_area', 'laus_series_name', 'year', 'month', 'value', 'geo_scope']]

            msa_names = msa_employment['name_area']
            msa_states = msa_names.str.split(',',1,expand=True)[1]
            msa_states_multiple = msa_states[msa_states.str.match('(.*?)\-(.*?)')].index
            msa_names_multiple = msa_employment.iloc[msa_states_multiple,:]['name_area'].drop_duplicates().sort_values()

            state_fips_mapping = geo.states.set_index('abbreviation_state').to_dict(orient = 'index')
            msa_multi_state = []
            for msa in msa_names_multiple:
                msa_employment.loc[msa_employment['name_area'] == msa,'flags'] = 'Multi-State Metro Area'
                copy = msa_employment.loc[msa_employment['name_area'] == msa,:].copy()

                states = msa.split(',',1)[1].strip()
                states = states.split('-')    
                for state in states[1:]:
                    state_fips = state_fips_mapping.get(state).get('fips_state')
                    state_name = state_fips_mapping.get(state).get('name_state')

                    copy_state = copy.copy()

                    copy_state['fips_state'] = state_fips
                    copy_state['name_state'] = state_name
                    msa_multi_state.append(copy_state)

            msa_multi_state = pd.concat(msa_multi_state)
            logger.info(f"MSA Multi-State Data - {msa_multi_state.shape}")

            msa_employment = pd.concat([msa_employment, msa_multi_state], ignore_index = True, sort = True)
            logger.info(f"MSA Data - {msa_employment.shape}")

            box.update_file('674554972731', us_employment)
            box.update_file('642944900692', state_employment)
            box.update_file('642940368966', msa_employment)

            logger.info(f"Data uploaded to Box")
        except:
            logger.error("Failed to extract Data")
            logger.error(json.dumps(sys.exc_info()[0]))
            raise signals.FAIL()
    else:
        logger.info("E - Skipped")
        
@task(name="Transform BLS Data")
def transform_bls(run_style):
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    
    if 't' in run_style:
        logger.info("Transforming Data")

        global sys, json, pd, datetime, box, geo, us_employment, state_employment, msa_employment
        
        if 'e' not in run_style:
            us_employment = pd.read_csv(box.get_file('674554972731'))
            state_employment = pd.read_csv(box.get_file('642944900692'))
            msa_employment = pd.read_csv(box.get_file('642940368966'))

        try:
            us_employment['value'] = us_employment['value'].astype('str')
            state_employment['value'] = state_employment['value'].astype('str')
            msa_employment['value'] = msa_employment['value'].astype('str')
            
            us_employment['value'] = us_employment['value'].str.replace('\.0','',regex=True)
            us_employment['value'] = us_employment['value'].astype('object')

            # Removed footnotes, laus_series_id, series_id column
            column_order = ['fips_state', 'fips_area', 'name_state', 'name_area', 'type_area', 'geo_scope', 'laus_series_name', 'year', 'month', 'date', 'value', 'flags']

            us_employment['flags'] = ''
            state_employment['flags'] = ''

            us_employment['type_area'] = ''
            state_employment['type_area'] = ''

            aggregated_employment = pd.concat([us_employment, state_employment, msa_employment], axis = 0, ignore_index = True, sort = True)

            aggregated_employment = aggregated_employment.loc[aggregated_employment['year'] != 'n',:]

            aggregated_employment['date'] = aggregated_employment['month'].astype('str')
            aggregated_employment.loc[aggregated_employment['date'].str.len() == 1, 'date'] = '0' + aggregated_employment['date']
            aggregated_employment['date'] =  aggregated_employment['date'] + '-' + aggregated_employment['year'].astype('str')
            aggregated_employment['date'] = pd.to_datetime(aggregated_employment['date'], format = '%m-%Y')
            aggregated_employment['date'] = aggregated_employment['date'].dt.date

            aggregated_employment['value'] = aggregated_employment['value'].str.replace('-','') # Remove "-" as value, comes from MSA file
            aggregated_employment['value'].replace({'(n)' : ''}, inplace = True)
            
            aggregated_employment['fips_area'] = aggregated_employment['fips_area'].astype('float64')
            # aggregated_employment['fips_area'] = aggregated_employment['fips_area'].str.replace('\.0','',regex=True)
            aggregated_employment['fips_area'] = aggregated_employment['fips_area'].astype('Int64')

            aggregated_employment = aggregated_employment[column_order]
            master = aggregated_employment

            import getpass
            master['data_loaded_on'] = datetime.now()
            master['data_loaded_by'] = getpass.getuser()
            logger.info(f"Combined Data Shape - {master.shape}")

            box.update_file('643017915800', master)
            box.update_file('657669300653', master, sep='\t', index = False, header = False)
            logger.info(f"Transformed Data uploaded to Box")     
        except:
            logger.error("Failed to tranform Data")
            logger.error(json.dumps(sys.exc_info()[0]))
            raise signals.FAIL()
    else:
        logger.info("T - Skipped")
        
@task(name="Load BLS Data")        
def load_bls(run_style):
    global sys, json, ppy_sql, auth, box
    
    if 'l' in run_style:

        db = ppy_sql.PostgreSQL(**auth.get_secret("dev/rds/postgresql"))
        # Drop existing table
        db.drop_table('dems_bls_laus')
        # -- Prefect Setup -- #
        logger = prefect.context.get("logger")
        # -- Prefect Setup -- #
        logger.info(f"Uploading data to {db.dbname}")

        # SQL command to create LAUS table
        create_table_dems_bls_laus = """
            CREATE TABLE IF NOT EXISTS dems_bls_laus(
                fips_state INT,
                fips_area INT,
                name_state TEXT,
                name_area TEXT,
                type_area TEXT,
                geo_scope TEXT,
                laus_series_name TEXT,
                laus_year INT,
                laus_month INT,
                laus_date DATE,
                laus_value NUMERIC,
                flags TEXT,
                data_loaded_on TIMESTAMP,
                data_loaded_by TEXT
            )
            """

        # Execute SQL Command and commit to DB
        db.execute_commit(create_table_dems_bls_laus)

        f = box.get_file('657669300653')
        db.copy_from(f, 'dems_bls_laus', sep='\t', null = '')

        # Disconnect from DB
        db.disconnect()

        logger.info(f"Data uploaded successfully")
    else:
        logger.info("L - Skipped")

cron = '0 0 1 * *'
schedule = Schedule(clocks=[CronClock(cron)])
with Flow("bls", schedule) as flow:
    bls_key = Parameter("bls_key", default="key")
    run_style = Parameter("run_style", default="cetl")
    
    a, b, c, d = create_static_variables(run_style, bls_key), extract_bls(run_style), transform_bls(run_style), load_bls(run_style)

    flow.add_edge(a, b)
    flow.add_edge(b, c)
    flow.add_edge(c, d)

flow.register(project_name = "probitas-production")
flow.run_agent()