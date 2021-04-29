import prefect
from prefect import task, Flow
from prefect.engine import signals
from prefect.schedules import IntervalSchedule

from datetime import timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

@task
def create_static_variables():
    
    global np, pd, datetime, os, sys, time, json, ppy, ppy_auth, ppy_api, ppy_sql, ppy_geo, ppy_box
    
    import numpy as np
    import pandas as pd
    from datetime import datetime
    import os, sys, time
    import json
    
    sys.path.append(os.path.join(os.getcwd(),'probitaspy'))
    
    import ppy
    import ppy_auth
    import ppy_api
    import ppy_sql
    import ppy_geography as ppy_geo
    import ppy_box
    
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    
    global auth, box, geo, testing, fred, fred_codes_us, fips, name_mapping, adjustment_mapping
    
    auth = ppy_auth.Auth()
    box = ppy_box.ProbitasBox()
    geo = ppy_geo.Geographies()
    
    testing = False
    
    try:
        fred = ppy_api.FRED(api_key = auth.get_secret('dev/api/fred').get('key'), debug = testing)
        
        fred_codes_us = [
            'ICSA', # Initial Claims, Seasonally Adjusted
            'IC4WSA', # 4-Week Moving Average of Initial Claims, Seasonally Adjusted
            'CCSA', # Continued Claims (Insured Unemployment), Seasonally Adjusted
            'IURSA', # Insured Unemployment Rate, Seasonally Adjusted
            'ICNSA', # Initial Claims, Not Seasonally Adjusted
            'CCNSA', # Continued Claims (Insured Unemployment), Not Seasonally Adjusted
            'IURNSA', # Insured Unemployment Rate, Not Seasonally Adjusted
            'COVEMP',  # Covered Employment, Not Seasonally Adjusted
            'GDP', # Gross Domestic Product, Seasonally Adjusted
            'CPIAUCSL', # Consumer Price Index for All Urban Consumers: All Items in U.S. City Average, Seasonally Adjusted
            'INDPRO', # Industrial Production Index, Seasonally Adjusted
            'UMCSENT', # University of Michigan: Consumer Sentiment, Not Seasonally Adjusted
            'M1', # M1 Money Stock, Seasonally Adjusted
            'M1V', # Velocity of M1 Money Stock, Seasonally Adjusted
            'M2', # M2 Money Stock, Seasonally Adjusted
            'M2V', # Velocity of M2 Money Stock, Seasonally Adjusted
            'WALCL', # Total Assets (Less Eliminations from Consolidation): Wednesday Level, Not Seasonally Adjusted
            'FEDFUNDS' # Effective Federal Funds Rate, Not Seasonally Adjusted
        ]
        
        fips = ppy_geo.Geographies().states
        if testing:
            fips = fips.iloc[0:2,:]
        fips = fips.to_dict(orient = 'records')
        
        for i in range(0,len(fips)):
            fips[i]['codes'] = fred.generate_fred_codes(fips[i]['abbreviation_state'])
            
        name_mapping = {
            'ICSA' : 'Initial Claims',
            'IC4WSA' : '4-Week Moving Average of Initial Claims',
            'CCSA' : 'Continued Claims',
            'IURSA' : 'Insured Unemployment Rate',
            'ICNSA' : 'Initial Claims',
            'CCNSA' :  'Continued Claims',
            'IURNSA' :  'Insured Unemployment Rate',
            'COVEMP' : 'Covered Employment',
            'ICLAIMS' : 'Initial Claims',
            'CCLAIMS' : 'Continued Claims',
            'CEMPLOY' : 'Covered Employment',
            'INSUREDUR' : 'Insured Unemployment Rate',
            'GDP' : 'Gross Domestic Product',
            'CPIAUCSL' : 'Consumer Price Index',
            'INDPRO' : 'Industrial Production Index',
            'UMCSENT' : 'University of Michigan: Consumer Sentiment',
            'M1' : 'M1 Money Stock',
            'M1V' : 'Velocity of M1 Money Stock',
            'M2' : 'M2 Money Stock',
            'M2V' : 'Velocity of M2 Money Stock',
            'WALCL' : 'Total Assets (Less Eliminations from Consolidation): Wednesday Level',
            'FEDFUNDS' : 'Effective Federal Funds Rate'
        }

        adjustment_mapping = {
            'ICSA' : 'Seasonally Adjusted',
            'IC4WSA' : 'Seasonally Adjusted',
            'CCSA' : 'Seasonally Adjusted',
            'IURSA' : 'Seasonally Adjusted',
            'ICNSA' : 'Not Seasonally Adjusted',
            'CCNSA' :  'Not Seasonally Adjusted',
            'IURNSA' :  'Not Seasonally Adjusted',
            'COVEMP' : 'Not Seasonally Adjusted',
            'ICLAIMS' : 'Not Seasonally Adjusted',
            'CCLAIMS' : 'Not Seasonally Adjusted',
            'CEMPLOY' : 'Not Seasonally Adjusted',
            'INSUREDUR' : 'Not Seasonally Adjusted',
            'GDP' : 'Seasonally Adjusted',
            'CPIAUCSL' : 'Seasonally Adjusted',
            'INDPRO' : 'Seasonally Adjusted',
            'UMCSENT' : 'Not Seasonally Adjusted',
            'M1' : 'Seasonally Adjusted',
            'M1V' : 'Seasonally Adjusted',
            'M2' : 'Seasonally Adjusted',
            'M2V' : 'Seasonally Adjusted',
            'WALCL' : 'Not Seasonally Adjusted',
            'FEDFUNDS' : 'Not Seasonally Adjusted'
        } 
        
        logger.info("Static variables configured.")
    except:
        logger.error("Failed to load static variables.")
        logger.error(json.dumps(sys.exc_info()[0]))
        raise signals.FAIL()

@task
def extract_fred():
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    logger.info("Retrieving U.S. Data")
    
    global box, fred, fred_codes_us, fips, name_mapping, adjustment_mapping, us, state
    
    try:
        # -- US Data
        for code in fred_codes_us:
            globals()[code] = fred.series_observations(code)

        us = [eval(l) for l in fred_codes_us]
        us = pd.concat(us)
        us_cols = ['date', 'geo_scope', 'geo_abbreviation', 'variable', 'value']
        us = us[us_cols]
        logger.info(f"U.S. Data Retrieved - {us.shape}")
        
        # -- State Data
        for i in range(0,len(fips)):
            for k,v in fips[i]['codes'].items():
                time.sleep(1)
                for l in v:
                    data_key = str(l)[2:].lower() + '_data'
                    data = fred.series_observations(l, scope = 'state')
                    fips[i][data_key] = data
                    
        data = ['iclaims_data', 'cclaims_data', 'cemploy_data', 'insuredur_data']

        for d in data:
            globals()[d] = list(ppy.findkeys(fips, d))
            globals()[d] = pd.concat(globals()[d])
            globals()[d]['variable'] = d.replace('_data','').upper()
            data_year_range = [datetime.strptime(x, '%Y-%m-%d') for x in globals()[d]['date'].unique()]
            logger.info(f"Dataset: {d} | Start: {min(data_year_range)} | End: {max(data_year_range)} | Shape: {globals()[d].shape}")
            
        state = pd.concat([iclaims_data, cclaims_data, cemploy_data, insuredur_data])
        state_cols = ['date', 'geo_scope', 'geo_abbreviation', 'variable', 'value']
        state = state[state_cols]
        logger.info(f"State Data Retrieved - {state.shape}")
        
        box.update_file('666716428023', us)
        box.update_file('666717301983', state)
        logger.info(f"Data uploaded to Box")
        
    except:
        logger.error("Failed to extract Data")
        logger.error(json.dumps(sys.exc_info()[0]))
        raise signals.FAIL() 
        
@task
def transform_fred():
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    logger.info("Transforming Data")
    
    global box, fred, us, state, master, datetime
    
    try:
        master = pd.concat([us, state])
        master.loc[master['value'] == '.','value'] = '' # Replace with 0 instead?
        
        master['variable_name'] = master['variable'].replace(name_mapping)
        master['variable_adjustment'] = master['variable'].replace(adjustment_mapping)

        master_cols = ['date', 'geo_scope', 'geo_abbreviation', 'variable', 'variable_name', 'variable_adjustment', 'value']
        master = master[master_cols]
        
        import getpass
        master['data_loaded_on'] = datetime.now()
        master['data_loaded_by'] = getpass.getuser()
        logger.info(f"Combined Data Shape - {master.shape}")
        
        box.update_file('666718358067', master)
        box.update_file('666715107631', master, sep='\t', index = False, header = False)
        logger.info(f"Transformed Data uploaded to Box")
        
    except:
        logger.error("Failed to tranform Data")
        logger.error(json.dumps(sys.exc_info()[0]))
        raise signals.FAIL() 
        
@task        
def load_fred():
    global ppy_sql, auth
    
    db = ppy_sql.PostgreSQL(**auth.get_secret("dev/rds/postgresql"))
    db.drop_table('dems_fred')
    # -- Prefect Setup -- #
    logger = prefect.context.get("logger")
    # -- Prefect Setup -- #
    logger.info(f"Uploading data to {db.dbname}")

    create_table_dems_fred = """
        CREATE TABLE IF NOT EXISTS dems_fred(
            fred_date DATE,
            geo_scope TEXT,
            geo_abbreviation TEXT,
            variable TEXT,
            variable_name TEXT,
            variable_adjustment TEXT,
            value DECIMAL,
            data_loaded_on TIMESTAMP,
            data_loaded_by TEXT
        )
    """
    db.execute_commit(create_table_dems_fred)
    
    f = box.get_file('666715107631')
    db.copy_from(f, 'dems_fred', sep='\t', null = '')
    
    logger.info(f"Data uploaded successfully")

cron = '15 13 * * THU'
schedule = Schedule(clocks=[CronClock(cron)])
with Flow("fred", schedule) as flow:
    a, b, c, d = create_static_variables(), extract_fred(), transform_fred(), load_fred()
    flow.add_edge(a, b)
    flow.add_edge(b, c)
    flow.add_edge(c, d)

flow.register(project_name = "probitas-production")
flow.run_agent()