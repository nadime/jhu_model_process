
import pandas as pd
import numpy as np
import random
from datetime import datetime
from datetime import date
from pathlib import Path
import csv
import os
import math

import pickle
import functools
import itertools

from multiprocessing import Pool

import json
import boto

import logging

HOSP_DATA_CSV = "https://data.chhs.ca.gov/dataset/6882c390-b2d7-4b9a-aefa-2068cee63e47/resource/6cd8d424-dfaa-4bdd-9410-a3d656e1176e/download/covid19data.csv"
HOSP_DATA_NEEDED_COLS = {
                            'COVID-19 Positive Patients':           'actual_hosp',
                            'Suspected COVID-19 Positive Patients': 'actual_justsusp_hosp', 
                            'ICU COVID-19 Positive Patients':       'actual_icu', 
                            'ICU COVID-19 Suspected Patients':      'actual_justsusp_icu'
}

JHU_REMAP_COLS = {
        'hosp_curr': 'hosp_occup',
        'incidH':    'hosp_admit',
        'icu_curr':  'icu_occup',
        'incidICU':  'icu_admit',
        'incidI':    'new_infect',
        'incidD':    'new_deaths'
}

BEGIN_CA_COUNTY = 6000
END_CA_COUNTY   = 7000

OUTPUT_SUFFIXES = [ '_mean', '_median', '_q25', '_q75']

DATA_OUTPUT_COLS = ['hosp_curr','incidH','icu_curr','incidICU','incidI','incidD']

BASELOC        = "/home/ec2-user/data/kitgraphs"
LATESTLOC      = os.path.join(BASELOC,"%s" % date.today().isoformat().replace('-',''))
INPUTLOC       = os.path.join(LATESTLOC, "input")
OUTPUTLOC      = os.path.join(LATESTLOC, "output")
OUTGRAPH_LOC   = os.path.join(OUTPUTLOC,"graphs")
OUTDATA_LOC    = os.path.join(OUTPUTLOC,"data")
LATEST_SYMLINK = os.path.join(BASELOC,"latest")

STATE = 'CA'
DATESLUG = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPATH = Path('output/%s/graphs_%s' % (STATE, DATESLUG))

AWS_REGION            = 'us-east-2'
USERNAME							= 'rstudio'
S3_BUCKET_NAME        = "jhumodelaggregates"
S3_CREDBUCKET_NAME	  = "ca-covid-credentials"
S3_CREDFILE					  = S3_BUCKET_NAME + "/" + USERNAME + ".json"

# scenario name to file location
SCENARIOS = {
        'No Intervention': 'input/nonpi-hospitalization/model_output/unifiedNPI/',
        'Statewide KC 1918': 'input/kclong-hospitalization/model_output/mid-west-coast-AZ-NV_SocialDistancingLong/',
        'Statewide Lockdown 8 weeks': 'input/wuhan-hospitalization/model_output/unifiedWuhan/',
}

INFILE_PREFIX = 'high_death'

REGIONS = {}
REGION_COUNTIES = {}

def get_s3_credentials():
		conn = boto.s3.connect_to_region(AWS_REGION)
    bucket = conn.get_bucket(S3_CREDBUCKET_NAME,validate=True)
		credfile = bucket.get_key(S3_CREDFILE)
		creddict = json.loads(credfile.get_contents_as_string())
		if USERNAME != creddict['username']:
				raise Exception("Error: read credentials for wrong user (tried %s got %s)" % (USERNAME,creddict['username']))
		if AWS_REGION != creddict['aws-region']:
				raise Exception("Error: credentials region mismatch for user %s (wanted %s got %s)" % (USERNAME,AWS_REGION,creddict['aws-region']))
		if S3_BUCKET_NAME != creddict['bucketname']:
				raise Exception("Error: credentials bucket mismatch for user %s (wanted %s got %s)" % (USERNAME,S3_BUCKET_NAME,creddict['bucketname']))
		AWS_ACCESS_KEY = creddict['aws-access-key']
		AWS_SECRET_ACCESS_KEY = creddict['aws-secret-access-key']



@functools.lru_cache(maxsize=1)
def logger():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s:%(levelname)s: %(message)s')
    return logging.getLogger(__name__)

def setup_dirs():
    logger().info("Setting up directories")
    os.makedirs(INPUTLOC,exist_ok=True)
    os.makedirs(OUTGRAPH_LOC,exist_ok=True)
    os.makedirs(OUTDATA_LOC,exist_ok=True)
    if os.path.exists(LATEST_SYMLINK):
        os.unlink(LATEST_SYMLINK)
    os.symlink(LATESTLOC,os.path.join(BASELOC,"latest"))
    os.chdir(LATESTLOC)
    if not OUTPATH.exists():
        OUTPATH.mkdir(parents=True, exist_ok=True)
        
def read_region_data():
    logger().info("Getting region data")
    
    # regions from: http://www.geocurrents.info/wp-content/uploads/2016/01/California-Regions-Map-2.png
    # (but orange lumped in with inland empire)
    with open('input/%s_regions.csv' % STATE, 'r') as f:
        incsv = csv.reader(f)
        next(incsv) # ignore header
        regions = {}
        region_counties = {}
        for rec in incsv:
            regions[rec[0]] = rec[1]
            region_counties[rec[1]] = region_counties.get(rec[1], []) + [rec[0]]
    return regions,region_counties

def q25(x):
    return x.quantile(0.25)
def q75(x):
    return x.quantile(0.75)
def q50(x):
    return x.quantile(0.50)

def restrict_csv_to_ca(filename):
    input_df = pd.read_csv(filename)
    # some scenarios (e.g. Statewide KC 1918) have counties outside of CA, so ensure only CA counties present...
    return input_df.loc[(input_df['geoid'] >= BEGIN_CA_COUNTY) & (input_df['geoid'] < END_CA_COUNTY), ]

@functools.lru_cache(maxsize=None)
def read_jhu_model_output():
    logger().info("Reading JHU model output")
    # read in the raw model output scenario data for the scenarios in the SCENARIOS global...
    stack_dfs = {}
    with Pool(processes=math.ceil(os.cpu_count()/2)) as pool: # or whatever your hardware can support
        for scenario, inpath in SCENARIOS.items():
            files = [os.path.join(inpath, f) for f in os.listdir(inpath) if f.find(INFILE_PREFIX)==0]
            df_list = pool.map(restrict_csv_to_ca,files) 
            stack_dfs[scenario] = pd.concat(df_list,ignore_index=True)
    return stack_dfs

def write_csv_output(dfdict):
    logger().info("Writing scenario data to csv")
    all_agg_cols = { k+suff: JHU_REMAP_COLS[k]+suff for suff,k in itertools.product(OUTPUT_SUFFIXES,JHU_REMAP_COLS.keys()) }
    for scenario in dfdict.keys():
        all_dfs = []
        for col in JHU_REMAP_COLS.keys():
            new_df = dfdict[scenario].groupby(['time', 'sim_num']).sum().groupby('time')[col].agg([np.mean, q50, q25, q75])
				    new_df = new_df.reset_index().rename(columns={'mean': col + '_mean', 'q50': col + '_median', 'q25': col + '_q25', 'q75': col + '_q75'})
            new_df['time'] = pd.to_datetime(new_df['time'])
            all_dfs.append(new_df.sort_values(by='time'))      
        all_df = functools.reduce(lambda x,y: pd.merge(x,y,on=['time'],how='inner'),all_dfs)
        output_loc = os.path.join(OUTDATA_LOC,"%s.csv" % scenario.replace(' ','_'))
        logger().info("Writing '%s' (%d rows) to %s:" % (scenario,len(all_df),output_loc))
        all_df.rename(columns=all_agg_cols).to_csv(output_loc,header=True,index=False)
    
@functools.lru_cache(maxsize=1)
def connect_to_s3(access_key=None,secret_access_key=None,region=None):
    return boto.s3.connect_to_region(region,aws_access_key_id=access_key,aws_secret_access_key=secret_access_key)

@functools.lru_cache(maxsize=1)
def get_s3_bucket(bucket_name,access_key=None,secret_access_key=None,region=AWS_REGION):
    conn = connect_to_s3(access_key,secret_access_key,region)
    return conn.get_bucket(bucket_name,validate=True)

def write_file_to_s3(key,filepath):
    bucket = get_s3_bucket(S3_BUCKET_NAME,AWS_ACCESS_KEY,AWS_SECRET_ACCESS_KEY,AWS_REGION)
    k = boto.s3.key.Key(bucket)
    k.key = key
    k.set_contents_from_filename(filepath)

def write_scenarios_to_s3(dfdict):
    logger().info("Writing scenario data to s3")
    for s3dir in [ 'latest', date.today().isoformat().replace('-','')]:
        for scenario in dfdict.keys():
            filename = scenario.replace(' ','_')
            key = '%s/%s.csv' % (s3dir,filename)
            fullpath = os.path.join(OUTDATA_LOC,filename)+'.csv'
            write_file_to_s3(key,fullpath)

def load_actuals():
    logger().info("Loading actual hospital data")
    hosp_bycounty_df = pd.read_csv(HOSP_DATA_CSV,header=0)

    # make sure data looks right
    for col in HOSP_DATA_NEEDED_COLS.keys():
        if col not in hosp_bycounty_df.columns:
            raise Exception("DataError: could not find needed column: %s in data retrieved from %s, please check!" % (col,HOSP_DATA_CSV))

    hosp_bycounty_df = hosp_bycounty_df.rename(columns=HOSP_DATA_NEEDED_COLS)
    hosp_bycounty_df['time'] = hosp_bycounty_df['Most Recent Date'].map(lambda x: pd.to_datetime(x))

    grouped_df = hosp_bycounty_df.groupby('time')[list(HOSP_DATA_NEEDED_COLS.values())].aggregate(sum)

    hosp_df = grouped_df.reset_index()
    hosp_df['actual_susp_hosp'] = hosp_df['actual_hosp'] + hosp_df['actual_justsusp_hosp']
    hosp_df['actual_susp_icu']  = hosp_df['actual_icu'] + hosp_df['actual_justsusp_icu']
    hosp_df = hosp_df.drop(['actual_justsusp_hosp','actual_justsusp_icu'],axis=1)

    # we're missing data for 2020-03-27 - 2020-03-31, so fill in by hand
    HOSP_DATA_BY_HAND = { 'time':            map(lambda x: pd.to_datetime(x),[ '2020-03-27','2020-03-28','2020-03-29','2020-03-30','2020-03-31']),
                          'actual_hosp':     [ 1034,1253,1432,1675,1855 ],
                          'actual_susp_hosp':[ 5027,4362,3494+1432,3604+1675,np.nan],
                          'actual_icu':      [ 410,498,597,629,np.nan ],
                          'actual_susp_icu': [ 410+587,498+657,597+602,629+604,np.nan]
                        }

    new_hosp_df = pd.DataFrame(HOSP_DATA_BY_HAND)
    hosp_df = hosp_df.append(new_hosp_df)
    hosp_df = hosp_df.sort_values(by=['time'])

    # sigh [from kit]
    hosp_df['actual_hosp'] = hosp_df['actual_hosp'].astype('float')
    hosp_df['actual_susp_hosp'] = hosp_df['actual_susp_hosp'].astype('float')
    hosp_df['actual_icu'] = hosp_df['actual_icu'].astype('float')
    hosp_df['actual_susp_icu'] = hosp_df['actual_susp_icu'].astype('float')
    
    return hosp_df

def write_actuals_to_s3(input_df):
    logger().info("Writing actual hospital data to s3")
    filename = "actual_hosp_data.csv"
    actuals_outfile = os.path.join(OUTDATA_LOC,filename)
    input_df.to_csv(actuals_outfile,header=True,index=False)
1G    for s3dir in [ 'latest', date.today().isoformat().replace('-','')]:
        key = '%s/%s' % (s3dir,filename)
        write_file_to_s3(key,actuals_outfile)

def main():
    logger().info("Starting jhu model output conversion")
    setup_dirs()
    read_region_data()
    scenarios_dict = read_jhu_model_output()
    write_csv_output(scenarios_dict)
    write_scenarios_to_s3(scenarios_dict)
    # writing actuals causes problems for CA, so avoid for now
    # actuals_df = load_actuals()
    # write_actuals_to_s3(actuals_df)

if __name__ == "__main__":
		main()


