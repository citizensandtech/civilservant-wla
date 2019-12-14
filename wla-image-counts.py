
# coding: utf-8

# # Goal
# 
# 1. verify
# Hi Max, I wrote code to parse the mediacounts file, and I was disappointed to see how incomplete the data is.# The webrequests tables only had records for 608,991 images, and only 200 out of the 39,114 WikiLovesAfrica images.
# 
# 
# ## Notes:
# 1. Catgories: Images_from_Wiki_Loves_Africa_2017, have many subcategories
# 2. Is nathan searching for the usage on non-commons wikis?

# In[1]:


import bz2, codecs, csv, zipfile, glob, re, csv, pymysql, os, datetime, urllib, sys
import csv, json, argparse, sys, datetime, os, re, time, bz2
from collections import defaultdict, Counter
# from dateutil import parser

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pymysql.err import InternalError, OperationalError
import sys, os
import pandas as pd
import numpy as np


# In[2]:


# These are taken from
# http://dumps.wikimedia.org/other/mediacounts/README.txt
FIELDS = [
    "filename",
    "total_response_bytes",
    "total_transfers",
    "total_transfers_raw",
    "total_transfers_audio",
    "reserved6",
    "reserved7",
    "total_transfers_image",
    "total_transfers_image_0x199",
    "total_transfers_image_200x399",
    "total_transfers_image_400x599",
    "total_transfers_image_600x799",
    "total_transfers_image_800x999",
    "total_transfers_image_1000plus",
    "reserved15",
    "reserved16",
    "total_transfers_movie",
    "total_transfers_movie_0x239",
    "total_transfers_movie_240x479",
    "total_transfers_movie_480plus",
    "reserved21",
    "reserved22",
    "total_transfers_refer_wmf",
    "total_transfers_refer_nonwmf",
    "total_transfers_refer_invalid"
]


# In[3]:


wiki_database = "commonswiki_p"

constr = 'mysql+pymysql://{user}:{pwd}@{host}/commonswiki_p?charset=utf8'.format(user=os.environ['WMF_MYSQL_USERNAME'],
                                                      pwd=os.environ['WMF_MYSQL_PASSWORD'],
                                                      host=os.environ['WMF_MYSQL_HOST'],
                                                                     use_unicode=True)

con = create_engine(constr, encoding='utf-8')

def use_commons_exec():
    con.execute(f'use commonswiki_p;')
    
def wmftimestamp(bytestring):
    if bytestring:
        s = bytestring.decode('utf-8')
        return dt.strptime(s, '%Y%m%d%H%M%S')
    else:
        return bytestring
    

def decode_or_nan(b):
    return b.decode('utf-8') if b else float('nan')
    
use_commons_exec()

wla_years = [2014,2015,2016,2017,2019]


# In[4]:


def get_wla_image_titles_from_year(year):
    year_category = f"Images_from_Wiki_Loves_Africa_{year}"
    print(f"Year category is: {year_category}")
    year_cat_sql = f'''        SELECT img_actor, img_name 
            FROM image, page, categorylinks
            WHERE page.page_id=categorylinks.cl_from 
               AND image.img_name = page.page_title
               AND .categorylinks.cl_to = "{year_category}"'''
    use_commons_exec()
    year_cat_df = pd.read_sql(year_cat_sql, con)
    # year_cat_df['img_actor'] = year_cat_df['img_actor'].apply(decode_or_nan)
    year_cat_df['img_name'] = year_cat_df['img_name'].apply(decode_or_nan)
    year_cat_df['year']=year
    print(f"Number results are: {len(year_cat_df)}")
    return year_cat_df


# In[5]:


try:
    img_df = pd.read_pickle('cache/img_df.pickle')
except FileNotFoundError:
    wla_image_titles_dfs = [get_wla_image_titles_from_year(year) for year in wla_years]
    img_df = pd.concat(wla_image_titles_dfs)
    img_df.to_pickle('cache/img_df.pickle')


# ## borrowing from https://github.com/hay/wiki-tools/blob/master/etc/mediacounts-stats.py

# In[16]:


# %load https://raw.githubusercontent.com/hay/wiki-tools/master/etc/mediacounts-stats.py

def log(msg):
    print(msg)

def process(datafile, query):
    match_rows = []
#     log("Doing %s" % datafile)
    if datafile.endswith("bz2"):
        tsvfile = bz2.open(datafile, "rt")
    else:
        tsvfile = open(datafile)
    tsvfilesize = os.path.getsize(datafile)
    # Actually benefit from the generator, e.g. batch
    query = frozenset(query)

    for index, line in enumerate(tsvfile):
        row = line.split("\t")
        
        filename = row[0].split("/")[-1]
        if filename not in query:
            continue

        clean_row = [e.strip() for e in row]
        row_series = pd.Series(clean_row)
        match_rows.append(row_series)
    tsvfile.close()
    return match_rows


def parse_mediacounts(tsvs, target_filenames):
    print(f'Looking at {len(tsvs)} TSVs')
    query = target_filenames
    log("Searching statistics for %d files" % len(query))
    match_rows_dfs = []
    for tsv in tsvs:
        cache_key = os.path.basename(tsv)
        cache_file = f'cache/{cache_key}.result.pickle'
        if os.path.exists(cache_file):
            match_rows_df = pd.read_pickle(cache_file)
            match_rows_dfs.append(match_rows_df)
            sys.stdout.write('c')
        else:
            now = time.time()
            match_rows = process(tsv, query)
            match_rows_df = pd.DataFrame(match_rows)
            match_rows_df.columns = FIELDS
            match_rows_df['tsv_name'] = cache_key
            match_rows_df.to_pickle(cache_file)
            match_rows_dfs.append(match_rows_df)
            log("%s took %s seconds" % (tsv, round(time.time() - now, 2)))
    return match_rows_dfs


def make_wla_views_counts_df(year):
    try:
        target_filenames = frozenset(img_df['img_name'].values)
    except NameError:
        print("please compute img_df first")
        return
    
    tsvs_base = "/public/dumps/public/other/mediacounts/daily/"
    tsvs_year = os.path.join(tsvs_base, str(year))
    tsvs_all_year_files = os.listdir(tsvs_year)
    tsvs_rel = [path for path in tsvs_all_year_files if '.tsv.bz2' in path]
    tsvs = [os.path.join(tsvs_year, f) for f in tsvs_rel]
    tsvs = sorted(tsvs)
    match_rows_tsv = parse_mediacounts(tsvs, target_filenames)
    
    counts_df = pd.concat(match_rows_tsv)
    outfile = f'output/wla_mediacounts_{year}.csv'
    print(f'saving outfile: {outfile}')
    counts_df.to_csv(outfile, index=False)
    return counts_df


# In[ ]:


for year in wla_years:
    make_wla_views_counts_df(year+1)

