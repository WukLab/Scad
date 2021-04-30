#@ type: compute
#@ parents:
#@   - step3
#@   - step4
#@ dependents:
#@   - step6

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import *

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
from npjoin import join

import pandas as pd
import redis
from io import BytesIO
import datetime

redis_hostname = "localhost"
redis_port = 6379
redis_client = None

def redis_write_pd(key, table, client=redis_client):
    csv_buffer = BytesIO()
    table.to_csv(csv_buffer, sep="|", header=False, index=False)
    if client == None:
        redis_client = redis.Redis(host=redis_hostname, port=redis_port, db=0)
        client = redis_client
    client.set(key, csv_buffer.getvalue())

def redis_read_pd(key, names, dtypes, client=redis_client):
    if client == None:
        redis_client = redis.Redis(host=redis_hostname, port=redis_port, db=0)
        client = redis_client

    dtypes_raw = dtypes
    if isinstance(dtypes_raw, dict):
        dtypes = dtypes_raw
    else:
        dtypes = {}
        for i in range(len(names)):
            dtypes[names[i]] = dtypes_raw[i]

    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")
  
    return pd.read_table(BytesIO(client.get(key)), 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)

scheme_in = {
    "web_site_sk": np.dtype(np.float32),
    "web_site_id": np.dtype('S16'),
    "web_rec_start_date": np.dtype('S10'),
    "web_rec_end_date": np.dtype('S10'),
    "web_name": np.dtype('S50'),
    "web_open_date_sk": np.dtype(np.float32),
    "web_close_date_sk": np.dtype(np.float32),
    "web_class": np.dtype('S50'),
    "web_manager": np.dtype('S40'),
    "web_mkt_id": np.dtype(np.float32),
    "web_mkt_class": np.dtype('S50'),
    "web_mkt_desc": np.dtype('S100'),
    "web_market_manager": np.dtype('S40'),
    "web_company_id": np.dtype(np.float32),
    "web_company_name": np.dtype('S50'),
    "web_street_number": np.dtype('S10'),
    "web_street_name": np.dtype('S60'),
    "web_street_type": np.dtype('S15'),
    "web_suite_number": np.dtype('S10'),
    "web_city": np.dtype('S60'),
    "web_county": np.dtype('S30'),
    "web_state": np.dtype('S2'),
    "web_zip": np.dtype('S10'),
    "web_country": np.dtype('S20'),
    "web_gmt_offset": np.dtype(np.float32),
    "web_tax_percentage": np.dtype(np.float32),
}

def main(params, action):
    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    cs = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    context_dict = pickle.loads(base64.b64decode(params['step4']['meta']))
    ca = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    tableurl = "http://localhost:8123/web_site.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    cc = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('ws_ship_addr_sk', axis=1, inplace=True)
    
    cc_p = cc[cc['web_company_name'] == 'pri'.encode()][['web_site_sk']]
    
    df = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')
    
    df = df[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]
 
    table_key = 'df_step5'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

