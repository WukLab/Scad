#@ type: compute
#@ parents:
#@   - step2
#@   - step3
#@   - step4
#@ dependents:
#@   - step7

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join
from numpy_groupies.aggregate_numpy import aggregate

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
    "d_date_sk":            np.dtype(np.float32),
    "d_date_id":            np.dtype('S16'),
    "d_date":               np.dtype('S10'),
    "d_month_seq":          np.dtype(np.float32),
    "d_week_seq":           np.dtype(np.float32),
    "d_quarter_seq":        np.dtype(np.float32),
    "d_year":               np.dtype(np.float32),
    "d_dow":                np.dtype(np.float32),
    "d_moy":                np.dtype(np.float32),
    "d_dom":                np.dtype(np.float32),
    "d_qoy":                np.dtype(np.float32),
    "d_fy_year":            np.dtype(np.float32),
    "d_fy_quarter_seq":     np.dtype(np.float32),
    "d_fy_week_seq":        np.dtype(np.float32),
    "d_day_name":           np.dtype('S16'),
    "d_quarter_name":       np.dtype('S8'),
    "d_holiday":            np.dtype('S1'),
    "d_weekend":            np.dtype('S1'),
    "d_following_holiday":  np.dtype('S1'),
    "d_first_dom":          np.dtype(np.float32),
    "d_last_dom":           np.dtype(np.float32),
    "d_same_day_ly":        np.dtype(np.float32),
    "d_same_day_lq":        np.dtype(np.float32),
    "d_current_day":        np.dtype('S1'),
    "d_current_week":       np.dtype('S1'),
    "d_current_month":      np.dtype('S1'),
    "d_current_quarter":    np.dtype('S1'),
    "d_current_year":       np.dtype('S1'),
}

def main(params, action):
    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    context_dict = pickle.loads(base64.b64decode(params['step2']['meta']))
    ws_wh = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    cs = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    context_dict = pickle.loads(base64.b64decode(params['step4']['meta']))
    cr = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    d = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    cs_sj_f1 = cs.loc[cs['ws_order_number'].isin(ws_wh['ws_order_number'])]

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(cr.wr_order_number)]    
    
    dd = d[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date'].apply(bytes.decode)) > pd.to_datetime('1999-02-01')) & (pd.to_datetime(dd['d_date'].apply(bytes.decode)) < pd.to_datetime('1999-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]
    
    df = cs_sj_f2.merge(dd_filtered, left_on='ws_ship_date_sk', right_on='d_date_sk')
    del dd
    del cs_sj_f2
    del dd_select
    del dd_filtered
    df.drop('d_date_sk', axis=1, inplace=True)

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

