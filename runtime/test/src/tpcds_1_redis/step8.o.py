#@ type: compute
#@ parents:
#@   - step6
#@   - step7

import pickle
import pandas as pd
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
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

def main(params, action):
    tag_print = "step8"
    print(f"[tpcds] {tag_print}: begin")

    context_dict = pickle.loads(base64.b64decode(params['step6']['meta']))
    d = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    context_dict = pickle.loads(base64.b64decode(params['step7']['meta']))
    sr = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    merged = d.merge(sr, left_on='ctr_store_sk', right_on='s_store_sk')
    merged_u = merged[['ctr_store_sk', 'c_customer_id',
                       'ctr_total_return', 'ctr_customer_sk']]
  
    r_avg = merged_u.groupby(['ctr_store_sk']).agg(
        {'ctr_total_return': 'mean'}).reset_index()
    r_avg.rename(columns={'ctr_total_return': 'ctr_avg'}, inplace=True)
  
    #print(r_avg)
    #print("aaa")
    #aaa = [d.empty, sr.empty, merged_u.empty, r_avg.empty]
    #print(",".join([str(a) for a in aaa]))
    merged_u2 = merged_u.merge(
        r_avg, left_on='ctr_store_sk', right_on='ctr_store_sk')
    final_merge = merged_u2[merged_u2['ctr_total_return']
                            > merged_u2['ctr_avg']]
    final = final_merge[['c_customer_id']].drop_duplicates().sort_values(by=[
        'c_customer_id'])
    #print(final)

    print('final', final.dtypes, final.shape)

    df = final

    '''
    # check with reference csv
    # final.to_csv('output_final.csv')
    real_final = pd.read_csv('output_final.csv', encoding='utf8', dtype={'c_customer_id':'S16'})
    real_final = real_final.select_dtypes(include=object).astype(np.dtype('S16'))
    correct_final = pd.read_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_1/ref_output_final.csv', encoding='utf8', dtype={'c_customer_id':'S16'})
    correct_final = correct_final.select_dtypes(include=object).astype(np.dtype('S16'))
    print('Checking final...')
    print('real', real_final.dtypes, real_final.shape)
    print(real_final)
    print('correct', correct_final.dtypes, correct_final.shape)
    print(correct_final)
    print("[DEBUG] equals?", real_final.equals(correct_final))
    '''

    table_key = 'df_step8'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

