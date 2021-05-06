#@ type: compute
#@ dependents:
#@   - step3
#@ corunning:
#@   2_out_mem:
#@     trans: 2_out_mem
#@     type: rdma

import numpy as np
import urllib.request
import npjoin.join as join
from disaggrt.util import *

from numpy import genfromtxt


scheme_in = {
    "sr_returned_date_sk": np.dtype(np.float32),
    "sr_return_time_sk": np.dtype(np.float32),
    "sr_item_sk": np.dtype(np.float32),
    "sr_customer_sk": np.dtype(np.float32),
    "sr_cdemo_sk": np.dtype(np.float32),
    "sr_hdemo_sk": np.dtype(np.float32),
    "sr_addr_sk": np.dtype(np.float32),
    "sr_store_sk": np.dtype(np.float32),
    "sr_reason_sk": np.dtype(np.float32),
    "sr_ticket_number": np.dtype(np.float32),
    "sr_return_quantity": np.dtype(np.float32),
    "sr_return_amt": np.dtype(np.float32),
    "sr_return_tax": np.dtype(np.float32),
    "sr_return_amt_inc_tax": np.dtype(np.float32),
    "sr_fee": np.dtype(np.float32),
    "sr_return_ship_cost": np.dtype(np.float32),
    "sr_refunded_cash": np.dtype(np.float32),
    "sr_reversed_charge": np.dtype(np.float32),
    "sr_store_credit": np.dtype(np.float32),
    "sr_net_loss": np.dtype(np.float32)
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

total_csv = 216

def main(_, action):
    parIndex = getParIndex()

    remote_addr = 0
    groupList = []

    for i in range(num_csv_per_instance):
        tableurl = "http://localhost:8123/store_returns_{}_216.csv".format(
            parIndex * num_csv... + i, total_csv)
        csv = urllib.request.urlopen(tableurl)

        df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))

        # Why use this variable here?
        #wanted_columns = ['sr_customer_sk',
        #    'sr_store_sk',
        #    'sr_return_amt',
        #    'sr_returned_date_sk']

        s3_groups = 2
        indexer, groups = join.partition_on_float32(
            df['sr_returned_date_sk'], s3_groups)

        output_dtype = df.dtype
        output_size = len(df)
        output_bytes = df.nbytes

        # write output data
        trans_name = '2_out_mem'
        trans = action.get_transport(trans_name, 'rdma')
        trans.reg(output_bytes)

        output_array = asArray(trans, output_dtype, output_size)
        output_array[:] = df[indexer]

        trans.write(output_bytes, remote_addr)

        remote_addr + output_bytes
        groupList.append(groups.tolist())

    # transfer the metedata
    res = {
        # groups, array of groups 
        'groups': groupList,
        'size': remote_addr
    }
    return res

