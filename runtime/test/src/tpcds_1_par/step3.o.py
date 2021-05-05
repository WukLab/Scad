#@ type: compute
#@ parents:
#@   - step1
#@   - step2
#@ dependents:
#@   - step4
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma
#@   2_out_mem:
#@     trans: 2_out_mem
#@     type: rdma
#@   3_out_mem:
#@     trans: 3_out_mem
#@     type: rdma

import numpy as np
from disaggrt.util import *
from npjoin import join

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

s1_par = 1
s2_par = 2
s3_par = 2
s4_par = 4
s1_dtype = np.dtype([('d_date_sk', 'f4')])
s2_dtype = build_dtype(scheme_in)

trans1_size = 1024 * 1024 * 256
trans2_size = 1024 * 512 * s2_dtype.itemsize

def main(params, action):
    # load data from previous steps
    parIndex = getParIndex()
    print(params)

    trans_s1 = initTrans(action, '1_out_mem', s1_par, trans1_size)
    group_s1, offset_s1, _trans_s1 = getGroups(
        params['step1'], trans_s1, parIndex)
    print('s1', group_s1, offset_s1)
    arr_s1 = ArrayLoader(_trans_s1, s1_dtype,
                group_s1, offset_s1)

    trans_s2 = initTrans(action, '2_out_mem', s2_par, trans2_size)
    group_s2, offset_s2, _trans_s2 = getGroups(
        params['step2'], trans_s2, parIndex)
    print('s2', group_s2, offset_s2, _trans_s2)
    arr_s2 = ArrayLoader(_trans_s2, s2_dtype,
                group_s2, offset_s2)

    df1 = arr_s1.getAll()
    print('after load df1')
    df2 = arr_s2.getAll()
    print('after load df2')

    # calculate
    join_meta = join.prepare_join_float32(df1['d_date_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, df2['sr_returned_date_sk'])

    print('after join', len(df1_idx))

    df2k1 = df2['sr_customer_sk'][df2_idx]
    df2k2 = df2['sr_store_sk'][df2_idx]
    print('after load', len(df2k1))
    # partition
    indexer, groups = join.partition_on_float32_pair(
        df2k1, df2k2, s4_par)
    print('after partition')

    # since we are using index, we need to deindex
    df1_idx = join.deindex(df1_idx, indexer)
    df2_idx = join.deindex(df2_idx, indexer)
    print('after deindex')

    # build transport
    out_dtype = join.merge_dtypes(df1, df2, 
            [n for n in df1.dtype.names if n != 'd_date_sk'],
            [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])
    out_size = len(df1_idx)
    out_bytes = out_size * out_dtype.itemsize

    trans_s3_name = '3_out_mem'
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(out_bytes)

    # init rdma array, and allocate buffer
    join.structured_array_merge(trans_s3.buf,
            df1, df2, df1_idx, df2_idx,
            [n for n in df1.dtype.names if n != 'd_date_sk'],
            [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])

    # write back
    remoteWrite(trans_s3, 0, 0, out_bytes)

    # transfer the metedata
    res = {
        # groups, array of groups 
        'groups': [groups.tolist()],
        'size': out_bytes
    }


