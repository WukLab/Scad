import pickle
import os
import datetime
import pandas as pd
import numpy as np
import tpcds_data_schema
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

# query_name = '1'

# TODO: use command line argument
# path_work = "/home/jil/serverless/tpcds/q" + query_name + "-temp/"
path_data = "/home/jil/serverless/tpcds/data/"

# renamed
def get_table_paths(table):
    files = []
    path = path_data
    for f in os.listdir(path):
        if f.startswith(table) and f.endswith(".csv"):
            files.append(os.path.join(path, f))
    return files

# modified
def read_csv_df(path, names, dtypes):
    names.append("")
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")
    part_data = pd.read_table(path, 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              usecols=range(len(names)-1), 
                              dtype=dtypes, 
                              na_values = "-",
                              parse_dates=parse_dates)
    return part_data

def read_csv_df_combine(paths, names, dtypes):
    return read_csv_df(paths[0], names, dtypes)
    # dfs = []
    # for path in paths:
    #     dfs.append(read_csv_df(path, names, dtypes))
    # return pd.concat(dfs)

# original
def get_name_for_table(tablename):
    schema = tpcds_data_schema.schemas[tablename]
    names = [a[0] for a in schema]
    return names

# modified
def get_type(typename):
    typename = typename.lower()
    if typename == "date":
        return datetime.datetime
    if "decimal" in typename:
        return np.dtype("float")
    if typename == "int" or typename == "long":
        return np.dtype("float")
    if typename == "float":
        return np.dtype(typename)
    if typename == "varchar" or typename == "char":
        return np.dtype("str")
    raise Exception("Not supported type: " + typename)

# original
def get_dtypes_for_table(tablename):
    schema = tpcds_data_schema.schemas[tablename]
    dtypes = {}
    for a,b in schema:
        dtypes[a] = get_type(b)
    return dtypes

key_df_names = "columns"
key_df_dtypes = "dtypes"
key_tran = "tran"
key_buffer_meta = "buf_meta"
key_array_meta = "rdma_array_meta"

def write_table_rdma(action, transport_name, df):
    to_write = df.to_numpy()

    # trans = action.get_transport(transport_name, 'rdma')
    trans = action.get_transport('client1', 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    buffer_pool = buffer_pool_lib.buffer_pool(trans)
    rdma_array = remote_array(buffer_pool, input_ndarray=to_write)

    return {
        key_df_names: df.columns,
        key_df_dtypes: df.dtypes,
        # key_tran: transport_name,
        key_tran: 'client1',
        key_array_meta: buffer_pool.get_buffer_metadata(),
        key_array_meta: rdma_array.get_array_metadata()
    }

def read_table_rdma(action, io_dict):
    names = io_dict[key_df_names]
    dtypes_raw = io_dict[key_df_dtypes]

    dtypes = {}
    if isinstance(dtypes_raw, dict):
        dtypes = dtypes_raw
    else:
        for i in range(len(names)):
            dtypes[names[i]] = dtypes_raw[i]
 
    #print(dtypes)
    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")
  
    trans = action.get_transport(io_dict[key_tran], 'rdma')
    buffer_pool = buffer_pool_lib.buffer_pool(trans, io_dict[key_buffer_meta])
    rdma_array = remote_array(buffer_pool, metadata=io_dict[key_array_meta])

    part_data = pd.read_table(rdma_array, 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)
    #print(part_data.info())
    return part_data

scale = 100
parall_1 = 100
parall_2 = 100
parall_3 = 100
parall_4 = 100
parall_5 = 1
