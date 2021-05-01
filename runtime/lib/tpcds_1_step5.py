from tpcds_1_include import *

def main(_, action):
  table = "customer"
  table_paths = get_table_paths(table)
  names = get_name_for_table(table)
  dtypes = get_dtypes_for_table(table)
  df = read_csv_df_combine(table_paths, names, dtypes)[['c_customer_sk', 'c_customer_id']]

  transport_name = "output_step5"
  io_dict = write_table_rdma(action, transport_name, df)

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)