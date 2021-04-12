from tpcds_1_include import *

def main(_, action):
  print("[tpcds] step1: begin")

  table = "date_dim"
  table_paths = get_table_paths(table)
  names = get_name_for_table(table)
  dtypes = get_dtypes_for_table(table)
  print("[tpcds] step1: start reading csv")
  df = read_csv_df_combine(table_paths, names, dtypes)
  print("[tpcds] step1: finish reading csv")

  df = df[df['d_year']==2000][['d_date_sk']]

  transport_name = "output_step1"
  print("[tpcds] step1: start rdma write")
  io_dict = write_table_rdma(action, transport_name, df)
  print("[tpcds] step1: finish rdma write")

  print("[tpcds] step1: end")

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
main({}, action)