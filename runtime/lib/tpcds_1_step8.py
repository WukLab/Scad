from tpcds_1_include import *

# depends on 6 and 7
def main(args, action):
  ret_dict = args['step6']
  df_1 = read_table_rdma(action, ret_dict['io_dict'])
  ret_dict = args['step7']
  df_2 = read_table_rdma(action, ret_dict['io_dict'])

  merged = df_1.merge(df_2, left_on='ctr_store_sk', right_on='s_store_sk')
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
  #print(cc['cc_country'])

  transport_name = "output_step8"
  io_dict = write_table_rdma(action, transport_name, merged_u)

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)
