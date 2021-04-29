import numpy as np
import pandas as pd

'''
with open('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/ref_target_order_numbers.npy', 'rb') as f:
    ref_arr = np.load(f)
    with open('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/saved_target_order_numbers.npy', 'rb') as ff:
      saved_arr = np.load(ff)
      print(np.array_equal(ref_arr, saved_arr)) # True
'''

def check(step):
    # check with reference csv
    correct = pd.read_csv(f'/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/steps_ref/ref_output_{step}.csv', encoding='utf8', dtype={'cs_order_number': '<f4', 'cs_ext_ship_cost': '<f4', 'cs_net_profit': '<f4'})
    real = pd.read_csv(f'/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step{step}.csv', encoding='utf8', dtype={'cs_order_number': '<f4', 'cs_ext_ship_cost': '<f4', 'cs_net_profit': '<f4'})
    print(f'Checking step{step}...')
    print('correct', correct.dtypes, correct.shape)
    print(correct)
    print('real', real.dtypes, real.shape)
    print(real)
    print("[DEBUG] equals?", real.equals(correct))
    print("------------\n\n")

check(3)
check(4)
check(5)

step = 5

# special check
correct = pd.read_csv(f'/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/steps_ref/ref_output_{step}.csv', encoding='utf8', dtype={'cs_order_number': '<f4', 'cs_ext_ship_cost': '<f4', 'cs_net_profit': '<f4'})
real = pd.read_csv(f'/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step{step}_6.csv', encoding='utf8', dtype={'cs_order_number': '<f4', 'cs_ext_ship_cost': '<f4', 'cs_net_profit': '<f4'})
print(f'Checking step{step}...')
print('correct', correct.dtypes, correct.shape)
print(correct)
print('real', real.dtypes, real.shape)
print(real)
print("[DEBUG] equals?", real.equals(correct))
print("------------\n\n")

