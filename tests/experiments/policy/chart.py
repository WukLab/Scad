#!/usr/bin/env python3

import argparse
import re
import pandas as pd
import datetime
import matplotlib.pyplot as plt

def parse_raw_resources(file):
  res = []
  for line in open(file, 'r'):
    temp = re.findall(r'\d+', line)
    nums = list(map(int, temp))
    [2021, 12, 14, 9, 25, 1, 514, 71, 0, 95104, 183808, 7, 0, 175616, 175616, 48, 0, 49152, 196608]
    tm = datetime.datetime(nums[0], nums[1], nums[2], nums[3], nums[4], nums[5], 1000*nums[6])
    line = [
      tm,
      nums[7],
      nums[9],
      nums[10],
      nums[11],
      nums[13],
      nums[14],
      nums[15],
      nums[17],
      nums[18]
    ]
    res.append(line)
  return res

def main():

  parser = argparse.ArgumentParser()
  parser.add_argument("--file", help='file to parse', default="resources.txt")

  args = parser.parse_args()

  result = parse_raw_resources(args.file)
  cols = [
    'time',
    'cpu-cpu',
    'cpu-mem',
    'cpu-storage',
    'mem-cpu',
    'mem-mem',
    'mem-storage',
    'bal-cpu',
    'bal-mem',
    'bal-storage',
  ]
  df = pd.DataFrame(result, columns=cols)
  print(df)

  fig, ax = plt.subplots(3, 1)

  # plot CPU
  df.plot(x='time', y='bal-cpu', ax=ax[0])
  df.plot(x='time', y='cpu-cpu', ax=ax[1])
  df.plot(x='time', y='mem-cpu', ax=ax[2])

  # plot mem
  color = 'tab:red'
  df.plot(x='time', y='bal-mem', color=color, ax=ax[0].twinx())
  df.plot(x='time', y='cpu-mem', color=color, ax=ax[1].twinx())
  df.plot(x='time', y='mem-mem', color=color, ax=ax[2].twinx())
  plt.show()



if __name__ == "__main__":
  main()
