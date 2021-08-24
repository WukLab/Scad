#!/usr/bin/env python3

import matplotlib.pyplot as plt
import pandas
import os

pandas.options.mode.chained_assignment = None  # default='warn'

DIR = os.path.dirname(os.path.abspath(__file__))
FNAME = 'cold-start-data.csv'
DATA_PATH = os.path.join(DIR, FNAME)

def main():
  data = pandas.read_csv(DATA_PATH)
  # assign a group to each data point
  data['group'] = data['Chain Length'].astype(str) + " / " + data["Func Exec Time"].astype(str)
  # calculate the "overhead-per-element" in the sequence
  data['overhead-per'] = data['Overhead'] / data['Chain Length']
  # data['percent-overhead'] = (data['overhead-per'] / data['Func Exec Time']) * 100
  # consistent sort order
  data = data.sort_values(by=["Chain Length", "Func Exec Time", "System", "Using Overlay", "Dependency Prewarming"])
  # Filter the data..
  data = data[data['Func Exec Time'] != 10]
  data = data[data['Using Overlay'] == False]
  # Give a nice label to each bar in the plot
  data['dep-prewarm-text'] = data['Dependency Prewarming'].map({True: 'w/ Prewarming', False: 'w/o Prewarming'})
  data['labels'] = data['System'].map(str) + " " + data['dep-prewarm-text'] + " - " + data['Cold'].map({True: "Cold", False: "Warm"})
  # drop columns we don't want in the chart
  data = data.drop(columns=['Total Execution Time', "Chain Length", "Total Function Runtime",
    "Overhead", "Func Exec Time", "dep-prewarm-text", "System", "Cold", "Using Overlay", "Dependency Prewarming"])

  # sorting of bars in each group
  cols = ['OpenWhisk w/o Prewarming - Cold', 'Poros w/o Prewarming - Cold', 'Poros w/ Prewarming - Cold',  'Poros w/ Prewarming - Warm']
  data = data.sort_values('overhead-per', ascending=False)
  # rotate table to make each system label a column
  data = data.pivot(index="group", columns="labels", values="overhead-per")
  data = data.sort_values(by="group", ascending=False)
  data = data[cols] # magic for sorting
  print(data)
  data.plot(kind='bar', legend=True)
  lim = 1200
  skip = 50
  plt.ylabel("Overhead Latency Contribution per Element (ms)")
  plt.yticks(range(0, lim, skip))
  plt.ylim([0, lim])
  plt.xlabel("Sequence Length / Element Runtime (ms)")
  plt.xticks(rotation='horizontal')
  plt.legend(title=False)
  plt.grid()
  plt.suptitle("OpenWhisk v.s. Poros Cold Start Timing")
  plt.show()

if __name__ == "__main__":
  main()
