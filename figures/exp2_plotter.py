import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
import seaborn as sns
import pandas as pd

df = pd.read_csv('./exp2_results.csv')

fig, ax = plt.subplots()
fig.set_size_inches(5, 4.5)
lines = sns.lineplot(data=df, x='Element Length (ms)', y='Per-Element Overhead (ms)', hue='method', ax=ax, markers=['o', 'v', '^', '<', '>', 's'], style='method', dashes=False, markersize=9)

ax.set_xscale('log')
# ax.set_ylim([0,125])
ax.set_yscale('log')
ax.set_yticks([10, 100, 1000])
ax.set_yticklabels([10, 100, 1000])
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles, labels=labels, loc='upper center', ncol=2, bbox_to_anchor=(0.5, 1.4),fontsize=13)
formatter = ScalarFormatter()
formatter.set_scientific(False)
ax.xaxis.set_major_formatter(formatter)
ax.minorticks_off()

for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
    item.set_fontsize(18)

fig.tight_layout()
plt.show()
plt.savefig('exp2.pdf')
