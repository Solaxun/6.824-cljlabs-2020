import re
import argparse
import collections
import os
import sys

file_to_merge = './mr-tmp/mr-out-2'
cli_in = sys.argv
if len(cli_in) > 1:
    file_to_merge = cli_in[1]

with open(file_to_merge) as f:
    res = collections.defaultdict(int)
    for l in f.readlines():
        k,v = l.rstrip().split()
        res[k] += int(v)

os.remove(file_to_merge)

with open(file_to_merge,'w') as f:
    for k,v in sorted(res.items()):
        f.write("{} {}\n".format(k,v))
