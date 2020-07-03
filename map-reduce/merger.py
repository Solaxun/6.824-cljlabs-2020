import re
import argparse
import collections
import os
import sys

file_to_merge = None

cli_in = sys.argv
if len(cli_in) > 1:
    file_to_merge = cli_in[1]
else:
    raise ValueError("Must provide a file to run, ex: 'python merger.py foo.txt'")

merge_type = os.path.basename(file_to_merge)

if merge_type == 'mr-wc-all':
    with open(file_to_merge) as f:
        res = collections.defaultdict(int)
        for l in f.readlines():
            k,v = l.rstrip().split()
            res[k] += int(v)

    os.remove(file_to_merge)

    with open(file_to_merge,'w') as f:
        for k,v in sorted(res.items()):
            f.write("{} {}\n".format(k,v))

elif merge_type == 'mr-indexer-all':
    with open(file_to_merge) as f:
        res = {}
        for l in f.readlines():
            l = l.rstrip()
            word,cnt,files = l.split()
            if word in res:
                c, f = res[word]
                f = f.split(',')
                files = files.split(',')
                res[word] = [int(cnt) + int(c),",".join(sorted(f + files))]
            else:
                res[word] = [int(cnt),files]

    os.remove(file_to_merge)

    with open(file_to_merge,'w') as f:
        for word, cnt_and_files in sorted(res.items()):
            cnt, files = cnt_and_files
            f.write("{} {} {}\n".format(word,cnt,files))

else:
    raise ValueError('Only can merge `mr-wc-all` or `mr-indexer-all`')
