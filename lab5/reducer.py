#!/usr/bin/env python3
from collections import defaultdict
import sys

word_counts = defaultdict(int)

for line in sys.stdin:
    try:
        line = line.strip()
        word, count = line.split()
        word_counts[word] += int(count)
    except:
        continue

for word, count in word_counts.items():
    print(word, count)
