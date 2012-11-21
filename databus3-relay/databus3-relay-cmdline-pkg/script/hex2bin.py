#!/usr/bin/python

import re
import sys

def tobyte(match):
    return chr(int(match.group(0), 16))

for line in sys.stdin:
    l = re.sub(r"[G-Zg-z,. ]+", "", line.strip())
    l = re.sub(r"[0-9a-f]{2}", tobyte, l)
    sys.stdout.write(l)