#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Increase REL in spec files
'''

import re
import sys

re_variable = re.compile('^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=(.+)$')

def increaserel(spec):
    var = {}
    lines = spec.splitlines(True)
    for k, ln in enumerate(lines):
        match = re_variable.match(ln)
        if match:
            var[match.group(1)] = (k, match.group(2))
    if 'REL' in var:
        lines[var['REL'][0]] = 'REL=%d\n' % (int(var['REL'][1]) + 1)
    elif 'VER' in var:
        lines.insert(var['VER'][0] + 1, 'REL=1\n')
    else:
        lines.append('REL=1\n')
    return ''.join(lines)


if __name__ == '__main__':
    spec = open(sys.argv[1], 'r').read()
    sys.stdout.write(increaserel(spec))
