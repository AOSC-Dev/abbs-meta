#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import socket
import hashlib
import difflib

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
import bashvar
import requests

socket.setdefaulttimeout(100)

iso_8601 = lambda t=None: time.strftime('%Y-%m-%d %H:%M:%S +0000', time.gmtime(t))

def download_and_hash(url, hashname='sha256'):
    # NOTE the stream=True parameter
    h = hashlib.new(hashname)
    r = requests.get(url, stream=True, timeout=90)
    r.raise_for_status()
    for chunk in r.iter_content(chunk_size=1024):
        if chunk: # filter out keep-alive new chunks
            h.update(chunk)
    return h.hexdigest()

def make_diff(filename, spec, olddate, chksum):
    oldspec = spec.splitlines(True)
    lines = list(reversed(oldspec))
    for i, ln in enumerate(lines.copy()):
        if ln.startswith('SRCTBL'):
            lines.insert(i, 'CHKSUM="sha256::%s"\n' % chksum)
    lines.reverse()
    return ''.join(difflib.unified_diff(oldspec, lines, filename, filename, iso_8601(olddate), iso_8601()))

def main():
    fname = sys.argv[1]
    with open(fname, 'r', encoding='utf-8') as f:
        spec = f.read()
    specdate = os.stat(fname).st_mtime
    specvars = bashvar.eval_bashvar(spec, fname)
    if 'SRCTBL' not in specvars:
        return 0
    srctbl = specvars['SRCTBL']
    if 'CHKSUM' in specvars:
        oldchksum = specvars['CHKSUM'].lower().split('::', 1)
        newsum = download_and_hash(srctbl, oldchksum[0])
        if newsum == oldchksum[1]:
            return 0
        else:
            print("%s: existing CHKSUM mismatch" % fname, file=sys.stderr)
            return 2
    newsum1 = download_and_hash(srctbl)
    newsum2 = download_and_hash(srctbl)
    if newsum1 != newsum2:
        print("%s: two sha256sum's mismatch" % fname, file=sys.stderr)
        return 1
    sys.stdout.write(make_diff(fname, spec, specdate, newsum1))
    return 0

if __name__ == '__main__':
    sys.exit(main())
