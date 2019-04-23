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

def make_diff(filename, spec, olddate, chksum, replace=False):
    oldspec = spec.splitlines(True)
    lines = []
    for ln in oldspec:
        if replace:
            if ln.startswith('CHKSUM'):
                lines.append('CHKSUM="sha256::%s"\n' % chksum)
                continue
        elif ln.startswith('SRCTBL'):
            lines.append(ln)
            lines.append('CHKSUM="sha256::%s"\n' % chksum)
            continue
        lines.append(ln)
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
        hashtype = oldchksum[0]
    else:
        hashtype = 'sha256'
    newsum1 = download_and_hash(srctbl, hashtype)
    newsum2 = download_and_hash(srctbl, hashtype)
    replace = False
    if newsum1 != newsum2:
        print("%s: two sha256sum's mismatch" % fname, file=sys.stderr)
        return 1
    elif newsum1 == oldchksum[1]:
        return 0
    else:
        print("%s: existing CHKSUM mismatch" % fname, file=sys.stderr)
        replace = True
    sys.stdout.write(make_diff(fname, spec, specdate, newsum1, replace))
    return 0

if __name__ == '__main__':
    sys.exit(main())
