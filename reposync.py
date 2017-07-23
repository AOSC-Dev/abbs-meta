#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import sqlite3
import subprocess
import collections

re_committer = re.compile(b'^committer (.+) <(.+)> \d+.+$')

GIT = os.environ.get('GIT', 'git')
FOSSIL = os.environ.get('FOSSIL', 'fossil')

def touch(filename):
    open(filename, 'a').close()

def store_marks(db, gitmarks, fossilmarks):
    cur = db.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS marks ('
        'name TEXT UNIQUE, rid INT, uuid TEXT, githash TEXT'
    ')')
    with open(fossilmarks, 'r') as f:
        for ln in f:
            toks = ln.rstrip().split(' ')
            cur.execute(
                'INSERT OR IGNORE INTO marks (name, rid, uuid) VALUES (?,?,?)',
                (toks[1], int(toks[0][1:]), toks[2])
            )
    with open(gitmarks, 'r') as f:
        for ln in f:
            toks = ln.rstrip().split(' ')
            cur.execute(
                'UPDATE marks SET githash=? WHERE name=?', (toks[1], toks[0])
            )
    db.commit()

def store_committers(db, committers):
    cur = db.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS committers ('
        'email TEXT PRIMARY KEY, name TEXT'
    ')')
    for k, v in committers.items():
        cur.execute(
            'REPLACE INTO committers VALUES (?,?)',
            (k, v.most_common(1)[0][0])
        )
    db.commit()

def sync(gitpath, fossilpath, markpath):
    committers = collections.defaultdict(collections.Counter)
    gitname = os.path.basename(os.path.abspath(gitpath.rstrip('/')))
    fossilname = os.path.splitext(os.path.basename(fossilpath))[0]
    if not os.path.isfile(fossilpath):
        subprocess.Popen((FOSSIL, 'new', '--sha1', fossilpath)).wait()
    gitmarks = os.path.abspath(os.path.join(markpath, gitname + '.git-marks'))
    fossilmarks = os.path.abspath(os.path.join(markpath, fossilname + '.fossil-marks'))
    touch(gitmarks)
    touch(fossilmarks)
    git = subprocess.Popen(
        (GIT, 'fast-export', '--all', '--signed-tags=strip',
        '--import-marks=' + gitmarks, '--export-marks=' + gitmarks),
        stdout=subprocess.PIPE, cwd=gitpath)
    fossil = subprocess.Popen(
        (FOSSIL, 'import', '--git', '--incremental',
        '--import-marks', fossilmarks, '--export-marks', fossilmarks,
        fossilpath), stdin=subprocess.PIPE)
    for line in git.stdout:
        match = re_committer.match(line)
        if match:
            committers[match.group(2)][match.group(1)] += 1
        fossil.stdin.write(line)
    fossil.stdin.close()
    git.stdout.close()
    git.wait()
    fossil.wait()
    marksdb = sqlite3.connect(os.path.join(markpath, fossilname + '-marks.db'))
    store_marks(marksdb, gitmarks, fossilmarks)
    store_committers(marksdb, committers)

if __name__ == '__main__':
    sync(*sys.argv[1:])
