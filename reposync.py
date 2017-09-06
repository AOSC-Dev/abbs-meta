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
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('CREATE TABLE IF NOT EXISTS marks ('
        'name TEXT UNIQUE, rid INT, uuid TEXT, githash TEXT'
    ')')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_marks ON marks (rid)')
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

def store_branches(db, fossilpath):
    cur = db.cursor()
    sql = (
        # find branch ancestors and tag them with all child branches
        "WITH RECURSIVE t(rid, tagid) AS ("
            "SELECT leaf.rid, tagxref.tagid FROM leaf "
            "LEFT JOIN tagxref ON tagxref.rid=leaf.rid "
            "LEFT JOIN tag ON tag.tagid=tagxref.tagid "
            "WHERE tagxref.tagtype=2 AND tag.tagname LIKE 'sym-%' "
            "UNION "
            "SELECT plink.pid, t.tagid FROM t "
            "INNER JOIN plink ON plink.cid=t.rid "
        ") "
        "INSERT OR IGNORE INTO main.branches "
        "SELECT t.rid rid, t.tagid tagid, substr(tag.tagname, 5) tagname FROM t "
        "LEFT JOIN tag ON tag.tagid=t.tagid "
        "UNION "
        # and the branch name as in repo
        "SELECT tagxref.rid rid, tag.tagid tagid, substr(tag.tagname, 5) tagname "
        "FROM tagxref "
        "LEFT JOIN tag ON tag.tagid=tagxref.tagid "
        "WHERE tagxref.tagtype=2 AND tag.tagname LIKE 'sym-%' "
        "ORDER BY rid ASC, tagid ASC"
    )
    cur.execute('CREATE TABLE IF NOT EXISTS branches ('
        'rid INTEGER, tagid INTEGER, tagname TEXT, '
        'PRIMARY KEY (rid, tagid)'
    ')')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_branches ON branches (rid)')
    db.commit()
    cur.execute('ATTACH DATABASE ? AS fossil', (fossilpath,))
    cur.execute(sql)

def sync(gitpath, fossilpath, markpath):
    committers = collections.defaultdict(collections.Counter)
    gitname = os.path.basename(os.path.abspath(gitpath.rstrip('/')))
    fossilname = os.path.splitext(os.path.basename(fossilpath))[0]
    newfossil = False
    if not os.path.isfile(fossilpath):
        newfossil = True
        subprocess.Popen((FOSSIL, 'new', '--sha1', fossilpath)).wait()
        subprocess.Popen((FOSSIL, 'rebuild', '--wal', fossilpath)).wait()
    gitmarks = os.path.abspath(os.path.join(markpath, gitname + '.git-marks'))
    fossilmarks = os.path.abspath(os.path.join(markpath, fossilname + '.fossil-marks'))
    touch(gitmarks)
    touch(fossilmarks)
    git = subprocess.Popen(
        (GIT, 'fast-export', '--all', '--signed-tags=strip',
        '--import-marks=' + gitmarks, '--export-marks=' + gitmarks),
        stdout=subprocess.PIPE, cwd=gitpath)
    fossilcmd = (FOSSIL, 'import', '--git', '--incremental',
                 '--import-marks', fossilmarks, '--export-marks', fossilmarks)
    if not newfossil:
        fossilcmd += ('--no-rebuild',)
    fossil = subprocess.Popen(fossilcmd + (fossilpath,), stdin=subprocess.PIPE)
    for line in git.stdout:
        match = re_committer.match(line)
        if match:
            committers[match.group(2).decode('utf-8')][match.group(1).decode('utf-8')] += 1
        fossil.stdin.write(line)
    fossil.stdin.close()
    git.stdout.close()
    git.wait()
    fossil.wait()
    marksdb = sqlite3.connect(os.path.join(markpath, fossilname + '-marks.db'))
    store_marks(marksdb, gitmarks, fossilmarks)
    store_committers(marksdb, committers)
    store_branches(marksdb, fossilpath)
    cur = marksdb.cursor()
    cur.execute('PRAGMA optimize')
    if newfossil:
        cur.execute('VACUUM')
    marksdb.commit()

if __name__ == '__main__':
    sync(*sys.argv[1:])
