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

FSL_CONFIG = {
'index-page': '/dir?ci=tip',
}

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

def store_branches(db, gitpath, fossilpath, trackbranches=None):
    cur = db.cursor()
    cur.execute('PRAGMA case_sensitive_like=1')
    cur.execute('CREATE TABLE IF NOT EXISTS git_refs ('
        'ref TEXT PRIMARY KEY, githash TEXT '
    ')')
    git = subprocess.Popen(
        (GIT, 'for-each-ref', '--format=%(objectname)\t%(refname)'),
        stdout=subprocess.PIPE, cwd=gitpath)
    for ln in git.stdout:
        githash, ref = ln.decode().rstrip().split('\t', 1)
        if ref.startswith('refs/heads/'):
            tag = ref[11:]
        else:
            tag = ref[5:].split('/', 1)[1]
        cur.execute("REPLACE INTO git_refs VALUES (?,?)", (tag, githash))
    sql = (
        # find branch ancestors and tag them with all child branches
        "WITH RECURSIVE t(rid, tagid) AS ("
            "SELECT tagxref.rid, tagxref.tagid "
            "FROM main.git_refs gr "
            "INNER JOIN main.marks m USING (githash) "
            "INNER JOIN tag ON tag.tagname=('sym-' || gr.ref) "
            "INNER JOIN tagxref ON tagxref.rid=m.rid "
            "WHERE tagxref.tagtype=2 AND %s "
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
        "WHERE tagxref.tagtype=2 AND %s "
        "ORDER BY rid ASC, tagid ASC"
    )
    cur.execute('CREATE TABLE IF NOT EXISTS branches ('
        'rid INTEGER, tagid INTEGER, tagname TEXT, '
        'PRIMARY KEY (rid, tagid)'
    ')')
    cur.execute('ATTACH DATABASE ? AS fossil', (fossilpath,))
    if trackbranches:
        vals = ['sym-' + b for b in trackbranches]*2
        sql = sql % (('tag.tagname IN (%s)' % ','.join(
            '?' * len(trackbranches)),)*2)
        cur.execute(sql, vals)
    else:
        cur.execute(sql % (("tag.tagname LIKE 'sym-%%'",)*2))
    db.commit()

def sync(gitpath, fossilpath, markpath, rebuild=False, trackbranches=None):
    committers = collections.defaultdict(collections.Counter)
    gitname = os.path.basename(os.path.abspath(gitpath.rstrip('/')))
    fossilname = os.path.splitext(os.path.basename(fossilpath))[0]
    newfossil = not os.path.isfile(fossilpath)
    gitmarks = os.path.abspath(os.path.join(markpath, gitname + '.git-marks'))
    fossilmarks = os.path.abspath(os.path.join(markpath, fossilname + '.fossil-marks'))
    marksdbname = os.path.join(markpath, fossilname + '-marks.db')
    touch(gitmarks)
    touch(fossilmarks)
    if newfossil:
        subprocess.Popen((GIT, 'config', 'gc.auto', '0'), cwd=gitpath).wait()
    git = subprocess.Popen(
        (GIT, 'fast-export', '--all', '--signed-tags=strip',
        '--import-marks=' + gitmarks, '--export-marks=' + gitmarks),
        stdout=subprocess.PIPE, cwd=gitpath)
    fossilcmd = (FOSSIL, 'import', '--git', '--use-author', '--export-marks', fossilmarks)
    if not newfossil:
        fossilcmd += ('--no-rebuild', '--incremental', '--import-marks', fossilmarks)
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
    if newfossil or rebuild:
        subprocess.Popen((FOSSIL, 'sqlite3', '-R', fossilpath, "INSERT OR REPLACE INTO config VALUES ('project-name', '%s', now());" % fossilname)).wait()
        for row in FSL_CONFIG.items():
            subprocess.Popen((FOSSIL, 'sqlite3', '-R', fossilpath, "INSERT OR REPLACE INTO config VALUES ('%s', '%s', now());" % row)).wait()
        subprocess.Popen((FOSSIL, 'fts-config', '-R', fossilpath, 'enable', 'cdtwe')).wait()
        subprocess.Popen((FOSSIL, 'fts-config', '-R', fossilpath, 'stemmer', 'on')).wait()
        subprocess.Popen((FOSSIL, 'fts-config', '-R', fossilpath, 'index', 'on')).wait()
        subprocess.Popen((FOSSIL, 'rebuild', '--ifneeded', '--wal', '--analyze', '--index', fossilpath)).wait()
    marksdb = sqlite3.connect(marksdbname)
    store_marks(marksdb, gitmarks, fossilmarks)
    store_committers(marksdb, committers)
    store_branches(marksdb, gitpath, fossilpath, trackbranches)
    cur = marksdb.cursor()
    cur.execute('PRAGMA optimize')
    if newfossil:
        cur.execute('VACUUM')
    marksdb.commit()
    marksdb.close()

if __name__ == '__main__':
    sync(*sys.argv[1:])
