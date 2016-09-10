#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import tempfile
import subprocess
import collections

re_variable = re.compile(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)=')
re_packagename = re.compile(r'^([a-z0-9][a-z0-9+.-]*)(.*)$')

def init_db(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS packages ('
        'name TEXT PRIMARY KEY,' # coreutils
        'category TEXT,' # base
        'section TEXT,' # utils
        'pkg_section TEXT,' # (PKGSEC)
        'version TEXT,' # 8.25
        'release TEXT,' # None
        'description TEXT'
    ')')
    cur.execute('CREATE TABLE IF NOT EXISTS package_spec ('
        'package TEXT,'
        'key TEXT,'
        'value TEXT,'
        'PRIMARY KEY (package, key)'
    ')')
    cur.execute('CREATE TABLE IF NOT EXISTS package_dependencies ('
        'package TEXT,'
        'dependency TEXT,'
        'version TEXT,'
        # PKGDEP, PKGRECOM, PKGBREAK, PKGCONFL, PKGREP, BUILDDEP
        'relationship TEXT,'
        'PRIMARY KEY (package, dependency, relationship),'
        'FOREIGN KEY(package) REFERENCES packages(name)'
        # we may have unmatched dependency package name
        # 'FOREIGN KEY(dependency) REFERENCES packages(name)'
    ')')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_package_dependencies'
                ' ON package_dependencies (package)')

def uniq(seq): # Dave Kirby
    # Order preserving
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]

def read_bash_vars(filename):
    with open(filename, 'r') as sh, tempfile.NamedTemporaryFile('w') as tmpf:
        var = []
        for ln in sh:
            match = re_variable.match(ln)
            if match:
                var.append(match.group(1))
            tmpf.write(ln)
        var = uniq(var)
        for v in var:
            tmpf.write('echo "$%s"\n' % v)
        tmpf.flush()
        outs, errs = subprocess.Popen(('bash', tmpf.name),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        lines = outs.decode().splitlines()
        return collections.OrderedDict(zip(var, lines))

def scan_abbs_tree(cur, basepath):
    categories = ('base-', 'extra-')
    for path in os.listdir(basepath):
        secpath = os.path.join(basepath, path)
        if not (os.path.isdir(secpath) and any(path.startswith(x) for x in categories)):
            continue
        category, section = path.split('-')
        for pkgpath in os.listdir(secpath):
            fullpath = os.path.join(secpath, pkgpath)
            if not os.path.isdir(fullpath):
                continue
            print(os.path.join(path, pkgpath))
            spec = read_bash_vars(os.path.join(fullpath, 'spec'))
            for dirpath, dirnames, filenames in os.walk(fullpath):
                for filename in filenames:
                    if filename != 'defines':
                        continue
                    pkgspec = spec.copy()
                    pkgspec.update(read_bash_vars(os.path.join(dirpath, 'defines')))
                    name = pkgspec.pop('PKGNAME', None)
                    section2 = pkgspec.pop('PKGSEC', None)
                    description = pkgspec.pop('PKGDES', None)
                    version = pkgspec.pop('VER', None)
                    release = pkgspec.pop('REL', None)
                    dependencies = []
                    for rel in ('PKGDEP', 'PKGRECOM', 'PKGBREAK', 'PKGCONFL', 'PKGREP', 'BUILDDEP'):
                        for pkgname in pkgspec.pop(rel, '').split():
                            deppkg, depver = re_packagename.match(pkgname).groups()
                            dependencies.append((name, deppkg, depver, rel))
                    cur.execute('REPLACE INTO packages VALUES (?,?,?,?,?,?,?)',
                        (name, category, section, section2, version, release, description))
                    for k, v in pkgspec.items():
                        cur.execute('REPLACE INTO package_spec VALUES (?,?,?)', (name, k, v))
                    cur.executemany('REPLACE INTO package_dependencies VALUES (?,?,?,?)', dependencies)

def main(dbfile, path):
    db = sqlite3.connect(dbfile)
    cur = db.cursor()
    init_db(cur)
    scan_abbs_tree(cur, path)
    db.commit()

if __name__ == '__main__':
    import sys
    sys.exit(main(*sys.argv[1:]))
