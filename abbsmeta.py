#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import sqlite3
import logging
import subprocess
import collections
import concurrent.futures

logging.basicConfig(
    format='%(asctime)s %(levelname).1s %(message)s', level=logging.INFO)

re_variable = re.compile(b'^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=')
re_packagename = re.compile(r'^([a-z0-9][a-z0-9+.-]*)(.*)$')


def init_db(cur):
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('CREATE TABLE IF NOT EXISTS packages ('
                'name TEXT PRIMARY KEY,'  # coreutils
                'category TEXT,'  # base
                'section TEXT,'  # utils
                'pkg_section TEXT,'  # (PKGSEC)
                'version TEXT,'  # 8.25
                'release TEXT,'  # None
                'description TEXT'
                ')')
    cur.execute('CREATE TABLE IF NOT EXISTS package_spec ('
                'package TEXT,'
                'key TEXT,'
                'value TEXT,'
                'PRIMARY KEY (package, key),'
                'FOREIGN KEY(package) REFERENCES packages(name)'
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
    cur.execute('CREATE INDEX IF NOT EXISTS idx_package_spec'
                ' ON package_spec (package)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_package_dependencies'
                ' ON package_dependencies (package)')


def uniq(seq):  # Dave Kirby
    # Order preserving
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


def read_bash_vars(filename):
    # we don't specify encoding here because the env will do.
    var = []
    stdin = []
    with open(filename, 'rb') as sh:
        for ln in sh:
            match = re_variable.match(ln)
            if match:
                var.append(match.group(1))
            stdin.append(ln)
        stdin.append(b'\n')
    var = uniq(var)
    for v in var:
        # workaround variables containing newlines
        stdin.append(b'echo "${%s//$\'\\n\'/\\\\n}"\n' % v)
    outs, errs = subprocess.Popen(
        ('bash',), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE).communicate(b''.join(stdin))
    if errs:
        logging.warning('%s: %s', filename, errs.decode().rstrip())
    lines = [l.replace('\\n', '\n') for l in outs.decode().splitlines()]
    try:
        assert len(var) == len(lines)
    except:
        logging.exception(filename)
    return collections.OrderedDict(zip(map(bytes.decode, var), lines))


def read_package_info(category, section, secpath, pkgpath, fullpath):
    results = []
    logging.info(os.path.join(secpath, pkgpath))
    spec = read_bash_vars(os.path.join(fullpath, 'spec'))
    for dirpath, dirnames, filenames in os.walk(fullpath):
        for filename in filenames:
            if filename != 'defines':
                continue
            pkgspec = spec.copy()
            pkgspec.update(read_bash_vars(
                os.path.join(dirpath, 'defines')))
            name = pkgspec.pop('PKGNAME', None)
            if not name:
                # we assume it is a define for some specific architecture
                # print(dirpath, pkgspec)
                continue
            section2 = pkgspec.pop('PKGSEC', None)
            description = pkgspec.pop('PKGDES', None)
            version = pkgspec.pop('VER', None)
            release = pkgspec.pop('REL', None)
            dependencies = []
            for rel in ('PKGDEP', 'PKGRECOM', 'PKGBREAK', 'PKGCONFL', 'PKGREP',
                        'BUILDDEP', 'PKGDEP_DPKG', 'PKGDEP_RPM'):
                for pkgname in pkgspec.pop(rel, '').split():
                    deppkg, depver = re_packagename.match(pkgname).groups()
                    dependencies.append((name, deppkg, depver, rel))
            results.append(((name, category, section, section2, version, release,
                             description), pkgpath, pkgspec, uniq(dependencies)))
    return results


def scan_abbs_tree(cur, basepath):
    executor = concurrent.futures.ThreadPoolExecutor(os.cpu_count())
    futures = []
    packages = {}
    categories = ('base-', 'extra-')
    for path in os.listdir(basepath):
        secpath = os.path.join(basepath, path)
        if not (os.path.isdir(secpath) and any(path.startswith(x) for x in categories)):
            continue
        category, section = path.split('-')
        for pkgpath in os.listdir(secpath):
            fullpath = os.path.join(secpath, pkgpath)
            if not os.path.isdir(fullpath) or os.path.islink(fullpath):
                continue
            futures.append(executor.submit(
                read_package_info, category, section, path, pkgpath, fullpath))
    for future in futures:
        for result in future.result():
            pkginfo, pkgpath, pkgspec, pkgdep = result
            name = pkginfo[0]
            if name in packages:
                cat, sec, ppath = packages[name]
                logging.error(
                    'duplicate package "%s" found in %s-%s/%s and %s-%s/%s',
                    name, cat, sec, ppath, pkginfo[1], pkginfo[2], pkgpath
                )
            else:
                packages[name] = (pkginfo[1], pkginfo[2], pkgpath)
            cur.execute('REPLACE INTO packages VALUES (?,?,?,?,?,?,?)', pkginfo)
            pkgspec_old = [k[0] for k in cur.execute(
                'SELECT key FROM package_spec WHERE package = ? ORDER BY key ASC',
                (name,))]
            if pkgspec_old != sorted(pkgspec.keys()):
                logging.debug('updated spec: %s', name)
                cur.execute('DELETE FROM package_spec WHERE package = ?', (name,))
            for k, v in pkgspec.items():
                cur.execute('REPLACE INTO package_spec VALUES (?,?,?)', (name, k, v))
            pkgdep_old = cur.execute(
                'SELECT dependency, relationship FROM package_dependencies WHERE package = ? ORDER BY dependency, relationship ASC', (name,)).fetchall()
            if pkgdep_old != sorted((x[1], x[3]) for x in pkgdep):
                logging.debug('updated dependencies: %s', name)
                cur.execute('DELETE FROM package_dependencies WHERE package = ?',
                    (pkginfo[0],))
            cur.executemany('REPLACE INTO package_dependencies VALUES (?,?,?,?)', pkgdep)
    packages_old = set(x[0] for x in cur.execute('SELECT name FROM packages'))
    removed = packages_old.difference(packages.keys())
    if removed:
        for name in removed:
            cur.execute('DELETE FROM packages WHERE name = ?', (name,))
            cur.execute('DELETE FROM package_spec WHERE package = ?', (name,))
            cur.execute('DELETE FROM package_dependencies WHERE package = ?', (name,))
            logging.info('removed: ' + name)
    logging.info('Done.')


def main(dbfile, path):
    db = sqlite3.connect(dbfile)
    cur = db.cursor()
    init_db(cur)
    scan_abbs_tree(cur, path)
    db.commit()

if __name__ == '__main__':
    import sys
    sys.exit(main(*sys.argv[1:]))
