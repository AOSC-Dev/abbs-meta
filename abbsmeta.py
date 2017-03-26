#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import logging
import subprocess
import collections
import concurrent.futures

logging.basicConfig(
    format='%(asctime)s %(levelname).1s %(message)s', level=logging.INFO)

re_variable = re.compile(b'^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=')
re_packagename = re.compile(r'^([a-z0-9][a-z0-9+.-]*)(.*)$')
abbs_categories = frozenset(('core-', 'base-', 'extra-'))


def init_db(cur):
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('CREATE TABLE IF NOT EXISTS packages ('
                'name TEXT PRIMARY KEY,'  # coreutils
                'tree TEXT,'      # abbs tree name
                'category TEXT,'  # base
                'section TEXT,'  # utils
                'pkg_section TEXT,'  # (PKGSEC)
                'directory TEXT,' # second-level dir in aosc-os-abbs
                'version TEXT,'  # 8.25
                'release TEXT,'  # None
                'description TEXT,'
                'commit_time INTEGER' # git commit time
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
    except AssertionError:
        logging.exception(filename)
    return collections.OrderedDict(zip(map(bytes.decode, var), lines))


def read_commit_time(basepath, filename):
    outs, errs = subprocess.Popen(
        ('git', 'log', '-n', '1', '--pretty=format:%at', '--', filename),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=basepath).communicate()
    if errs:
        logging.warning('git %s: %s', basepath, errs.decode().rstrip())
    return int(outs.decode().strip())


def read_diff(basepath, lastupdate):
    # gitrevisions(7):
    # Note that this looks up the state of your local ref at a given time
    outs, errs = subprocess.Popen(
        ('git', 'diff', '--name-only', '@{@%d}' % lastupdate),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=basepath).communicate()
    if errs:
        logging.warning('git %s: %s', basepath, errs.decode().rstrip())
    return outs.decode().splitlines()


def read_package_info(tree, category, section, basepath, secpath, pkgpath):
    results = []
    repopath = os.path.join(secpath, pkgpath)
    logging.info(repopath)
    fullpath = os.path.join(basepath, repopath)
    spec = read_bash_vars(os.path.join(fullpath, 'spec'))
    commit_time = read_commit_time(basepath, os.path.join(repopath, 'spec'))
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
            results.append((
                (name, tree, category, section, section2, pkgpath, version, release,
                 description, commit_time), pkgpath, pkgspec, uniq(dependencies)
            ))
    return results


def list_abbs_dir(basepath, diff=None):
    if diff:
        pkgs = set()
        for filename in diff:
            pathspl = filename.split('/', 2)
            if not len(pathspl) > 2:
                continue
            path, pkgpath = pathspl[:2]
            if any(path.startswith(x) for x in abbs_categories):
                category, section = path.split('-', 1)
            else:
                category, section = None, path
            if (path, pkgpath) not in pkgs:
                fullpath = os.path.join(basepath, path, pkgpath)
                exists = (os.path.isdir(fullpath) and not os.path.islink(fullpath))
                if exists and not os.path.isfile(os.path.join(fullpath, 'spec')):
                    continue
                yield category, section, path, pkgpath, exists
                pkgs.add((path, pkgpath))
    else:
        for path in os.listdir(basepath):
            secpath = os.path.join(basepath, path)
            if not os.path.isdir(secpath):
                continue
            if any(path.startswith(x) for x in abbs_categories):
                category, section = path.split('-', 1)
            else:
                category, section = None, path
            for pkgpath in os.listdir(secpath):
                fullpath = os.path.join(secpath, pkgpath)
                if not os.path.isdir(fullpath) or os.path.islink(fullpath):
                    continue
                elif not os.path.isfile(os.path.join(fullpath, 'spec')):
                    continue
                yield category, section, path, pkgpath, True


def scan_abbs_tree(cur, basepath, tree):
    executor = concurrent.futures.ThreadPoolExecutor(os.cpu_count())
    futures = []
    packages_old = {row[0]:row[1:] for row in cur.execute(
        'SELECT name, category, section, directory FROM packages WHERE tree = ?',
        (tree,))}
    packages_other = {row[0]:row[1:] for row in cur.execute(
        'SELECT name, category, section, directory, tree'
        ' FROM packages WHERE tree != ?', (tree,))}
    removed = []
    try:
        last_updated = cur.execute(
            'SELECT commit_time FROM packages'
            ' WHERE tree = ? ORDER BY commit_time DESC LIMIT 1', (tree,)
            ).fetchone()[0]
        logging.info('using git diff from %d' % last_updated)
    except Exception:
        last_updated = None
    diff = read_diff(basepath, last_updated) if last_updated else None
    for category, section, path, pkgpath, exists in list_abbs_dir(basepath, diff):
        if exists:
            futures.append(executor.submit(read_package_info,
                tree, category, section, basepath, path, pkgpath))
        else:
            removed.extend(r[0] for r in cur.execute(
                'SELECT name FROM packages WHERE'
                ' category = ? AND section = ? AND directory = ?',
                (category, section, pkgpath)
            ))
    for future in futures:
        for result in future.result():
            pkginfo, pkgpath, pkgspec, pkgdep = result
            name = pkginfo[0]
            if name in packages_old:
                cat, sec, ppath = packages_old[name]
                if (cat, sec, ppath) != (pkginfo[2], pkginfo[3], pkgpath):
                    logging.error(
                        'duplicate package "%s" found in %s-%s/%s and %s-%s/%s',
                        name, cat, sec, ppath, pkginfo[2], pkginfo[3], pkgpath
                    )
            elif name in packages_other:
                cat, sec, ppath, othertree = packages_other[name]
                logging.error(
                    'duplicate package "%s" found in %s/%s-%s/%s and %s/%s-%s/%s',
                    name, othertree, cat, sec, ppath,
                    tree, pkginfo[2], pkginfo[3], pkgpath
                )
                continue
            cur.execute('REPLACE INTO packages VALUES (?,?,?,?,?,?,?,?,?,?)',
                        pkginfo)
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
    if removed:
        for name in removed:
            cur.execute('DELETE FROM packages WHERE name = ?', (name,))
            cur.execute('DELETE FROM package_spec WHERE package = ?', (name,))
            cur.execute('DELETE FROM package_dependencies WHERE package = ?', (name,))
            logging.info('removed: ' + name)
    logging.info('Done.')


def main(dbfile, path, tree):
    db = sqlite3.connect(dbfile)
    cur = db.cursor()
    init_db(cur)
    scan_abbs_tree(cur, path, tree)
    db.commit()

if __name__ == '__main__':
    import sys
    sys.exit(main(*sys.argv[1:]))
