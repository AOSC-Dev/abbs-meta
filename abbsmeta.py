#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import time
import sqlite3
import logging
import argparse
import collections

import fossil
import bashvar
import reposync

logging.basicConfig(
    format='%(asctime)s %(levelname).1s %(message)s', level=logging.INFO)

re_variable = re.compile(b'^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=')
re_packagename = re.compile(r'^([a-z0-9][a-z0-9+.-]*)(.*)$')
re_commitmsg = re.compile(r'^\[?([a-z0-9][a-z0-9+. ,{}*/-]*)\]?\:? (.+)$', re.M)
re_commitrevert = re.compile(r'^(?:Revert ")+(.+?)"+$', re.M)
abbs_categories = frozenset(('core-', 'base-', 'extra-'))
repo_ignore = frozenset((
    '.git', '.githubwiki', '.abbs-repo', 'repo-spec',
    'groups', 'newpak', 'assets'
))

FileChange = collections.namedtuple('FileChange', 'status name rid islink')

def uniq(seq):  # Dave Kirby
    # Order preserving
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]

class Package:
    def __init__(self, tree, secpath, directory, name=None):
        self.name = name
        self.tree = tree
        self.secpath = secpath
        if any(secpath.startswith(x) for x in abbs_categories):
            self.category, self.section = secpath.split('-', 1)
        else:
            self.category, self.section = None, secpath
        self.directory = directory
        self.pkg_section = None
        self.version = None
        self.release = None
        self.epoch = None
        self.description = None
        self.commit_time = None
        self.spec = collections.OrderedDict()
        self.dependencies = []

    def __repr__(self):
        return "Package('%s', '%s', '%s', %r)" % (
                self.tree, self.secpath, self.directory, self.name)

    def __eq__(self, other):
        return ((self.__class__.__name__, self.tree,
                self.secpath, self.directory, self.name) ==
                (other.__class__.__name__, other.tree,
                other.secpath, other.directory, self.name))

    def load_spec(self, fp, filename=None):
        result = bashvar.read_bashvar(fp, filename)
        self.spec.update(result)
        self.version = self.spec.pop('VER', None)
        self.release = self.spec.pop('REL', None)

    def load_defines(self, fp, filename=None):
        result = bashvar.read_bashvar(fp, filename)
        self.spec.update(result)
        name = self.spec.pop('PKGNAME', None)
        if not name:
            # we assume it is a define for some specific architecture
            return
        self.name = name
        self.pkg_section = self.spec.pop('PKGSEC', None)
        self.description = self.spec.pop('PKGDES', None)
        self.epoch = self.spec.pop('PKGEPOCH', None)
        dependencies = []
        for rel in ('PKGDEP', 'PKGRECOM', 'PKGBREAK', 'PKGCONFL', 'PKGREP',
                    'BUILDDEP', 'PKGDEP_DPKG', 'PKGDEP_RPM'):
            for pkgname in self.spec.pop(rel, '').split():
                deppkg, depver = re_packagename.match(pkgname).groups()
                dependencies.append((name, deppkg, depver, rel))
        self.dependencies = uniq(dependencies)

class PackageGroup(Package):
    def __init__(self, tree, secpath, directory, name=None):
        self.name = name or directory
        self.tree = tree
        self.secpath = secpath
        if any(secpath.startswith(x) for x in abbs_categories):
            self.category, self.section = secpath.split('-', 1)
        else:
            self.category, self.section = None, secpath
        self.version = None
        self.release = None
        self.directory = directory
        self.commit_time = None
        self.spec = collections.OrderedDict()

    def __repr__(self):
        return "PackageGroup('%s', '%s', '%s', %r)" % (
                self.tree, self.secpath, self.directory, self.name)

    def __eq__(self, other):
        return ((self.__class__.__name__, self.tree,
                self.secpath, self.directory) ==
                (other.__class__.__name__, other.tree,
                other.secpath, other.directory))

    def package(self, defines_fp, defines_filename=None):
        cls = Package(self.tree, self.secpath, self.directory, self.name)
        cls.commit_time = self.commit_time
        cls.spec = self.spec.copy()
        cls.version = self.version
        cls.release = self.release
        cls.load_defines(defines_fp, defines_filename)
        return cls

def parse_commit_msg(name, text):
    if text.startswith('Merge branch '):
        return
    match = re_commitrevert.match(text)
    if match:
        text = match.group(1)
    match = re_commitmsg.match(text)
    if match:
        if name in match.group(1):
            return match.group(2)
    return text

class SourceRepo:
    def __init__(self, name, basepath, markpath, dbfile, mainbranch, branches=None,
                 category='base', url=None, priority=0):
        # tree name
        if '/' in name:
            raise ValueError("'/' not allowed in name. Use basepath to change directory")
        self.name = name
        self.basepath = basepath
        self.markpath = markpath
        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile)
        self.branches = branches
        self.mainbranch = mainbranch
        if branches and mainbranch not in branches:
            raise ValueError("mainbranch '%s' not in branches" % mainbranch)
        self.category = category
        self.url = url
        self.priority = priority
        self.gitpath = os.path.join(basepath, name + '.git')
        if not os.path.isdir(self.gitpath):
            gitpathwork = os.path.join(basepath, name)
            if os.path.isdir(gitpathwork):
                self.gitpath = gitpathwork
            else:
                raise NotADirectoryError("can't find git working tree or base repo at %s(.git)" % gitpathwork)
        self.fossilpath = os.path.join(basepath, name + '.fossil')
        # db for syncing among Fossil, Git and Abbs-meta database
        self.marksdbfile = os.path.join(markpath, name + '-marks.db')
        self.marksdb = sqlite3.connect(self.marksdbfile)
        self.db.row_factory = self.marksdb.row_factory = sqlite3.Row
        if not os.path.isfile(self.fossilpath):
            self.sync()
        self.fossil = fossil.Repo(self.fossilpath)
        self._cache_flist = fossil.LRUCache(16)

    def __repr__(self):
        return "<SourceRepo %s, basepath=%s>" % (self.name, self.basepath)

    def update(self, sync=True, reset=False):
        self.init_db()
        logging.info('Update ' + self.name)
        if sync:
            logging.info('Syncing...')
            self.sync()
        if reset:
            self.reset_progress()
        try:
            self.repo_update()
        except KeyboardInterrupt:
            logging.error('Interrupted.')
        except:
            logging.exception('Error.')
        self.close()

    def init_db(self):
        cur = self.db.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('CREATE TABLE IF NOT EXISTS trees ('
                    'name TEXT PRIMARY KEY,'
                    'category TEXT,' # base, bsp
                    'url TEXT,' # github
                    'priority INTEGER,'
                    'mainbranch TEXT'
                    ')')
        cur.execute('CREATE TABLE IF NOT EXISTS packages ('
                    'name TEXT PRIMARY KEY,'  # coreutils
                    'tree TEXT,'     # abbs tree name
                    'category TEXT,' # base
                    'section TEXT,'  # utils
                    'pkg_section TEXT,'  # (PKGSEC)
                    'directory TEXT,' # second-level dir in aosc-os-abbs
                    'description TEXT'
                    ')')
        cur.execute('CREATE TABLE IF NOT EXISTS package_duplicate ('
                    'package TEXT,'  # coreutils
                    'tree TEXT,'     # abbs tree name
                    'category TEXT,' # base
                    'section TEXT,'  # utils
                    'directory TEXT,' # second-level dir in aosc-os-abbs
                    'UNIQUE (package, tree, category, section, directory)'
                    ')')
        cur.execute('CREATE TABLE IF NOT EXISTS package_versions ('
                    'package TEXT,'  # coreutils
                    'branch TEXT,'   # abbs tree branch
                    'version TEXT,'  # 8.25
                    'release TEXT,'  # -1
                    'epoch TEXT,'    # 1:
                    'commit_time INTEGER,'
                    'PRIMARY KEY (package, branch)'
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
        cur.execute("DROP VIEW IF EXISTS v_packages")
        cur.execute("CREATE VIEW IF NOT EXISTS v_packages AS "
                    "SELECT p.name name, p.tree tree, "
                    "  t.category tree_category, "
                    "  pv.branch branch, p.category category, "
                    "  section, pkg_section, directory, description, "
                    "  ((CASE WHEN ifnull(epoch, '') = '' THEN '' "
                    "    ELSE epoch || ':' END) || version || "
                    "   (CASE WHEN ifnull(release, '') = '' THEN '' "
                    "    ELSE '-' || release END)) full_version, "
                    "  pv.commit_time commit_time, t.category tree_category "
                    "FROM packages p "
                    "LEFT JOIN trees t ON t.name=p.tree "
                    "LEFT JOIN package_versions pv "
                    "  ON pv.package=p.name AND pv.branch=t.mainbranch")
        cur.execute('CREATE INDEX IF NOT EXISTS idx_package_duplicate'
                    ' ON package_duplicate (package)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_package_versions'
                    ' ON package_versions (package, branch)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_package_spec'
                    ' ON package_spec (package)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_package_dependencies'
                    ' ON package_dependencies (package)')
        cur.execute('REPLACE INTO trees VALUES (?,?,?,?,?)', (self.name,
                    self.category, self.url, self.priority, self.mainbranch))
        mcur = self.marksdb.cursor()
        mcur.execute('PRAGMA journal_mode=WAL')
        mcur.execute('CREATE TABLE IF NOT EXISTS package_rel ('
                    'rid INTEGER, package TEXT, '
                    'version TEXT, release TEXT, epoch TEXT, '
                    'message TEXT, '
                    'PRIMARY KEY (rid, package)'
                    ')')
        self.db.commit()
        self.marksdb.commit()

    def sync(self):
        reposync.sync(self.gitpath, self.fossilpath, self.markpath)

    def file_list(self, mid):
        try:
            return self._cache_flist[mid]
        except KeyError:
            self._cache_flist[mid] = flist = collections.OrderedDict((
                (row[0], (row[1], row[2] if len(row) > 2 else ''))
                for row in self.fossil.manifest(mid).F
            ))
            return flist

    def getfile(self, mid, path, text=False):
        uuid = self.file_list(mid)[path][0]
        blob = self.fossil.file(uuid).blob
        if text:
            return uuid, io.StringIO(blob.decode('utf-8'))
        else:
            return uuid, io.BytesIO(blob)

    def file_change(self, mid, full=False):
        ret = []
        if full:
            for fn, v in self.file_list(mid).items():
                ret.append(FileChange('+', fn,
                    self.fossil.to_rid(v[0]), ('l' in v[1])))
            return ret
        for fid, pid, fn, pfn, mperm in self.fossil.execute(
            'SELECT mlink.fid, mlink.pid, fn.name fn, pfn.name pfn, mlink.mperm '
            'FROM mlink '
            'LEFT JOIN filename fn ON fn.fnid=mlink.fnid '
            'LEFT JOIN filename pfn ON pfn.fnid=mlink.pfnid '
            'WHERE mid = ?', (mid,)):
            if fid == 0:
                # deleted
                ret.append(FileChange('-', fn, pid, mperm == 2))
            elif pfn:
                # renamed
                ret.append(FileChange('-', pfn, pid, mperm == 2))
                ret.append(FileChange('+', fn, fid, mperm == 2))
            else:
                # added if pid == 0 else changed
                ret.append(FileChange('+', fn, fid, mperm == 2))
        return ret

    def file_mtime(self, mid, path):
        return int(self.fossil.execute(
                'SELECT round((mtime-2440587.5)*86400) FROM event '
                'LEFT JOIN mlink ON mlink.mid = event.objid '
                'WHERE mlink.fid = (SELECT rid FROM blob WHERE uuid = ?) '
                'ORDER BY mtime ASC '
                'LIMIT 1', (self.file_list(mid)[path][0],)).fetchone()[0])

    def exists(self, mid, path, isdir=False, ignorelink=False):
        for fn, v in self.file_list(mid).items():
            if fn == path:
                if ignorelink and 'l' in v[1]:
                    return False
                else:
                    return True
            elif isdir and fn.startswith(path + '/'):
                return True
        return False

    def branches_of_commit(self, mid):
        mcur = self.marksdb.cursor()
        results = frozenset(x[0] for x in mcur.execute(
            "SELECT tagname FROM branches WHERE rid=?", (mid,)).fetchall())
        if not self.branches:
            return list(results)
        branches = [b for b in self.branches if b in results]
        return branches

    def list_update(self, mid, full=False):
        pkgs = set()
        diff = self.file_change(mid, full)
        for change in diff:
            pathspl = change.name.split('/', 2)
            if not len(pathspl) > 2:
                continue
            path, pkgpath = pathspl[:2]
            changestatus = change.status
            if path in repo_ignore or (path, pkgpath) in pkgs:
                continue
            elif (change.status == '+' and not self.exists(
                mid, os.path.join(path, pkgpath, 'spec'), ignorelink=True)):
                continue
            elif (change.status == '-' and self.exists(
                mid, os.path.join(path, pkgpath, 'spec'), ignorelink=True)):
                changestatus = '+'
            yield PackageGroup(self.name, path, pkgpath), changestatus
            pkgs.add((path, pkgpath))

    def read_package_info(self, mid, pkggroup):
        results = []
        repopath = os.path.join(pkggroup.secpath, pkggroup.directory)
        logging.debug('read %r', pkggroup)
        filelist = self.file_list(mid)
        uuid, specstr = self.getfile(mid, os.path.join(repopath, 'spec'), True)
        pkggroup.load_spec(specstr, uuid[:16])
        pkggroup.commit_time = self.file_mtime(mid, os.path.join(repopath, 'spec'))
        for path, fattr in filelist.items():
            if path.startswith(repopath + '/'):
                dirpath, filename = os.path.split(path)
                if filename != 'defines':
                    continue
                uuid, defines = self.getfile(
                    mid, os.path.join(dirpath, 'defines'), True)
                pkg = pkggroup.package(defines, uuid[:16])
                results.append(pkg)
        return results

    def update_package(self, mid, pkg):
        cur = self.db.cursor()
        existing = cur.execute(
            'SELECT tree, category, section, directory '
            'FROM packages WHERE name=?', (pkg.name,)).fetchone()
        if not existing:
            pass
        elif existing[0] != self.name:
            logging.warning(
                'duplicate package "%s" found in different trees '
                '%s/%s-%s/%s and %s/%s-%s/%s', pkg.name,
                existing[0], existing[1], existing[2], existing[3],
                self.name, pkg.category, pkg.section, pkg.directory
            )
            cur.execute(
                'INSERT OR IGNORE INTO package_duplicate VALUES (?,?,?,?,?)',
                (pkg.name, self.name, pkg.category or '', pkg.section, pkg.directory)
            )
            cur.execute(
                'INSERT OR IGNORE INTO package_duplicate VALUES (?,?,?,?,?)',
                (pkg.name, existing[0], existing[1] or '', existing[2], existing[3])
            )
            # trees with lower priority will not override
            return
        elif ((pkg.category, pkg.section, pkg.directory) !=
              tuple(existing[1:])):
            logging.warning(
                'duplicate package "%s" found in %s-%s/%s and %s-%s/%s',
                pkg.name, existing[1], existing[2], existing[3],
                pkg.category, pkg.section, pkg.directory
            )
            cur.execute(
                'INSERT OR IGNORE INTO package_duplicate VALUES (?,?,?,?,?)',
                (pkg.name, self.name, pkg.category or '', pkg.section, pkg.directory)
            )
            cur.execute(
                'INSERT OR IGNORE INTO package_duplicate VALUES (?,?,?,?,?)',
                (pkg.name, existing[0], existing[1] or '', existing[2], existing[3])
            )
        cur.execute(
            'REPLACE INTO packages VALUES (?,?,?,?,?,?,?)',
            (pkg.name, self.name, pkg.category, pkg.section,
            pkg.pkg_section, pkg.directory, pkg.description)
        )
        for branch in self.branches_of_commit(mid):
            cur.execute(
                'REPLACE INTO package_versions VALUES (?,?,?,?,?,?)',
                (pkg.name, branch, pkg.version, pkg.release,
                pkg.epoch, pkg.commit_time)
            )
            if branch == self.mainbranch:
                cur.execute('DELETE FROM package_spec WHERE package = ?', (pkg.name,))
                for k, v in pkg.spec.items():
                    cur.execute('REPLACE INTO package_spec VALUES (?,?,?)',
                                (pkg.name, k, v))
                cur.execute('DELETE FROM package_dependencies WHERE package = ?',
                            (pkg.name,))
                cur.executemany('REPLACE INTO package_dependencies VALUES (?,?,?,?)',
                                pkg.dependencies)
        logging.debug('add: ' + pkg.name)

    def scan_abbs_tree(self, mid):
        cur = self.db.cursor()
        mcur = self.marksdb.cursor()
        exist = mcur.execute(
            'SELECT 1 FROM package_rel WHERE rid = ?', (mid,)).fetchone()
        if exist:
            return
        for pkggroup, change in self.list_update(mid):
            for row in cur.execute(
                'SELECT name FROM packages p '
                'WHERE category IS ? AND section=? AND directory=? AND tree=?',
                (pkggroup.category, pkggroup.section, pkggroup.directory,
                self.name)).fetchall():
                name = row[0]
                for branch in self.branches_of_commit(mid):
                    cur.execute('DELETE FROM package_versions WHERE '
                                'package=? AND branch=?', (name, branch))
                if not cur.execute(
                    'SELECT 1 FROM package_versions WHERE package=?',
                    (name,)).fetchone():
                    cur.execute('DELETE FROM package_duplicate '
                                'WHERE package=? AND tree=? AND category=? '
                                ' AND section=? AND directory=?',
                                (name, self.name, pkggroup.category or '',
                                pkggroup.section, pkggroup.directory))
                    cur.execute('DELETE FROM package_spec WHERE package=?',
                                (name,))
                    cur.execute('DELETE FROM package_dependencies WHERE package=?',
                                (name,))
                    cur.execute('DELETE FROM packages WHERE name=?', (name,))
                    if change == '-':
                        logging.info('removed: ' + name)
                    else:
                        logging.debug('rm+: ' + name)
            cur.execute(
                'DELETE FROM package_duplicate '
                'WHERE category=? AND section=? AND directory=? AND tree=?',
                (pkggroup.category or '', pkggroup.section,
                 pkggroup.directory, self.name)
            )
            if change == '+':
                for pkg in self.read_package_info(mid, pkggroup):
                    self.update_package(mid, pkg)
                    cmsg = self.fossil.execute(
                        'SELECT comment FROM event WHERE objid=?', (mid,)).fetchone()
                    cmsg = parse_commit_msg(pkg.name, cmsg[0]) if cmsg else None
                    if not cmsg:
                        continue
                    mcur.execute(
                        'REPLACE INTO package_rel VALUES (?,?,?,?,?,?)',
                        (mid, pkg.name, pkg.version, pkg.release, pkg.epoch, cmsg)
                    )
        # make up for the deleted duplicate
        for secpath, directory in cur.execute(
            "SELECT "
            " CASE WHEN category='' THEN section "
            " ELSE category || '-' || section END, directory "
            "FROM package_duplicate "
            "WHERE package NOT IN (SELECT name FROM packages) AND tree=?",
            (self.name,)).fetchall():
            if not self.exists(mid, os.path.join(secpath, directory, 'spec'), True):
                continue
            pkggroup = PackageGroup(self.name, secpath, directory)
            for pkg in self.read_package_info(mid, pkggroup):
                self.update_package(mid, pkg)
        cur.execute(
            'DELETE FROM package_duplicate WHERE package IN '
            '(SELECT package FROM package_duplicate '
            ' GROUP BY package HAVING count(package) = 1)'
        )

    def repo_update(self):
        cur = self.db.cursor()
        mcur = self.marksdb.cursor()
        last_update = cur.execute(
            'SELECT pv.commit_time FROM packages p '
            'INNER JOIN package_versions pv ON pv.package=p.name '
            'WHERE p.tree=? '
            'ORDER BY pv.commit_time DESC LIMIT 1', (self.name,)).fetchone()
        last_update = last_update[0] if last_update else 0
        last_rid = mcur.execute(
            'SELECT rid FROM package_rel ORDER BY rid DESC LIMIT 1').fetchone()
        last_rid = last_rid[0] if last_rid else 0
        for mtime, mid, uuid in self.fossil.execute(
            "SELECT round((mtime-2440587.5)*86400), objid, blob.uuid "
            "FROM event "
            "LEFT JOIN blob ON blob.rid=event.objid "
            "WHERE (mtime>=? OR objid>?) AND type='ci' ORDER BY mtime, objid",
            (fossil.unix_to_julian(last_update), last_rid)).fetchall():
            if not self.branches_of_commit(mid):
                continue
            logging.info('%s: %d %s', time.strftime('%Y-%m-%d', time.gmtime(mtime)), mid, uuid[:16])
            self.scan_abbs_tree(mid)
        logging.info('Done.')

    def reset_progress(self):
        cur = self.db.cursor()
        cur.execute('DELETE FROM package_versions WHERE package IN '
                    '(SELECT name FROM packages WHERE tree=?)', (self.name,))
        cur.execute('DELETE FROM package_duplicate WHERE tree=?', (self.name,))
        cur.execute('VACUUM')
        mcur = self.marksdb.cursor()
        mcur.execute('DELETE FROM package_rel')
        mcur.execute('VACUUM')
        self.db.commit()
        self.marksdb.commit()

    def close(self):
        logging.info('Committing...')
        self.db.execute('PRAGMA optimize')
        self.marksdb.execute('PRAGMA optimize')
        self.db.commit()
        self.marksdb.commit()

def main():
    parser = argparse.ArgumentParser(description="Generate metadata database for abbs trees.")
    parser.add_argument("-p", "--basepath", help="Directory with both Git and Fossil repositories", default=".", metavar='PATH')
    parser.add_argument("-m", "--markpath", help="Directory with Git and Fossil sync marks", default=".", metavar='PATH')
    parser.add_argument("-d", "--dbfile", help="Abbs meta database file", default="abbs.db", metavar='FILE')
    parser.add_argument("-b", "--branches", help="Branches to consider, seperated by comma (,)", default="staging,master,bugfix")
    parser.add_argument("-B", "--mainbranch", help="Git repo main branch name", default="master", metavar='BRANCH')
    parser.add_argument("-c", "--category", help="Category, 'base' or 'bsp'", default="base")
    parser.add_argument("-u", "--url", help="Repo url")
    parser.add_argument("-P", "--priority", help="Priority to consider", type=int, default=0)
    parser.add_argument("-v", "--verbose", help="Show debug logs", action='store_true')
    parser.add_argument("--no-sync", help="Don't sync Git and Fossil repos", action='store_true')
    parser.add_argument("--reset", help="Reset sync status", action='store_true')
    parser.add_argument("name", help="Repository / abbs tree name")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    repo = SourceRepo(
        args.name, args.basepath, args.markpath, args.dbfile,
        args.mainbranch, args.branches.split(','),
        args.category, args.url, args.priority)
    repo.update(not args.no_sync, args.reset)

if __name__ == '__main__':
    main()
