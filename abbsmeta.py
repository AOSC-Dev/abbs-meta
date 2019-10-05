#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import time
import stat
import sqlite3
import logging
import argparse
import posixpath
import collections

import bashvar

logging.basicConfig(
    format='%(asctime)s %(levelname).1s %(message)s', level=logging.INFO)

re_variable = re.compile(b'^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=')
re_packagerel = re.compile(
    r'^([a-z0-9][a-z0-9+.-]*)([<>=]=)?([0-9A-Za-z.+~:-]*)$')
re_commitmsg = re.compile(r'^\[?([a-z0-9][a-z0-9+. ,{}*/-]*)\]?\:? (.+)$', re.M)
re_commitrevert = re.compile(r'^(?:Revert ")+(.+?)"+$', re.M)
abbs_categories = frozenset(('core-', 'base-', 'extra-'))
repo_ignore = frozenset((
    '.git', '.github', '.githubwiki', '.abbs-repo', 'repo-spec',
    'groups', 'newpak', 'assets'
))
relvars = ('PKGDEP', 'PKGRECOM', 'PKGBREAK', 'PKGCONFL', 'PKGREP',
           'PKGPROV', 'PKGSUG', 'BUILDDEP', 'PKGDEP_DPKG', 'PKGDEP_RPM')
re_relvars = re.compile(r'^(%s)(__\w+)?$' % '|'.join(relvars), re.ASCII)

Version = collections.namedtuple('Version', ('version', 'release', 'epoch'))
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
        self.vermask_arch = collections.defaultdict(
            lambda: Version(None, None, None))
        self.description = None
        self.spec = collections.OrderedDict()
        self.dependencies = []
        self.fn_spec = None
        self.fn_defines = None
        self.err_spec = None
        self.err_defines = None

    def __repr__(self):
        return "Package('%s', '%s', '%s', %r)" % (
                self.tree, self.secpath, self.directory, self.name)

    def __eq__(self, other):
        return ((self.__class__.__name__, self.tree,
                self.secpath, self.directory, self.name) ==
                (other.__class__.__name__, other.tree,
                other.secpath, other.directory, self.name))

    def load_spec(self, fp, filename=None, fileid=None):
        self.fn_spec = filename
        result, self.err_spec = bashvar.read_bashvar(fp, fileid, True)
        self.spec.update(result)
        for key in tuple(self.spec.keys()):
            if key == 'VER':
                self.version = self.spec.pop(key)
            elif key == 'REL':
                self.release = self.spec.pop(key)
            elif key.startswith('VER__'):
                arch = key[5:].lower()
                self.vermask_arch[arch] = self.vermask_arch[arch]._replace(
                    version=self.spec.pop(key))
            elif key.startswith('REL__'):
                arch = key[5:].lower()
                self.vermask_arch[arch] = self.vermask_arch[arch]._replace(
                    release=self.spec.pop(key))

    def load_defines(self, fp, filename=None, fileid=None):
        self.fn_defines = filename
        result, self.err_defines = bashvar.read_bashvar(fp, fileid, True)
        self.spec.update(result)
        name = self.spec.pop('PKGNAME', None)
        if not name:
            # we assume it is a define for some specific architecture
            return
        self.name = name
        for key in tuple(self.spec.keys()):
            if key == 'PKGSEC':
                self.pkg_section = self.spec.pop(key)
            elif key == 'PKGDES':
                self.description = self.spec.pop(key)
            elif key == 'PKGEPOCH':
                self.epoch = self.spec.pop(key)
            elif key.startswith('PKGEPOCH__'):
                arch = key[10:].lower()
                self.vermask_arch[arch] = self.vermask_arch[arch]._replace(
                    epoch=self.spec.pop(key))
        dependencies = []
        relerrs = [self.err_defines] if self.err_defines else []
        for k, relvalue in tuple(self.spec.items()):
            if not re_relvars.match(k):
                continue
            relsp = k.rsplit('__', 1)
            rel = relsp[0]
            arch = '' if len(relsp) == 1 else relsp[1].lower()
            for pkgname in relvalue.split():
                match = re_packagerel.match(pkgname)
                if not match:
                    logging.warning('invalid dependency definition in %s/%s: "%s"' % (
                        name, rel, pkgname))
                    relerrs.append('%s: invalid dependency definition in "%s"' % (
                        rel, pkgname))
                    continue
                deppkg, relop, depver = match.groups()
                dependencies.append((name, deppkg, relop, depver or None, arch, rel))
            del self.spec[k]
        self.dependencies = uniq(dependencies)
        if relerrs:
            self.err_defines = '\n'.join(relerrs)

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
        self.vermask_arch = collections.defaultdict(
            lambda: Version(None, None, None))
        self.directory = directory
        self.spec = collections.OrderedDict()
        self.fn_spec = None
        self.err_spec = None

    def __repr__(self):
        return "PackageGroup('%s', '%s', '%s', %r)" % (
                self.tree, self.secpath, self.directory, self.name)

    def __eq__(self, other):
        return ((self.__class__.__name__, self.tree,
                self.secpath, self.directory) ==
                (other.__class__.__name__, other.tree,
                other.secpath, other.directory))

    def package(self, defines_fp, defines_filename=None, defines_fileid=None):
        cls = Package(self.tree, self.secpath, self.directory, self.name)
        cls.spec = self.spec.copy()
        cls.version = self.version
        cls.release = self.release
        cls.vermask_arch = self.vermask_arch.copy()
        cls.fn_spec = self.fn_spec
        cls.err_spec = self.err_spec
        cls.load_defines(defines_fp, defines_filename, defines_fileid)
        return cls

def parse_commit_msg(name, text):
    if text is None:
        return
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


class LocalRepo:
    def __init__(self, path, dbfile, name=None, branch='_local',
                 category='base', priority=0):
        self.path = path
        self.dbfile = dbfile
        # tree name
        self.name = name or os.path.basename(path.rstrip('/'))
        if '/' in self.name:
            raise ValueError("'/' not allowed in name. Use basepath to change directory")
        self.db = sqlite3.connect(dbfile)
        self.branch = branch
        self.mainbranch = branch
        self.category = category
        self.priority = priority
        self.db.row_factory = sqlite3.Row

    def __repr__(self):
        return "<LocalRepo %s, path=%s>" % (self.name, self.path)

    def update(self, reset=False):
        self.init_db()
        logging.info('Update ' + self.name)
        if reset:
            self.reset_progress()
        try:
            self.repo_update()
        except KeyboardInterrupt:
            logging.error('Interrupted.')
        except:
            logging.exception('Error.')
        self.close()

    def init_db_schema(self):
        cur = self.db.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('CREATE TABLE IF NOT EXISTS trees ('
                    'tid INTEGER PRIMARY KEY,' # also priority
                    'name TEXT UNIQUE,'
                    'category TEXT,' # base, bsp
                    'url TEXT,' # github
                    'mainbranch TEXT'
                    ')')
        cur.execute('CREATE TABLE IF NOT EXISTS tree_branches ('
                    'name TEXT,'
                    'tree TEXT,'
                    'branch TEXT,'
                    'priority INTEGER,'
                    'PRIMARY KEY (name),'
                    'UNIQUE (tree, branch)'
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
                    'architecture TEXT,'   # abbs tree branch
                    'version TEXT,'  # 8.25
                    'release TEXT,'  # -1
                    'epoch TEXT,'    # 1:
                    'commit_time INTEGER,'
                    'committer TEXT,'
                    'githash TEXT,'
                    'PRIMARY KEY (package, branch, architecture)'
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
                    'relop TEXT,'
                    'version TEXT,'
                    'architecture TEXT,'
                    # PKGDEP, PKGRECOM, PKGBREAK, PKGCONFL, PKGREP,
                    # PKGPROV, PKGSUG, BUILDDEP
                    'relationship TEXT,'
                    'PRIMARY KEY (package, dependency, architecture, relationship),'
                    'FOREIGN KEY(package) REFERENCES packages(name)'
                    # we may have unmatched dependency package name
                    # 'FOREIGN KEY(dependency) REFERENCES packages(name)'
                    ')')
        #cur.execute("DROP VIEW IF EXISTS v_packages")
        cur.execute("CREATE VIEW IF NOT EXISTS v_packages AS "
                    "SELECT p.name name, p.tree tree, "
                    "  t.category tree_category, "
                    "  pv.branch branch, p.category category, "
                    "  section, pkg_section, directory, description, version, "
                    "  ((CASE WHEN ifnull(epoch, '') = '' THEN '' "
                    "    ELSE epoch || ':' END) || version || "
                    "   (CASE WHEN ifnull(release, '') IN ('', '0') THEN '' "
                    "    ELSE '-' || release END)) full_version, "
                    "  pv.commit_time commit_time, pv.committer committer "
                    "FROM packages p "
                    "INNER JOIN trees t ON t.name=p.tree "
                    "LEFT JOIN package_versions pv "
                    "  ON pv.package=p.name AND pv.branch=t.mainbranch")
        cur.execute('CREATE INDEX IF NOT EXISTS idx_packages_directory'
                    ' ON packages (directory)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_package_dependencies_rev'
                    ' ON package_dependencies (dependency)')
        cur.execute('CREATE VIRTUAL TABLE IF NOT EXISTS fts_packages'
                    ' USING fts5(name, description, tokenize = porter)')
        self.db.commit()

    def init_db(self):
        self.init_db_schema()
        cur = self.db.cursor()
        cur.execute('REPLACE INTO trees VALUES (?,?,?,?,?)', (self.priority,
            self.name, self.category, None, self.branch))
        cur.execute('REPLACE INTO tree_branches VALUES (?,?,?,?)', (
                    self.name + '/' + self.branch, self.name, self.branch, 0))
        self.db.commit()

    def update_package(self, branches, pkg):
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
        res = cur.execute(
            'SELECT description FROM fts_packages WHERE name=?',
            (pkg.name,)).fetchone()
        if res is None:
            cur.execute(
                'INSERT INTO fts_packages VALUES (?, ?)',
                (pkg.name, pkg.description)
            )
        elif res[0] != pkg.description:
            cur.execute(
                'UPDATE fts_packages SET description=? WHERE name=?',
                (pkg.description, pkg.name)
            )
        for branch in branches:
            cur.execute(
                'REPLACE INTO package_versions VALUES (?,?,?,?,?,?,?,?,?)',
                (pkg.name, branch, '', pkg.version, pkg.release,
                pkg.epoch, None, None, None)
            )
            for arch, mask in pkg.vermask_arch.items():
                cur.execute(
                    'REPLACE INTO package_versions VALUES (?,?,?,?,?,?,?,?,?)', (
                    pkg.name, branch, arch,
                    mask.version or pkg.version, mask.release or pkg.release,
                    mask.epoch or pkg.epoch, None, None, None
                ))
            if branch == self.mainbranch:
                cur.execute('DELETE FROM package_spec WHERE package = ?', (pkg.name,))
                for k, v in pkg.spec.items():
                    cur.execute('REPLACE INTO package_spec VALUES (?,?,?)',
                                (pkg.name, k, v))
                cur.execute('DELETE FROM package_dependencies WHERE package = ?',
                            (pkg.name,))
                cur.executemany(
                    'REPLACE INTO package_dependencies VALUES (?,?,?,?,?,?)',
                    pkg.dependencies)
        logging.debug('add: ' + pkg.name)

    def read_package_info(self, pkggroup):
        results = []
        repopath = os.path.join(pkggroup.secpath, pkggroup.directory)
        logging.debug('read %r', pkggroup)
        specfn = os.path.join(repopath, 'spec')
        with open(os.path.join(self.path, specfn), 'r', encoding='utf-8') as f:
            pkggroup.load_spec(f, specfn)
        for root, dirs, files in os.walk(os.path.join(self.path, repopath)):
            for filename in files:
                if filename != 'defines':
                    continue
                definesfn = os.path.join(root, 'defines')
                definesfn_rel = os.path.relpath(definesfn, self.path)
                with open(definesfn, 'r', encoding='utf-8') as f:
                    pkg = pkggroup.package(f, definesfn_rel)
                results.append(pkg)
        return results

    def scan_abbs_tree(self):
        dir_mtime = {}
        for root, dirs, files in os.walk(self.path):
            pathspl = os.path.relpath(root, self.path).split(os.sep)
            if not len(pathspl) >= 2:
                continue
            if pathspl[0] in repo_ignore:
                continue
            pkgpath = '/'.join(pathspl[:2])
            for filename in files:
                fullname = os.path.join(root, filename)
                fstat = os.lstat(fullname)
                if not stat.S_ISREG(fstat.st_mode):
                    continue
                dir_mtime[pkgpath] = max(
                    dir_mtime.get(pkgpath, 0), int(fstat.st_mtime))
        cur = self.db.cursor()
        cur.execute("CREATE TEMP TABLE t_localdirs ("
            "fullpath TEXT PRIMARY KEY, mtime INTEGER)")
        cur.executemany("INSERT INTO t_localdirs VALUES (?,?)",
            dir_mtime.items())
        self.db.commit()
        # one directory -> multiple packages
        cur.execute("""
            CREATE TEMP TABLE t_lastdirs AS
            SELECT (CASE WHEN ifnull(p.category, '')='' THEN ''
              ELSE p.category || '-' END) || p.section || '/' ||
              p.directory fullpath, p.name,
              ifnull(p.category, '') category, p.section, p.directory,
              v.commit_time mtime
            FROM package_versions v
            INNER JOIN packages p ON p.name=v.package
            WHERE p.tree=? AND v.branch=?""", (self.name, self.branch)
        )
        cur.execute("CREATE INDEX idx_t_lastdirs ON t_lastdirs (fullpath)")
        cur.execute("""
            CREATE TEMP TABLE t_pkgrm AS
            SELECT a.name, (b.fullpath IS NULL) isdel
            FROM t_lastdirs a
            LEFT JOIN t_localdirs b USING (fullpath)
            WHERE b.fullpath IS NULL OR a.mtime IS NULL OR b.mtime > a.mtime
        """)
        for name, isdel in cur.execute("SELECT name, isdel FROM t_pkgrm"):
            if isdel:
                logging.info('removed: ' + name)
            else:
                logging.debug('rm+: ' + name)
        cur.execute('DELETE FROM package_duplicate '
                    'WHERE tree=? AND (category,section,directory) IN '
                    ' (SELECT category, section, directory FROM t_pkgrm)',
                    (self.name,))
        cur.execute(
            "DELETE FROM package_versions WHERE branch=? "
            "AND package IN (SELECT name FROM t_pkgrm)", (self.branch,))
        cur.execute(
            "DELETE FROM package_spec "
            "WHERE package IN (SELECT name FROM t_pkgrm)")
        cur.execute(
            "DELETE FROM package_dependencies "
            "WHERE package IN (SELECT name FROM t_pkgrm)")
        cur.execute(
            "DELETE FROM packages WHERE name IN (SELECT name FROM t_pkgrm)")
        cur.execute(
            "DELETE FROM fts_packages WHERE name IN (SELECT name FROM t_pkgrm)")
        self.db.commit()
        cur.execute("""
            SELECT b.fullpath, b.mtime
            FROM t_localdirs b
            LEFT JOIN t_lastdirs a USING (fullpath)
            WHERE a.fullpath IS NULL OR a.mtime IS NULL OR b.mtime > a.mtime
        """)
        for fullpath, mtime in cur.fetchall():
            path, pkgpath = fullpath.split('/')
            pkggroup = PackageGroup(self.name, path, pkgpath)
            for pkg in self.read_package_info(pkggroup):
                self.update_package((self.branch,), pkg)
                cur.execute(
                    'UPDATE package_versions SET commit_time=? '
                    'WHERE package=? AND branch=?',
                    (mtime, pkg.name, self.branch)
                )
        cur.execute(
            'DELETE FROM package_duplicate WHERE package IN '
            '(SELECT package FROM package_duplicate '
            ' GROUP BY package HAVING count(package) = 1)'
        )
        self.db.commit()

    def repo_update(self):
        self.scan_abbs_tree()
        logging.info('Done.')

    def reset_progress(self):
        cur = self.db.cursor()
        cur.execute('DELETE FROM package_versions WHERE package IN '
                    '(SELECT name FROM packages WHERE tree=?)', (self.name,))
        cur.execute('DELETE FROM package_duplicate WHERE tree=?', (self.name,))
        self.db.commit()
        cur.execute('VACUUM')
        self.db.commit()

    def close(self):
        if self.db.in_transaction:
            logging.info('Committing...')
            self.db.commit()
        self.db.close()

class SourceRepo(LocalRepo):
    def __init__(self, name, basepath, markpath, dbfile, mainbranch,
                 branches=None, category='base', url=None, priority=0):
        import fossil
        import reposync

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
        self._cache_branch = fossil.LRUCache(16)
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
        self.init_db_schema()
        cur = self.db.cursor()
        cur.execute('REPLACE INTO trees VALUES (?,?,?,?,?)', (self.priority,
                    self.name, self.category, self.url, self.mainbranch))
        for k, branch in enumerate(self.branches):
            cur.execute('REPLACE INTO tree_branches VALUES (?,?,?,?)', (
                        self.name + '/' + branch, self.name, branch, k))
        mcur = self.marksdb.cursor()
        mcur.execute('PRAGMA journal_mode=WAL')
        mcur.execute('CREATE TABLE IF NOT EXISTS package_rel ('
                     'rid INTEGER, package TEXT, '
                     'version TEXT, release TEXT, epoch TEXT, '
                     'message TEXT, '
                     'PRIMARY KEY (rid, package)'
                     ')')
        mcur.execute('CREATE TABLE IF NOT EXISTS package_basherr ('
                     'rid INTEGER, filename TEXT, '
                     'category TEXT, section TEXT, directory TEXT, '
                     'package TEXT, err TEXT, '
                     'PRIMARY KEY (rid, filename)'
                     ')')
        mcur.execute('CREATE INDEX IF NOT EXISTS idx_package_rel'
                     ' ON package_rel (package)')
        self.db.commit()
        self.marksdb.commit()

    def sync(self):
        reposync.sync(self.gitpath, self.fossilpath, self.markpath,
                      trackbranches=self.branches)

    def file_list(self, mid):
        if mid in self._cache_flist:
            return self._cache_flist[mid]
        else:
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
        if mid in self._cache_branch:
            return self._cache_branch[mid]
        mcur = self.marksdb.cursor()
        results = frozenset(x[0] for x in mcur.execute(
            "SELECT tagname FROM branches WHERE rid=?", (mid,)).fetchall())
        if not self.branches:
            branches = list(results)
        else:
            branches = [b for b in self.branches if b in results]
        self._cache_branch[mid] = branches
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
                mid, posixpath.join(path, pkgpath, 'spec'), ignorelink=True)):
                continue
            elif (change.status == '-' and self.exists(
                mid, posixpath.join(path, pkgpath, 'spec'), ignorelink=True)):
                changestatus = '+'
            yield PackageGroup(self.name, path, pkgpath), changestatus
            pkgs.add((path, pkgpath))

    def read_package_info(self, mid, pkggroup):
        results = []
        repopath = posixpath.join(pkggroup.secpath, pkggroup.directory)
        logging.debug('read %r', pkggroup)
        filelist = self.file_list(mid)
        specfn = posixpath.join(repopath, 'spec')
        uuid, specstr = self.getfile(mid, specfn, True)
        pkggroup.load_spec(specstr, specfn, uuid[:16])
        for path, fattr in filelist.items():
            if path.startswith(repopath + '/'):
                dirpath, filename = os.path.split(path)
                if filename != 'defines':
                    continue
                definesfn = posixpath.join(dirpath, 'defines')
                uuid, defines = self.getfile(mid, definesfn, True)
                pkg = pkggroup.package(defines, definesfn, uuid[:16])
                results.append(pkg)
        return results

    def scan_abbs_tree(self, mid):
        cur = self.db.cursor()
        mcur = self.marksdb.cursor()
        githash, exist = mcur.execute(
            "SELECT m.githash, r.rid FROM marks m "
            "LEFT JOIN package_rel r USING (rid) "
            "WHERE rid = ?", (mid,)).fetchone()
        if exist:
            return
        commitmsg = self.fossil.execute(
            'SELECT comment FROM event WHERE objid=?', (mid,)).fetchone()
        if commitmsg:
            commitmsg = commitmsg[0]
        for pkggroup, change in self.list_update(mid):
            removedpkgs = []
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
                    cur.execute('DELETE FROM fts_packages WHERE name=?',
                                (name,))
                    if change == '-':
                        logging.info('removed: ' + name)
                    else:
                        logging.debug('rm+: ' + name)
                if change == '-':
                    removedpkgs.append(name)
            cur.execute(
                'DELETE FROM package_duplicate '
                'WHERE category=? AND section=? AND directory=? AND tree=?',
                (pkggroup.category or '', pkggroup.section,
                 pkggroup.directory, self.name)
            )
            if change == '+':
                for pkg in self.read_package_info(mid, pkggroup):
                    self.update_package(self.branches_of_commit(mid), pkg)
                    cmsg = parse_commit_msg(pkg.name, commitmsg)
                    if not cmsg:
                        continue
                    mcur.execute(
                        'REPLACE INTO package_rel VALUES (?,?,?,?,?,?)',
                        (mid, pkg.name, pkg.version, pkg.release, pkg.epoch, cmsg)
                    )
                    if pkg.err_defines:
                        mcur.execute(
                            'REPLACE INTO package_basherr VALUES (?,?,?,?,?,?,?)',
                            (mid, pkg.fn_defines, pkggroup.category or '',
                            pkggroup.section, pkggroup.directory, pkg.name,
                            pkg.err_defines)
                        )
                if pkggroup.err_spec:
                    mcur.execute(
                        'REPLACE INTO package_basherr VALUES (?,?,?,?,?,?,?)',
                        (mid, pkggroup.fn_spec, pkggroup.category or '',
                        pkggroup.section, pkggroup.directory, None,
                        pkggroup.err_spec)
                    )
            else:
                for name in removedpkgs:
                    cmsg = parse_commit_msg(name, commitmsg)
                    mcur.execute(
                        'REPLACE INTO package_rel VALUES (?,?,?,?,?,?)',
                        (mid, name, None, None, None, cmsg)
                    )
        # make up for the deleted duplicate
        for secpath, directory in cur.execute(
            "SELECT "
            " CASE WHEN category='' THEN section "
            " ELSE category || '-' || section END, directory "
            "FROM package_duplicate "
            "WHERE package NOT IN (SELECT name FROM packages) AND tree=?",
            (self.name,)).fetchall():
            if not self.exists(
                mid, posixpath.join(secpath, directory, 'spec'), True):
                continue
            pkggroup = PackageGroup(self.name, secpath, directory)
            for pkg in self.read_package_info(mid, pkggroup):
                self.update_package(self.branches_of_commit(mid), pkg)
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
        mcur.execute('PRAGMA optimize')
        mcur.close()
        self.marksdb.commit()
        self.marksdb.close()
        logging.info('Updating branches...')
        cur.execute("ATTACH ? AS marks", (self.marksdbfile,))
        cur.execute("ATTACH ? AS fossil", (self.fossilpath,))
        cur.execute('PRAGMA temp_store=MEMORY')
        cur.execute('''
            CREATE TEMP TABLE t_package_versions AS
            SELECT pr.package, b.tagname branch,
              COALESCE(v.architecture, '') architecture,
              pr.version, pr.release, pr.epoch,
              CAST(round((max(e.mtime)-2440587.5)*86400) AS INTEGER) commit_time,
              c.name || ' <' || c.email || '>' committer, m.githash
            FROM marks.package_rel pr
            INNER JOIN marks.marks m USING (rid)
            INNER JOIN marks.branches b USING (rid)
            INNER JOIN main.tree_branches mb ON mb.branch=b.tagname
            INNER JOIN fossil.event e ON e.objid=pr.rid
            INNER JOIN marks.committers c ON e.user=c.email
            LEFT JOIN package_versions v
            ON pr.package=v.package AND b.tagname=v.branch
            WHERE mb.tree=?
            GROUP BY pr.package, b.tagname
            ORDER BY commit_time, pr.package
        ''', (self.name,))
        cur.execute('DELETE FROM t_package_versions WHERE version IS NULL')
        cur.execute('CREATE INDEX idx_t_package_versions '
            'ON t_package_versions (package)')
        cur.execute('''
            REPLACE INTO package_versions
            SELECT t.* FROM t_package_versions t
            LEFT JOIN package_versions v ON t.package=v.package
            AND t.branch=v.branch AND t.version IS v.version
            AND t.release IS v.release AND t.epoch IS v.epoch
            AND t.commit_time IS v.commit_time AND t.githash IS v.githash
            WHERE v.package IS NULL
        ''')
        cur.execute('DROP TABLE t_package_versions')
        self.db.execute('PRAGMA optimize')
        self.db.commit()
        logging.info('Done.')

    def reset_progress(self):
        super().reset_progress()
        mcur = self.marksdb.cursor()
        mcur.execute('DELETE FROM package_rel')
        mcur.execute('DELETE FROM package_basherr')
        self.marksdb.commit()
        mcur.execute('VACUUM')
        self.marksdb.commit()


def main():
    parser = argparse.ArgumentParser(description="Generate metadata database for abbs trees.")
    parser.add_argument("-l", "--local", help="Record local directory only. -p is the path of the directory; -B specifies the branch (optional); -m, -b, -u, --no-sync are ignored.", action='store_true')
    parser.add_argument("-p", "--basepath", help="Directory with both Git and Fossil repositories", default=".", metavar='PATH')
    parser.add_argument("-m", "--markpath", help="Directory with Git and Fossil sync marks", default=".", metavar='PATH')
    parser.add_argument("-d", "--dbfile", help="Abbs meta database file", default="abbs.db", metavar='FILE')
    parser.add_argument("-b", "--branches", help="Branches to consider, seperated by comma (,)", default="stable,testing,explosive")
    parser.add_argument("-B", "--mainbranch", help="Git repo main branch name", metavar='BRANCH')
    parser.add_argument("-c", "--category", help="Category, 'base' or 'bsp'", default="base")
    parser.add_argument("-u", "--url", help="Repo url")
    parser.add_argument("-P", "--priority", help="Priority to consider", type=int, default=0)
    parser.add_argument("-v", "--verbose", help="Show debug logs", action='store_true')
    parser.add_argument("--no-sync", help="Don't sync Git and Fossil repos", action='store_true')
    parser.add_argument("--reset", help="Reset sync status", action='store_true')
    parser.add_argument("name", help="Repository / abbs tree name", nargs='?')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.local:
        repo = LocalRepo(
            args.basepath, args.dbfile, args.name,
            args.mainbranch or '_local', args.category, args.priority)
        repo.update(args.reset)
    else:
        if not args.name:
            raise ValueError("repo name is not specified")
        repo = SourceRepo(
            args.name, args.basepath, args.markpath, args.dbfile,
            args.mainbranch or 'master', args.branches.split(','),
            args.category, args.url, args.priority)
        repo.update(not args.no_sync, args.reset)

if __name__ == '__main__':
    main()
