#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import shutil
import sqlite3
import warnings
import tempfile
import unittest
import subprocess
import collections

import bashvar
import reposync

class TestRepoSync(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.gitupstream = os.path.join(self.path, 'upstream')
        os.mkdir(self.gitupstream)
        subprocess.run(('git', 'init', '--bare'), cwd=self.gitupstream).check_returncode()
        self.gitrepo = os.path.join(self.path, 'local')
        self.markpath = os.path.join(self.path, 'marks')
        os.mkdir(self.markpath)
        subprocess.run(('git', 'clone', 'upstream', 'local'), cwd=self.path).check_returncode()
        cmds = (
            'echo a > a',
            'echo b > b',
            'git add .',
            'git commit -m "1"',
            'echo c > c',
            'echo d > d',
            'git add .',
            'git commit -m "2"',
            'git push'
        )
        for cmd in cmds:
            subprocess.run(cmd, cwd=self.gitrepo, shell=True).check_returncode()
        self.assertTrue(os.path.isdir(self.gitrepo))
        self.fossil = os.path.join(self.path, 'repo.fossil')

    def tearDown(self):
        shutil.rmtree(self.path)

    def test_sync(self):
        if os.path.isfile(self.fossil):
            os.unlink(self.fossil)
        reposync.sync(self.gitrepo, self.fossil, self.markpath)
        self.assertTrue(os.path.isfile(self.fossil))
        marksdb = os.path.join(self.markpath, 'repo-marks.db')
        self.assertTrue(os.path.isfile(marksdb))
        db = sqlite3.connect(marksdb)
        cur = db.cursor()
        self.assertTrue(cur.execute('SELECT 1 FROM marks'))
        self.assertTrue(cur.execute('SELECT 1 FROM committers'))
        db.close()

    def test_forcepush(self):
        if os.path.isfile(self.fossil):
            os.unlink(self.fossil)
        reposync.sync(self.gitrepo, self.fossil, self.markpath) 
        self.gitrepo2 = os.path.join(self.path, 'local2')
        subprocess.run(('git', 'clone', 'upstream', 'local2'), cwd=self.path).check_returncode()
        self.assertTrue(os.path.isdir(self.gitrepo2))
        cmds = (
            'git reset --hard HEAD^',
            'git push --force',
            'git gc',
            'echo e > e',
            'git add .',
            'git commit -m "3"',
            'echo f > f',
            'git add .',
            'git commit -m "4"',
            'git push'
        )
        for cmd in cmds:
            subprocess.run(cmd, cwd=self.gitrepo, shell=True).check_returncode()
        cmds = (
            'git fetch --all',
            'git reset --hard origin/master',
            'git pull'
        )
        for cmd in cmds:
            subprocess.run(cmd, cwd=self.gitrepo2, shell=True).check_returncode()
        reposync.sync(self.gitrepo, self.fossil, self.markpath)
        # time.sleep(1000000)


class TestBashVar(unittest.TestCase):

    def test_parse(self):
        empty = collections.OrderedDict()
        self.assertEqual(bashvar.eval_bashvar_literal(''), empty)
        self.assertEqual(bashvar.eval_bashvar_literal('\n'), empty)
        source = '''
PKGDES="SDL and OpenGL bindings for Erlang"
PKGNAME=elixir # sfdsfsdf
# tebbddvs
PKGSEC=devel
AUTOTOOLS_AFTER="--without-included-boost \
                 --with-enchant \
                 --with-hunspell \
                 QTDIR=/usr/lib/qt4 \
                 MOC=/usr/lib/qt4/bin/moc"
PKGDEP="dialog ghostscript x11-lib x11-app fontconfig freetype gc graphite \
        harfbuzz icu libpaper libpng poppler libgd t1lib python-2 ruby \
        perl-tk openjdk libsigsegv mpfr pixman poppler ed openjpeg-legacy"
GITSRC='https://github.com/Icenowy/RUCE'
VER=4.89
SRCTBL="http://www.mirrorservice.org/sites/ftp.exim.org/pub/exim/exim${VER:0:1}/exim-$VER.tar.gz"
SRCTBL2=http://quassel-irc.org/pub/quassel-${VER}.tar.bz2
SRCTBL3=http://quassel-irc.org/pub/quassel-$VER.tar.bz2

a=4
a+=5
a+=$PKGSEC
b=""
c=''
d=
e='a\\
b'

string=01234567890abcdefgh
s1=${string:7}
# 7890abcdefgh
s2=${string:7:0}
s3=${string:7:2}
# 78
s4=${string:7:-2}
# 7890abcdef
s5=${string: -7}
# bcdefgh
s6=${string: -7:0}

s7=${string: -7:2}
# bc
s8=${string: -7:-2}
# bcdef
'''
        expected = collections.OrderedDict((
            ('PKGDES', 'SDL and OpenGL bindings for Erlang'),
            ('PKGNAME', 'elixir'),
            ('PKGSEC', 'devel'),
            ('AUTOTOOLS_AFTER', '--without-included-boost                  '
            '--with-enchant                  --with-hunspell                  '
            'QTDIR=/usr/lib/qt4                  MOC=/usr/lib/qt4/bin/moc'),
            ('PKGDEP', 'dialog ghostscript x11-lib x11-app fontconfig freetype'
            ' gc graphite         harfbuzz icu libpaper libpng poppler libgd'
            ' t1lib python-2 ruby         perl-tk openjdk libsigsegv mpfr'
            ' pixman poppler ed openjpeg-legacy'),
            ('GITSRC', 'https://github.com/Icenowy/RUCE'),
            ('VER', '4.89'),
            ('SRCTBL', 'http://www.mirrorservice.org/sites/ftp.exim.org/pub/exim/exim4/exim-4.89.tar.gz'),
            ('SRCTBL2', 'http://quassel-irc.org/pub/quassel-4.89.tar.bz2'),
            ('SRCTBL3', 'http://quassel-irc.org/pub/quassel-4.89.tar.bz2'),
            ('a', '45devel'), ('b', ''), ('c', ''), ('d', ''), ('e', 'a\\\nb'),
            ('string', '01234567890abcdefgh'),
            ('s1', '7890abcdefgh'), ('s2', ''), ('s3', '78'),
            ('s4', '7890abcdef'), ('s5', 'bcdefgh'), ('s6', ''),
            ('s7', 'bc'), ('s8', 'bcdef'))
        )
        result = bashvar.eval_bashvar_literal(source)
        self.assertEqual(result, expected)

    def test_fail(self):
        sources = (
            '  true',
            'a=1 b=2',
            ' a = 1 ',
            'a=1; b=2',
            'a=1 && b=2',
            'a=~/.config',
            'a=b*.txt',
            'a=a{d,c,b}e',
            'a=${parameter:-word}',
            'a=${parameter:=word}',
            'a=${parameter:?word}',
            'a=${parameter:+word}',
            'a=${!prefix*}',
            'a=${!prefix@}',
            'a=${!name[@]}',
            'a=${!name[*]}',
            'a=${#parameter}',
            'a=${parameter#word}',
            'a=${parameter##word}',
            'a=${parameter%word}',
            'a=${parameter%%word}',
            'a=${parameter/pattern/string}',
            'a=${parameter^pattern}',
            'a=${parameter^^pattern}',
            'a=${parameter,pattern}',
            'a=${parameter,,pattern}',
            'a=${parameter@Q}',
            'a=$(command)',
            'a=`command`',
            'a=$(( 1+1 ))',
            'a=<(list)',
            'a=>(list)',
            'a=[abc].txt',
            'a=1?2.txt',
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for source in sources:
                with self.assertRaises(bashvar.ParseException):
                    bashvar.eval_bashvar_literal(source)

    def test_warn(self):
        source = '''
        b=${a:2:4}
        a=2
        c=b$a
        '''
        expected = collections.OrderedDict((
            ('b', ''), ('a', '2'), ('c', 'b2')
        ))
        with self.assertWarns(bashvar.VariableWarning) as cm:
            result = bashvar.eval_bashvar_literal(source)
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()