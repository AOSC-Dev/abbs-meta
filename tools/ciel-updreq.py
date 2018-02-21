#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Use Ciel to automatically do updates
'''

import os
import re
import sys
import logging
import argparse
import requests
import readline
import subprocess

logging.basicConfig(
    format='[%(levelname)s] %(message)s', level=logging.INFO)

re_variable = re.compile('^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=(.+)$')

ANSI_BOLD = '\x1b[1m'
ANSI_RESET = '\x1b[0m'
SUDO = ('sudo',) if os.geteuid() != 0 else ()

def ask(s, default=None, yn=False):
    while 1:
        question = ANSI_BOLD + s + ANSI_RESET
        if yn:
            if default is None:
                question += ' [y/n]'
            elif default:
                question += ' [Y/n]'
            else:
                question += ' [y/N]'
        elif default:
            question += ' [%s]' % default
        res = input(question + ': ')
        if not res and default is not None:
            return default
        elif yn:
            res = res.lower().strip()
            if res == 'y':
                return True
            elif res == 'n':
                return False
            else:
                continue
        elif res:
            return res

def find_package(tree, package):
    for pkgcate in os.listdir(tree):
        pkgcate_path = os.path.join(tree, pkgcate)
        if not os.path.isdir(pkgcate_path):
            continue
        for pkgname in os.listdir(pkgcate_path):
            if pkgname == package:
                return os.path.join(pkgcate, pkgname)

def specparse(spec):
    var = {}
    lines = spec.splitlines(True)
    for k, ln in enumerate(lines):
        match = re_variable.match(ln)
        if match:
            var[match.group(1)] = (k, match.group(2))
    return var, lines

def specupdate(var, lines, version):
    if 'VER' in var:
        lines[var['VER'][0]] = 'VER=%s\n' % version
    else:
        raise ValueError('variable $VER not found in spec')
    if 'REL' in var:
        del lines[var['REL'][0]]
    return ''.join(lines)

def find_upstream(package):
    req = requests.get('https://packages.aosc.io/packages/%s?type=json' % package)
    req.raise_for_status()
    d = req.json()
    srcurl = d['pkg'].get('srcurl_base')
    if not srcurl:
        return None, None
    ver = None
    if srcurl.startswith('https://github.com'):
        repo = '/'.join(srcurl.split('/')[3:5])
        req = requests.get(
            'https://api.github.com/repos/%s/releases/latest' % repo)
        req.raise_for_status()
        ghrel = req.json()
        ver = ghrel['name']
    return srcurl, ver

def try_build(instance, package):
    subprocess.run(
        SUDO + ('ciel', 'rollback', '-i', instance)).check_returncode()
    subprocess.run(
        SUDO + ('ciel', 'build', '-i', instance, package)).check_returncode()

def interactive(instance, package, build_only=False):
    path = find_package('TREE', package)
    if not path:
        raise ValueError("Error: package '%s' not found" % package)
    specfile = os.path.join('TREE', path, 'spec')
    specvar, speclines = specparse(open(specfile, 'r').read())
    if 'VER' in specvar:
        curver = specvar['VER'][1]
    else:
        raise ValueError('variable $VER not found in spec')
    logging.info("Package %s/%s spec version: %s" %
        (path.split('/')[0], package, curver))
    if not build_only:
        try:
            srcurl, latestver = find_upstream(package)
        except Exception as ex:
            logging.exception('Unable to find upstream links.')
            srcurl, latestver = None, None
        if srcurl:
            logging.info('Upstream link: ' + srcurl)
        if latestver:
            logging.info('Latest version: ' + latestver)
        try:
            newver = ask('Update to', latestver)
        except (EOFError, KeyboardInterrupt):
            logging.warning('Will not modify spec.')
            newver = None
        if newver:
            newspec = specupdate(specvar, speclines, newver)
            with open(specfile, 'w', encoding='utf-8') as f:
                f.write(newspec)
            logging.info('New spec written.')
    logging.info('Start building...')
    try:
        try_build(instance, package)
    except Exception as ex:
        logging.error('Build failed, %s: %s' % (type(ex).__name__, str(ex)))
        return
    logging.info('Build done.')
    try:
        testcmd = ask('Test command', package + ' --version')
    except (EOFError, KeyboardInterrupt):
        logging.warning('Will not test.')
        testcmd = None
    subprocess.run(SUDO + ('ciel', 'shell', '-i', instance, testcmd))
    subprocess.run(('git', 'diff'), cwd='TREE').check_returncode()
    if ask('Commit?', yn=True):
        msg = '%s: update to %s' % (package, newver)
        subprocess.run(
            ('git', 'add', path), cwd='TREE').check_returncode()
        subprocess.run(
            ('git', 'commit', '-m', msg), cwd='TREE').check_returncode()

def main():
    parser = argparse.ArgumentParser(
        description="Use ciel to automatically make updates.",
        epilog="Current directory should be ciel working directory.")
    parser.add_argument("-b", "--build-only", help="Don't ask to modify spec file.", action='store_true')
    parser.add_argument("instance", help="Ciel instance")
    parser.add_argument("package", help="Package to build")
    args = parser.parse_args()
    interactive(args.instance, args.package, args.build_only)

if __name__ == '__main__':
    sys.exit(main())
