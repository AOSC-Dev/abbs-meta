#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import sqlite3
import argparse
import collections
import configparser

import requests

API_ENDPOINT = 'https://release-monitoring.org/api/'

re_projectrep = re.compile(r'^[^/]+/|[. _-]')

def anitya_api(method, **params):
    req = requests.get(API_ENDPOINT + method, params=params, timeout=45)
    req.raise_for_status()
    return json.loads(req.content.decode('utf-8'))

def init_db(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS anitya_projects ('
                'id INTEGER PRIMARY KEY,'
                'name TEXT,'
                'homepage TEXT,'
                'backend TEXT,'
                'version_url TEXT,'
                'regex TEXT,'
                'latest_version INTEGER,'
                'updated_on INTEGER,'
                'created_on INTEGER'
                ')')
    cur.execute('CREATE TABLE IF NOT EXISTS anitya_link ('
                'package TEXT PRIMARY KEY,'
                'projectid INTEGER'
                ')')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_anitya_projects'
                ' ON anitya_projects (name)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_anitya_link'
                ' ON anitya_link (projectid)')

def check_update(cur):
    if not anitya_api('version')['version'].startswith('1.'):
        raise ValueError('anitya API version not supported')
    projects = anitya_api('projects')
    for project in projects['projects']:
        cur.execute('REPLACE INTO anitya_projects VALUES (?,?,?,?,?,?,?,?,?)', (
            project['id'], project['name'], project['homepage'], project['backend'],
            project['version_url'], project['regex'], project['version'],
            int(project['updated_on']), int(project['created_on'])
        ))

def detect_links(cur):
    projects = cur.execute('SELECT id, name FROM anitya_projects GROUP BY name').fetchall()
    project_index = {}
    for row in projects:
        name_index = re_projectrep.sub('', row[1].lower())
        if name_index not in project_index:
            project_index[name_index] = row
    links = collections.OrderedDict()
    for row in cur.execute('SELECT name FROM packages ORDER BY name'):
        name = row[0]
        name_index = name.lower().replace('-', '').replace(' ', '').replace('_', '')
        if name_index in project_index:
            links[name] = project_index[name_index]
    return links

def load_config(cur, filename, extra_links=None):
    config = configparser.ConfigParser()
    if os.path.isfile(filename):
        config.read(filename, 'utf-8')
    else:
        config['anitya_links'] = collections.OrderedDict()
    links = collections.OrderedDict()
    for k, v in config['anitya_links'].items():
        values = v.split(' ', 1)
        if len(values) == 1:
            links[k] = (values[0], None)
        else:
            links[k] = (values[0], values[1])
    if extra_links:
        for k, v in extra_links.items():
            if k not in links:
                links[k] = v
                config['anitya_links'][k] = '%d %s' % (v[0], v[1])
        with open(filename, 'w', encoding='utf-8') as f:
            config.write(f)
    for k, v in links.items():
        cur.execute('REPLACE INTO anitya_link VALUES (?,?)', (v[0], k))

def main():
    parser = argparse.ArgumentParser(description="Store and process project versions from Anitya.")
    parser.add_argument("-d", "--detect", help="Auto detect links", action='store_true')
    parser.add_argument("--reset", help="Reset database", action='store_true')
    parser.add_argument("database", help="Abbs database file (abbs.db)")
    parser.add_argument("linkconfig", help="Anitya links ini file")
    args = parser.parse_args()

    db = sqlite3.connect(args.database)
    cur = db.cursor()

    if args.reset:
        cur.execute('DROP TABLE IF EXISTS anitya_projects')
        cur.execute('DROP TABLE IF EXISTS anitya_link')
        cur.execute('VACUUM')

    init_db(cur)
    print('Checking updates...')
    check_update(cur)
    print('Detecting links...')
    if args.detect:
        links = detect_links(cur)
    else:
        links = None
    print('Saving links...')
    load_config(cur, args.linkconfig, links)
    cur.execute('PRAGMA optimize')
    db.commit()
    print('Done.')

if __name__ == '__main__':
    main()
