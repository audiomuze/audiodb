#!/usr/bin/env python2

import argparse
import os
import sqlite3
import sys
import logging

from operator import itemgetter

from puddlestuff import audioinfo

def get_column_names(conn):
    columns = {}
    for row in conn.execute('PRAGMA table_info(audio)'):
        columns[row[1].lower()] = row[1]
    return columns

def removeslash(x):
    while x.endswith('/'):
        return removeslash(x[:-1])
    return x

def issubfolder(parent, child, level=1):
    dirlevels = lambda a: len(a.split('/'))
    parent, child = removeslash(parent), removeslash(child)
    if isinstance(parent, unicode):
        sep = unicode(os.path.sep)
    else:
        sep = os.path.sep
    if child.startswith(parent + sep) and dirlevels(parent) < dirlevels(child):
        return True
    return False

def getfiles(files, subfolders = False):
    if isinstance(files, basestring):
        files = [files]

    isdir = os.path.isdir
    join = os.path.join

    temp = []

    if not subfolders:
        for f in files:
            if not isdir(f):
                yield f
            else:
                dirname, subs, fnames = os.walk(f).next()
                for fname in fnames:
                    yield join(dirname, fname)
    else:
        for f in files:
            if not isdir(f):
                yield f
            else:                
                for dirname, subs, fnames in os.walk(f):
                    for fname in fnames:
                        yield join(dirname, fname)
                    for sub in subs:
                        for fname in getfiles(join(dirname, sub), subfolders):
                            pass

def execute(conn, sql, args=None):
    if args:
        try:
            log_args = (str(z) if not isinstance(z, basestring) else z for z in args)
            logging.debug(sql + u' ' + u';'.join(log_args))
        except:
            pass
        cursor = conn.execute(sql, args)
    else:
        logging.debug(sql)
        cursor =  conn.execute(sql)
    conn.commit()
    return cursor

def initdb(dbpath):
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()
    execute(conn, '''CREATE TABLE IF NOT EXISTS audio (
    __path blob unique,
    __filename blob,
    __dirpath blob,
    __filename_no_ext blob,
    __ext blob)''');
    conn.commit()
    return conn

def import_tag(tag, conn, columns):
    keys = {}
    values = {}
    for key, value in tag.items():
        if key == '__path':
            value = buffer(tag.filepath)
        else:
            if isinstance(value, (int, long)):
                value = unicode(value)
            elif not isinstance(value, basestring):
                value = u"\\\\".join(value)

        try:
            key.decode('ascii')
        except UnicodeEncodeError:
            logging.warning('Invalid tag found %s: %s. Not parsing field.' % (tag.filepath, key))
            continue
        keys[key.lower()] = key
        values[key.lower()] = value

    if set(keys).difference(columns):
        columns = update_db_columns(conn, keys)
    
    keys = sorted(keys)
    values = [values[key] for key in keys]
    placeholder = u','.join(u'?' for z in values)
    keys = ['"%s"' % key for key in keys]
    insert = u"INSERT OR REPLACE INTO audio (%s) VALUES (%s)" % (u','.join(keys), placeholder)
    execute(conn, insert, values)
    return columns

def update_db_columns(conn, columns):
    new_columns = set(columns).difference(get_column_names(conn))
    for column in new_columns:
        logging.info(u'Creating %s column' % columns[column])
        execute(conn, u'ALTER TABLE audio ADD COLUMN "%s" text' % columns[column])
    conn.commit()
    return get_column_names(conn)
    
def import_dir(dbpath, dirpath):
    conn = initdb(dbpath)
    cursor = execute(conn, 'SELECT * from audio')
    columns = get_column_names(conn)
    
    for filepath in getfiles(dirpath, True):
        try:
            logging.info("Import started: " + filepath)
            tag = audioinfo.Tag(filepath)
        except Exception, e:
            logging.error("Could not import file: " + filepath)
            logging.exception(e)
        else:
            if tag is not None:
                try:
                    columns = import_tag(tag, conn, columns)
                    logging.info('Imported completed: ' + filepath)
                except Exception, e:
                    logging.error('Error occured importing file %s' % filepath)
                    logging.exception(e)
                    raise
            else:
                logging.warning('Invalid file: ' + filepath)

    logging.info('Import completed')

def clean_value_for_export(value):
    if not value:
        return value
    if isinstance(value, buffer):
        return str(value)
    elif isinstance(value, str):
        return value
    elif u'\\\\' in value:
        return filter(None, value.split(u'\\\\'))
    else:
        return value

def export_db(dbpath, dirpath):
    conn = sqlite3.connect(dbpath)
    fields = get_column_names(conn)
    cursor = execute(conn, 'SELECT %s from audio' % ",".join('"%s"' % f for f in fields))
    for values in cursor:
        values = map(clean_value_for_export, values)
        new_tag = dict((k,v) for k,v in zip(fields, values))
        filepath = new_tag['__path']
        new_values = dict(z for z in new_tag.iteritems() if not z[0].startswith('__'))
        if not issubfolder(dirpath, filepath):
            logging.info('Skipped %s. Not in dirpath.' % filepath)
            continue
        try:
            logging.info('Updating %s' % filepath)
            tag = audioinfo.Tag(filepath)
        except Exception, e:
            logging.exception(e)
        else:
            logging.debug(new_tag)
            for key, value in new_tag.iteritems():
                if not value and key in tag:
                    del(tag[key])
                else:
                    tag[key] = value
            try:
                tag.save()
                logging.info('Updated tag to %s' % filepath)
            except Exception, e:
                logging.error('Could not save tag to %s' % filepath)
                logging.exception(e)
                
    logging.info('Export complete')

def parse_args():
    parser = argparse.ArgumentParser(description='Import/Save files to sqlite database.')
    parser.add_argument('action', choices=['import', 'export'],
                        help='Action to perform. Either import or export')
    parser.add_argument('dbpath', type=str,
                        help='Path to sqlite database.')
    parser.add_argument('musicdir', 
                   help='path to musicdir used for import/export')
    parser.add_argument('--log', 
                        help='Log level. Can be DEBUG, INFO, WARNING, ERROR. All output is printed to console.', required=False)
    
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=args.log.upper())
    if args.action == 'import':
        import_dir(args.dbpath, args.musicdir)
    else:
        export_db(args.dbpath, args.musicdir)
        
    
if __name__ == '__main__':
    parse_args()