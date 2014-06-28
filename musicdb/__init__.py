import argparse
import os
import sqlite3
import sys
import logging

from operator import itemgetter

from puddlestuff import audioinfo

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
        logging.debug(sql + u' ' + u';'.join(args))
        return conn.execute(sql, args)
    else:
        logging.debug(sql)
        return conn.execute(sql)

def initdb(dbpath):
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()
    execute(conn, 'CREATE TABLE IF NOT EXISTS audio (__filename text unique)');
    conn.commit()
    return conn

def import_tag(tag, conn, tables):
    keys = []
    values = []
    for key, value in tag.items():
        if isinstance(value, (int, long)):
            value = unicode(value)
        elif not isinstance(value, basestring):
            value = u"\\\\".join(value)
        keys.append(key)
        values.append(value)

    if set(keys).difference(tables):
        update_db_columns(conn, keys)
    
    placeholder = u','.join(u'?' for z in values)
    insert = u"INSERT OR REPLACE INTO audio (%s) VALUES (%s)" % (u','.join(keys), placeholder)
    execute(conn, insert, values)

def update_db_columns(conn, tables):
    cursor = execute(conn, 'SELECT * from audio')
    new_tables = set(tables).difference(map(itemgetter(0), cursor.description))
    for table in new_tables:
        logging.info(u'Creating table ' % table)
        execute(conn, u'ALTER TABLE audio ADD COLUMN %s text' % table)
    conn.commit()
    
def import_dir(dbpath, dirpath):
    conn = initdb(dbpath)
    cursor = execute(conn, 'SELECT * from audio')
    tables = set(map(itemgetter(0), cursor.description))
    
    for filepath in getfiles(dirpath, True):
        try:
            logging.info("Import started: " + filepath)
            tag = audioinfo.Tag(filepath)
        except Exception, e:
            logging.error("Could not import file: " + filepath)
            logging.exception(e)
        else:
            if tag is not None:
                import_tag(tag, conn, tables)
                logging.info('Imported completed: ' + filepath)
            else:
                logging.warning('Invalid file: ' + filepath)

def parse_args():
    parser = argparse.ArgumentParser(description='Import/Save files to sqlite database.')
    parser.add_argument('action', choices=['import', 'export'],
                        help='action to perform import/export')
    parser.add_argument('dbpath', type=str,
                        help='path to sqlite database')
    parser.add_argument('musicdir', 
                   help='path to musicdir used for import/export')
    parser.add_argument('--log', 
                        help='log level', required=False)
    
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=args.log.upper())
    if args.action == 'import':
        import_dir(args.dbpath, args.musicdir)
    
if __name__ == '__main__':
    parse_args()