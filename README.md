audiodb
=======

Simple python script to import and export audit tags to an SQLite DB

Dependencies
------------

+ Python2.7: http://python.org (might work with 2.6 if you install argparse, but haven't tested)
+ puddletag: http://puddletag.sourceforge.net

Usage
-----

    python2 musicdb.py [-h] [--log LOG] {import,export} dbpath musicdir
    
    Import/Save files to sqlite database.
    
    positional arguments:
      {import,export}  action to perform. Either import or export
      dbpath           path to sqlite database
      musicdir         path to musicdir used for import/export
    
    optional arguments:
      -h, --help       show this help message and exit
      --log LOG        log level, can be DEBUG, INFO, WARNING, ERROR. Will be printed to console.
  
  
License
-------

GPLv3: http://www.gnu.org/licenses/gpl.html
