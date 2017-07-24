#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
__author__ = 'rus'
import hashlib
import os
import json
import ConfigParser
import MySQLdb
import MySQLdb.cursors
import datetime
import sys
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf8')

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()


class RTUConfigFetcher(object):
    def __init__(self, fetcher_config_path):
        self.fetcher_config = ConfigParser.ConfigParser()
        self.fetcher_config.readfp(open(fetcher_config_path))

        self.db = MySQLdb.connect(
            host=self.fetcher_config.get(
                'mysql_credentials', 'host'),
            user=self.fetcher_config.get(
                'mysql_credentials', 'user'),
            passwd=self.fetcher_config.get(
                'mysql_credentials', 'password'),
            db=self.fetcher_config.get(
                'mysql_credentials', 'db'),
            charset='utf8',
            cursorclass=MySQLdb.cursors.Cursor
        )

        self.db_cursor = self.db.cursor()

    def fetch_all(self):
        self.fetch_files()
        self.fetch_mysql()        
        self.fetch_md5()

    def fetch_files(self):
        config={}
        if self.fetcher_config.has_section("files"):
            for f in self.fetcher_config.items("files"):
                f_name, f_path = f
                if os.path.isfile(f_path) and os.access(f_path, os.R_OK):
                    try:
                        with open(f_path, "rt") as config_f:
                            config[f_name] = config_f.read()
                    except:
                        pass
        print ('==[ files ]=========================\n ')
        for k in config:
            d = ["==[ %s ]=========================\n%s" % (k, config[k])]
            print ("\n".join(d))

    def fetch_mysql(self):
        config=dict()
        if self.fetcher_config.has_section("mysql"):
            for t in self.fetcher_config.items("mysql"):
                t_name, t_query = t
                try:
                    self.db_cursor.execute(t_query)
                    config[t_name] = self.pretty_rows()
                except MySQLdb.ProgrammingError as why:
                    print(why)
        print ('==[ mysql ]=========================\n')
        print (json.dumps(config, cls=DateTimeEncoder, sort_keys=False, indent=2, separators=(',',': '), ensure_ascii=False))

    def pretty_rows(self):
        row_keys = [each[0] for each in self.db_cursor.description]
        return [OrderedDict(zip(row_keys, each)) for each in self.db_cursor.fetchall()]

    def fetch_md5(self):
        if self.fetcher_config.has_section("md5"):
            dict_md5 = {}
            print ('==[ md5 ]=========================\n')
            for f in self.fetcher_config.items("md5"):
                f_name, f_path = f
                hash_md5 = hashlib.md5()

                try:
                    with open(f_path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash_md5.update(chunk)
                    dict_md5[f_name] = hash_md5.hexdigest()
                except:
                    dict_md5[f_name] = ''
            print (json.dumps(dict_md5, cls=DateTimeEncoder, sort_keys=False,\
            indent=2, separators=(',',': '), ensure_ascii=False))

if __name__ == '__main__':
    cf = RTUConfigFetcher('config_fetcher')
    cf.fetch_all()
