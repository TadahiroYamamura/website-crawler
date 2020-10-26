import logging
import os
import sqlite3
import hashlib
from collections.abc import Iterable


class SQLiteRepository:
    def __init__(self, path):
        self._path = path
        if os.path.isfile(path):
            os.remove(path)
        self._connection = sqlite3.connect(path)
        c = self._connection.cursor()
        try:
            c.execute('create table content (url text primary key, status integer, content_type text not null, content text not null, digest text not null)')
            c.execute('create table link (source text not null, dest text not null)')
        finally:
            c.close()

    def check_if_url_registered(self, url):
        c = self._connection.cursor()
        try:
            c.execute('select count(*) from content where url = ?', (url,))
            return c.fetchone()[0] != 0
        except StopIteration:
            return None
        finally:
            c.close()

    def store_content(self, url, status, content_type, content):
        c = self._connection.cursor()
        try:
            digest = hashlib.sha256(content.encode('utf-8')).hexdigest()
            c.execute('insert into content(url, status, content_type, content, digest) values (?, ?, ?, ?, ?)', (url, status, content_type, content, digest))
            self._connection.commit()
        except Exception as e:
            logging.error('error page: {} message: {}'.format(url, e.message))
            self._connection.rollback()
            raise
        finally:
            c.close()

    def store_link(self, link_from, link_to):
        c = self._connection.cursor()
        sql = 'insert into link(source, dest) values (?, ?)'
        try:
            if isinstance(link_to, str):
                c.execute(sql, (str(link_from), link_to))
            elif isinstance(link_to, Iterable):
                args = list(map(lambda l: (link_from, str(l)), link_to))
                c.executemany(sql, args)
            else:
                c.execute(sql, (str(link_from), str(link_to)))
            self._connection.commit()
        except:
            self._connection.rollback()
            raise
        finally:
            c.close()

    def close(self):
        self._connection.close()
