import logging
import os
import sqlite3
import hashlib


class SQLiteRepository:
    def __init__(self, path):
        self._path = path
        if os.path.isfile(path):
            os.remove(path)
        self._connection = sqlite3.connect(path)
        c = self._connection.cursor()
        try:
            c.execute('create table cache (url text primary key, status)')
            c.execute('create table content (url text primary key, content_type text not null, content text not null, digest text not null)')
            c.execute('create table link (source text not null, dest text not null, status integer not null)')
        finally:
            c.close()

    def get_statuscode_from_cache(self, url):
        c = self._connection.cursor()
        try:
            c.execute('select status from cache where url = ?', (url,))
            res = c.fetchone()
            return None if res is None else res[0]
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

    def store_cache(self, url, status):
        c = self._connection.cursor()
        try:
            c.execute('insert into cache(url, status) values (?, ?)', (url, status))
        finally:
            c.close()

    def store_content(self, url, content_type, content):
        c = self._connection.cursor()
        try:
            digest = hashlib.sha256(content.encode('utf-8')).hexdigest()
            c.execute('insert into content(url, content_type, content, digest) values (?, ?, ?, ?)', (url, content_type, content, digest))
            self._connection.commit()
        except Exception as e:
            logging.error('error page: {} message: {}'.format(url, e.message))
            self._connection.rollback()
            raise
        finally:
            c.close()

    def store_link(self, link_from, link_to, status):
        c = self._connection.cursor()
        try:
            sql = 'insert into link(source, dest, status) values (?, ?, ?)'
            c.execute(sql, (link_from, link_to, status))
            self._connection.commit()
        except:
            self._connection.rollback()
            raise
        finally:
            c.close()

    def close(self):
        self._connection.close()
