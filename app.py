import sqlite3
import os
import sys
import hashlib
import datetime
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import chromedriver_binary


debug_log_path = 'debug.log'
error_log_path = 'error.log'

def spit(path, type, message):
    msg = '{0:%Y-%m-%d %H:%M:%S} [{1}] {2}\n'.format(datetime.datetime.now(), type, message)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(msg)

def spit_log(message):
    spit(debug_log_path, 'info', message)

def spit_error(message):
    spit(error_log_path, 'error', message)

class Indexer:
    def __init__(self, path):
        self._repository = SQLiteRepository(os.path.abspath(os.path.join(path, 'result.db')))
        self._browser = WebdriverBrowser(service_args=['--log-path=' +  os.path.abspath(os.path.join(path, 'browser.log'))])

    def start(self, url):
        self.store_contents(URL(url))

    def store_contents(self, url):
        if not url.available():
            return
        elif not self._repository.check_if_url_is_unique(url):
            return
        self._browser.change_url(url)
        spit_log('{}\t{}'.format(url.value, self._browser.current_url.value))
        self._repository.store_content(self._browser.current_url, self._browser.get_content())
        available_links = list(
            filter(lambda lnk: self._repository.store_link(self._browser.current_url, lnk),
            self._browser.list_internal_link_urls()))
        for x in available_links:
            self.store_contents(x)

    def close(self):
        self._browser.close()
        self._repository.close()


class WebdriverBrowser:
    def __init__(self, *, service_args=[]):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--incognito')
        self._driver = webdriver.Chrome(options=options, service_args=service_args)

    def change_url(self, url):
        self._domain = urlparse(url.value).netloc
        self._driver.get(url.value)
        self._url = URL(self._driver.current_url)

    def get_content(self):
        return self._driver.find_element_by_xpath("//*").get_attribute("outerHTML")

    def list_internal_link_urls(self):
        return list(
            map(lambda x: URL(x),
                filter(lambda x: x is not None and x.strip() != '' and self._domain in x,
                    map(lambda x: x.get_attribute('href'), 
                        self._driver.find_elements_by_tag_name('a')))))

    @property
    def current_url(self):
        return self._url

    def close(self):
        self._driver.close()

class URL:
    def __init__(self, url):
        self._url = url
        try:
            res = urlopen(url)
        except HTTPError as e:
            self._code = e.getcode()
            self._canonical_url = None
        else:
            self._code = res.getcode()
            self._canonical_url = res.geturl().split('#')[0]

    def exists(self):
        return self._code != 404

    def available(self):
        return str(self._code)[0] == '2'

    @property
    def code(self):
        return self._code

    @property
    def value(self):
        return self._canonical_url

    @property
    def dirty_value(self):
        return self._url

class SQLiteRepository:
    def __init__(self, path):
        self._path = path
        if os.path.isfile(path):
            os.remove(path)
        self._connection = sqlite3.connect(path)
        c = self._connection.cursor()
        try:
            c.execute('create table content (url text primary key, content text not null, digest text not null)')
            c.execute('create table link (source text not null, href text not null, link_to text, status integer not null)')
        finally:
            c.close()

    def check_if_url_is_unique(self, url):
        c = self._connection.cursor()
        try:
            c.execute('select count(*) from content where url = ?', (url.value,))
            return c.fetchone()[0] == 0
        finally:
            c.close()

    def store_link(self, link_from, link_to):
        c = self._connection.cursor()
        sql = 'insert into link(source, href, link_to, status) values (?, ?, ?, ?)'
        try:
            if link_to.available():
                c.execute(sql, (link_from.value, link_to.dirty_value, link_to.value, link_to.code))
            else:
                c.execute(sql, (link_from.value, link_to.dirty_value, None, link_to.code))
            self._connection.commit()
            return link_to.available()
        except:
            self._connection.rollback()
            raise
        finally:
            c.close()

    def store_content(self, url, content):
        c = self._connection.cursor()
        try:
            digest = hashlib.sha256(content.encode('utf-8')).hexdigest()
            c.execute('insert into content(url, content, digest) values (?, ?, ?)', (url.value, content, digest))
            self._connection.commit()
        except sqlite3.IntegrityError as e:
            spit_error('error page: {} message: {}'.format(url.dirty_value, e.with_traceback(sys.exc_info()[2])))
            self._connection.rollback()
        except Exception as e:
            spit_error('error page: {} message: {}'.format(url.dirty_value, e.message))
            self._connection.rollback()
            raise
        finally:
            c.close()

    def close(self):
        self._connection.close()


if __name__ == '__main__':
    url = sys.argv[1]
    path = sys.argv[2] if len(sys.argv) > 2 else '.'
    debug_log_path = os.path.abspath(os.path.join(path, debug_log_path))
    error_log_path = os.path.abspath(os.path.join(path, error_log_path))

    if not os.path.isdir(path):
        os.makedirs(path)
    indexer = Indexer(path)
    try:
        indexer.start(url)
    finally:
        indexer.close()
