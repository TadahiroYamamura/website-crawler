import logging
import re
from urllib.parse import urlparse, urljoin, quote
from urllib.request import urlopen
from urllib.error import HTTPError

from bs4 import BeautifulSoup


class ChromeBrowser:
    def __init__(self, page_url, from_page=None):
        self._init_vars()
        url = self._normalize_url(page_url, from_page)

        try:
            with urlopen(self._quote_url(url)) as res:
                self._header = { h[0]: h[1] for h in res.getheaders() }
                self._code = res.getcode()
                self._url = res.geturl()
                self._soup = BeautifulSoup(res.read(), 'html.parser')
        except HTTPError as e:
            self._header = {  h[0]: h[1] for h in e.getheaders() }
            self._code = e.getcode()
            self._url = e.geturl()
            self._response_error = e

    def _init_vars(self):
        self._header = {}
        self._code = -1
        self._url = ''
        self._soup = None
        self._error = None

    def _quote_url(self, url):
        return ''.join(map(lambda x: x if ord(x) < 256 else quote(x), url))

    def _normalize_url(self, page_url, from_page):
        if from_page is None: return page_url
        if urlparse(page_url).netloc != '': return page_url

        urlinfo = urlparse(from_page.url)
        path = urlinfo.path
        if '/' not in path:
            base_url = from_page.url.rstrip('/') + '/'
        if len(urlinfo.path.split('/')[-1].split('.')) > 1:
            base_url = from_page.url
        else:
            base_url = from_page.url.rstrip('/') + '/'
        return urljoin(base_url, page_url)

    def get_header(self, key):
        try:
            return self._header[next(filter(lambda k: k.lower() == key.lower(), self._header.keys()))]
        except StopIteration:
            return None

    def get_meta(self, key):
        try:
            filterd = filter(lambda item: item[0].lower() == key.lower(), self.meta.items())
            return next(filterd)[1]
        except StopIteration:
            return None

    def is_url_available(self):
        return str(self.code)[0] == '2'

    def _attr_name(self, element, key):
        try:
            return next(filter(lambda a: a.lower() == key.lower(), element.attrs.keys()))
        except StopIteration:
            return None

    def _is_internal(self, anchor):
        name = self._attr_name(anchor, 'href')
        if name is None: return False
        url = anchor.get(name)
        if url is None: return False
        if url.strip() == '': return False
        return self.domain in urlparse(self._normalize_url(url, self)).netloc

    @property
    def code(self):
        return self._code

    @property
    def available(self):
        return str(self.code)[0] == '2'

    @property
    def meta(self):
        return { self._attr_name(m, 'name').lower(): m.get(self._attr_name(m, 'content'))
                for m in self._soup.find_all('meta')
                if self._attr_name(m, 'name') is not None }

    @property
    def domain(self):
        return urlparse(self.url).netloc

    @property
    def url(self):
        return self._url

    @property
    def canonical_url(self):
        canon = self.get_meta('canonical')
        return self.url if canon is None else canon

    @property
    def content(self):
        return str(self._soup)

    @property
    def links(self):
        return [e for e in self._soup.find_all('a')]

    @property
    def internal_links(self):
        return [e for e in self.links if self._is_internal(e)]

    @property
    def internal_link_urls(self):
        return list(set([self._normalize_url(e.get(self._attr_name(e, 'href')).strip(), self)
            for e in self.internal_links]))
