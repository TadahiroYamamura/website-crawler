import csv
import logging
from urllib.error import URLError

from crawler.browser import ChromeBrowser as Browser


class ContentIndexer:
    def __init__(self, repository):
        self._repository = repository
        self._queue = set()
        self.headers = {}
        self.timeout = 30

    def start(self, url):
        if url is not None:
            logging.info('crawl started at: ' + url)
            self._queue.add(Browser.normalize_url(url, None))
        while len(self._queue) > 0:
            x = self._queue.pop()
            try:
                page = Browser(x, None, self.headers, self.timeout)
            except Exception as err:
                self._queue.add(x)
                logging.error('error occured during opening url')
                raise err
            else:
                if self._repository.check_if_url_registered(page.canonical_url):
                    logging.info('the page already processed.')
                    continue
                self._store_content(page)
                self._store_links(page)
                for url in page.internal_link_urls:
                    self._queue.add(Browser.normalize_url(url, page))
                logging.info('page processed.')

    def _store_content(self, page):
        logging.info('crawling...')
        self._repository.store_content(page.canonical_url, page.code, page.content_type, page.content)

    def _store_links(self, page):
        logging.info('list links...')
        url_from = page.url
        self._repository.store_link(url_from, page.internal_link_urls)

    def close(self):
        self._repository.close()

    def dump(self, filepath):
        """
        クロール中にエラーが発生した場合等を想定し、クロールできていないページをファイルに出力する
        """
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            for data in self._queue:
                w.writerow([data])

    def restore(self, filepath):
        """
        上記で吐き出したデータを元にqueue内の値を再構築する
        """
        logging.info('restoreing...')
        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            r = csv.reader(f)
            for line in r:
                self._queue.add(line[0])
