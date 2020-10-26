import csv
import logging
from collections import deque
from urllib.error import URLError

from crawler.browser import ChromeBrowser as Browser


class ContentIndexer:
    def __init__(self, repository):
        self._repository = repository
        self._queue = deque()
        self.headers = {}

    def start(self, url):
        if url is not None:
            logging.info('crawl started at: ' + url)
            self._queue.append((url, None, self.headers))
        while len(self._queue) > 0:
            x = self._queue.popleft()
            try:
                page = Browser(*x)
            except Exception as err:
                self._queue.appendleft(x)
                logging.error('error occured during opening url')
                raise err
            else:
                if self._repository.check_if_url_registered(page.canonical_url):
                    logging.info('the page already processed.')
                    continue
                self._store_content(page)
                self._store_links(page)
                for url in page.internal_link_urls:
                    self._queue.append((url, page, self.headers))
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
                if isinstance(data, Browser):
                    w.writerow([data.url, ''])
                else:
                    w.writerow([data[0], '' if data[1] is None else data[1].url])

    def restore(self, filepath):
        """
        上記で吐き出したデータを元にqueue内の値を再構築する
        """
        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            r = csv.reader(f)
            for line in r:
                self._queue.append((line[0], None if line[1] == '' else Browser(line[1], None, self.headers), self.headers))
