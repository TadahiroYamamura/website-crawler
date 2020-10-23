import logging

from crawler.browser import ChromeBrowser as Browser


class ContentIndexer:
    def __init__(self, repository):
        self._repository = repository

    def start(self, url):
        logging.info('crawl started at: ' + url)
        self._store(Browser(url))

    def _store(self, page):
        if not page.available:
            return
        if self._repository.check_if_url_registered(page.canonical_url):
            return
        logging.info('crawling...: ' + page.url)
        self._repository.store_content(page.canonical_url, page.content_type, page.content)
        for url in page.internal_link_urls:
            code = self._repository.get_statuscode_from_cache(url)
            if code is None:
                linked_page = Browser(url, page)
                self._repository.store_cache(url, linked_page.code)
                self._repository.store_link(page.canonical_url, linked_page.url, linked_page.code)
                self._store(linked_page)
            else:
                self._repository.store_link(page.canonical_url, url, code)

    def close(self):
        self._repository.close()
