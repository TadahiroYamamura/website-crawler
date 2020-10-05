import sqlite3
import os
import sys
import hashlib
import datetime
import logging
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urlparse

from crawler.repository import SQLiteRepository
from crawler.indexer import ContentIndexer


if __name__ == '__main__':
    logging.basicConfig(filename='crawler.log', level=logging.INFO)
    url = sys.argv[1]
    repository_path = sys.argv[2] if len(sys.argv) > 2 else '.'

    if not os.path.isdir(repository_path):
        os.makedirs(repository_path)
    repository = SQLiteRepository(os.path.join(repository_path, 'crawldata.db'))
    indexer = ContentIndexer(repository)
    try:
        indexer.start(url)
    finally:
        indexer.close()
