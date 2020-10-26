import sqlite3
import os
import sys
import hashlib
import datetime
import logging
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import urlparse

import click

from crawler.repository import SQLiteRepository
from crawler.indexer import ContentIndexer


@click.command()
@click.option('-o', '--output', default='crawler.log', help='the file name to output log.')
@click.option('-d', '--repository-dir', default='.', type=click.Path(file_okay=False, dir_okay=True), help='repository directory that stores webpage\'s contents.')
@click.option('--user-agent', help='User-Agent header value')
@click.option('--timeout', default=30, type=int, help='request timeout seconds')
@click.argument('url', required=True)
def start(url, output, repository_dir, user_agent, timeout):
    click.echo('============================')
    click.echo('arguments')
    click.echo()
    click.echo('url: ' + url)
    click.echo('repository dir: ' + repository_dir)
    click.echo('log file: ' + os.path.join(repository_dir, output))
    click.echo('db file: ' + os.path.join(repository_dir, 'crawldata.db'))
    click.echo('user agent: ' + user_agent)
    click.echo('timeout: ' + str(timeout) + ' seconds')
    click.echo('============================')

    if not os.path.isdir(repository_dir):
        os.makedirs(repository_dir)

    logging.basicConfig(filename=os.path.join(repository_dir, output), level=logging.INFO)

    # indexer初期化
    repository = SQLiteRepository(os.path.join(repository_dir, 'crawldata.db'))
    indexer = ContentIndexer(repository)
    indexer.timeout = timeout
    if user_agent is not None:
        indexer.headers['User-Agent'] = user_agent

    # もしエラーが起こった場合はここに吐き出される想定
    errordump_file = os.path.join(repository_dir, 'errordump.csv')
    if os.path.isfile(errordump_file):
        indexer.restore(errordump_file)
    try:
        indexer.start(url)
    except Exception as err:
        logging.error(err)
        indexer.dump(errordump_file)
        click.echo('dump file created at ' + errordump_file)
    finally:
        indexer.close()

if __name__ == '__main__':
    start()
