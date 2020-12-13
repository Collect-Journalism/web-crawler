import requests

# https://www.crummy.com/software/BeautifulSoup/bs4/doc/
from bs4 import BeautifulSoup, SoupStrainer

# https://toolz.readthedocs.io/en/latest/index.html
from toolz import pipe
# from toolz.curried import map, get
from toolz.curried import map
from toolz.itertoolz import unique

# https://github.com/googleapis/google-cloud-python
# https://googleapis.dev/python/storage/latest/index.html
from google.cloud import storage

import configparser
import logging
from functools import partial
import re
import json

logging.basicConfig(level=logging.DEBUG, filename='debug.log',
                    filemode='w', format='%(asctime)s %(levelname)s: %(message)s')

config = configparser.ConfigParser()
config.read('config.ini')
config_default = config['DEFAULT']


def crawl_oja(year: int) -> list:
    suop_with_lxml_parser = partial(BeautifulSoup, features='lxml')
    get_suop_under_a_tags = partial(
        suop_with_lxml_parser,
        parse_only=SoupStrainer('a'),
    )

    def get_href(link) -> str:
        return link and link.get('href')

    def get_hrefs(links) -> map:
        return map(lambda link: get_href(link), links)

    def get_text(tag) -> str:
        return tag and tag.get_text('', strip=True)

    def load_content(url: str) -> str:
        try:
            return requests.get(url).content.decode()
        except Exception:
            logging.exception(f'Catch an exception - url={url}')

    def get_start_urls(year: int) -> tuple:
        def find_links(soup: BeautifulSoup) -> list:
            return soup.find_all(href=re.compile('^https://awards.journalists.org/entries/'))

        try:
            return pipe(
                f'https://awards.journalists.org/winners/{year}/',
                load_content,
                get_suop_under_a_tags,
                find_links,
                get_hrefs,
                unique,
                tuple,
            )
        except Exception:
            logging.exception(f'Catch an exception - year={year}')

    def parse(soup: BeautifulSoup) -> dict:
        try:
            # Get title & subtitle from '.pagetitle'
            page_title = soup.find(class_='pagetitle')
            title = pipe(
                page_title.find('h1'),
                get_text,
            )
            subtitle = pipe(
                page_title.find('h2'),
                get_text,
            )

            # Get organizations, award, entry links & main entry link from '.meta.side'
            meta_side = soup.select_one('.meta.side')
            orgs = pipe(
                meta_side.find('strong', string=re.compile(
                    '^Organization')).find_next_siblings('a'),
                map(get_text),
                tuple,
            )
            award = pipe(
                meta_side.find(
                    'strong', string='Award').find_next_sibling('a'),
                get_text,
            )
            entry_links = pipe(
                meta_side.find(
                    'strong', string='Entry Links').find_next_siblings('a'),
                get_hrefs,
                tuple,
            )
            entry_link_main = pipe(
                meta_side.find('a', string='View Entry'),
                get_href,
            )

            return {
                'title': title,
                'subtitle': subtitle,
                'orgs': orgs,
                'award': award,
                'year': year,
                'entry_links': entry_links,
                'entry_link_main': entry_link_main,
                'about_link': about_url,
            }
        except Exception:
            logging.exception(
                f'Catch an exception - year={year} & url={about_url}')

    result = []

    start_urls = pipe(
        year,
        get_start_urls,
        # get([1, 2]),
    )

    for about_url in start_urls:
        item = pipe(
            about_url,
            load_content,
            suop_with_lxml_parser,
            parse,
        )

        result.append(item)

    return result


def upload_to_gcs(bucket_name: str, data: str, destination_path: str) -> None:
    # https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client
    client = storage.Client(project='Collect Journalism')

    # https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client.bucket
    bucket = client.bucket(bucket_name)

    # https://googleapis.dev/python/storage/latest/buckets.html#google.cloud.storage.bucket.Bucket.blob
    blob = bucket.blob(destination_path)

    # https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.upload_from_string
    blob.upload_from_string(data, content_type='application/json')

    logging.debug(f'DATA UPLOADED TO "{bucket_name}/{destination_path}"')


def notify_slack(text: str) -> requests.Response:
    data = {
        'text': text
    }

    # https://api.slack.com/apps/A01FJE2KKLN/incoming-webhooks
    response = requests.post(
        config_default['SLACK_WEBHOOK_URL'],
        headers={
            'Content-Type': 'application/json'
        },
        data=json.dumps(data),
    )

    return response


if __name__ == '__main__':
    YEARS = (2020, 2019, 2018, 2017, 2016, 2015, 2014)
    # YEARS = (2020, 2019)

    for year in YEARS:
        result = crawl_oja(year)

        logging.debug(f'PAGES IN {year} CRAWLED')

        upload_to_gcs(
            config_default['BUCKET_NAME'],
            json.dumps(result),
            f'{config_default['FOLDER_PATH']}general-{year}.json',
        )

    logging.debug('ALL PAGES CRAWLED')

    response = notify_slack(
        f'All OJA data are completed, stored in <{config_default['GCS_LINK_URL']}|GCS>.'
    )

    logging.debug(f'Slack notified - {response.text}')
