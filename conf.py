from requests import get
from lxml import etree
from time import time, tzset
import os
import logging

THREAD_COUNT = 3
THREAD_SLEEP_TIME = 1
FETCH_SLEEP_TIME = 600
CASS_DB_FETCH_SIZE = 2000
CASS_BATCH_SIZE = 5
REDIS_BATCH_SIZE = 100

KEYSPACE = "trending"
ALLOWABLE_DATE_RANGE = ["daily", "weekly", "monthly"]
os.environ['TZ'] = 'US/Eastern'
tzset()
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def page_date_sort(word):
    return ALLOWABLE_DATE_RANGE.index(word)


def download_languages():
    trending_page = "https://github.com/trending/?since=monthly"
    resp = get(trending_page)
    html = etree.HTML(resp.text)
    languages = html.xpath('//div[@data-filterable-for="text-filter-field"]/a[@role="menuitemradio"]')
    code_name_set = set()
    for lang in languages:
        link = lang.xpath('string(./@href)').strip()
        assert link.endswith("?since=monthly")
        code = link[link.rfind("/") + 1:link.rfind("?")]
        name = lang.xpath('string(.//span)').strip()
        code_name_set.add((code, name))
    code_name_set.add(("/", "Any Language"))
    return code_name_set


def fetch_trending_page(html):
    items = html.xpath('//*[@class="Box-row"]')
    item_dicts = []
    rank_counter = 1
    for item in items:
        name_author = item.xpath('string(./h1/a/@href)').strip()
        if name_author[0] != "/":
            logging.warning("error in fetching a trending page")
        name_author = name_author[1:]
        if name_author.count("/") != 1:
            logging.warning("error in fetching a trending page")

        author = name_author[:name_author.index("/")]
        repo_name = name_author[name_author.index("/") + 1:]
        description = item.xpath('string(./p)').strip()
        lang = item.xpath('string(.//*[@itemprop="programmingLanguage"])').strip()
        stars_and_forks = item.xpath('.//*[@class="muted-link d-inline-block mr-3"]')

        star, fork = 0, 0
        for e in stars_and_forks:
            if 'stargazers' in e.xpath('string(./@href)'):
                star = e.xpath('string(.)').replace(",", "").strip()
                star = int(star)
            elif 'members' in e.xpath('string(./@href)'):
                fork = e.xpath('string(.)').replace(",", "").strip()
                fork = int(fork)
            else:
                logging.warning("error in fetching a trending page")
        article_dict = {
            "repo_name": repo_name,
            "author": author,
            "description": description,
            "lang": lang,
            "time": int(time()),
            "rank": rank_counter,
            "star": star,
            "fork": fork,
        }

        item_dicts.append(article_dict)
        rank_counter += 1
    return len(item_dicts), item_dicts


if __name__ == '__main__':
    pass
