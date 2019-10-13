"""
MIT License

Copyright (c) 2019 Haochen Pan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


"""

from requests import get
from lxml import etree
from time import time, tzset
from os import environ
import logging

THREAD_COUNT = 3
THREAD_SLEEP_TIME = 1
FETCH_SLEEP_TIME = 600
CASS_DB_FETCH_SIZE = 2000
CASS_BATCH_SIZE = 5
REDIS_BATCH_SIZE = 50

KEYSPACE = "trending"
ALLOWABLE_DATE_RANGE = ["daily", "weekly", "monthly"]
environ['TZ'] = 'US/Eastern'
tzset()
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def page_date_sort(word):
    return ALLOWABLE_DATE_RANGE.index(word)


def download_languages():
    trending_page = "https://github.com/trending/?since=monthly"
    resp = get(trending_page)
    html = etree.HTML(resp.text)
    languages = html.xpath('//div[@data-filterable-for="text-filter-field"]/a')
    code_name_set = set()
    for lang in languages:
        link = lang.xpath('string(./@href)').strip()
        assert link.endswith("?since=monthly")
        code = link[link.rfind("/") + 1:link.rfind("?")]
        name = lang.xpath('string((./span)[2])').strip()
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
        star = item.xpath('(./div[@class="f6 text-gray mt-2"]/a)[1]')
        if len(star) != 1:
            logging.warning("error in fetching a trending page")
        else:
            star = star[0].xpath('string(.)').replace(",", "").strip()
            star = int(star)
        fork = item.xpath('(./div[@class="f6 text-gray mt-2"]/a)[2]')
        if len(fork) != 1:
            logging.warning("error in fetching a trending page")
        else:
            fork = fork[0].xpath('string(.)').replace(",", "").strip()
            fork = int(fork)
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
    # for e in download_languages():
    #     print(e)
    # url = "https://github.com/trending/html?since=weekly"
    # resp = get(url)
    # html = etree.HTML(resp.text)
    # l, items = fetch_trending_page(html)
    # print(l)
    # for item in items:
    #     print(item)