from requests import get
from lxml import etree
from time import time

THREAD_COUNT = 3
THREAD_SLEEP_TIME = 1
FETCH_SLEEP_TIME = 120
CASS_DB_FETCH_SIZE = 1000
CASS_BATCH_SIZE = 50
REDIS_BATCH_SIZE = 100

ALLOWABLE_DATE_RANGE = ["daily", "weekly", "monthly"]


def page_date_sort(word):
    return ALLOWABLE_DATE_RANGE.index(word)


def download_languages():
    trending_page = "https://github.com/trending"
    resp = get(trending_page)
    html = etree.HTML(resp.text)
    languages = html.xpath('//div[@data-filterable-for="text-filter-field"]/a[@role="menuitemradio"]')
    code_name_set = set()
    ctr = 0
    for lang in languages:
        # if ctr == 10:
        #     break
        ctr += 1
        link = lang.xpath('string(./@href)').strip()
        assert link.endswith("?since=daily")
        code = link[link.rfind("/") + 1:link.rfind("?")]
        name = lang.xpath('string(.//span)').strip()
        code_name_set.add((code, name))
    code_name_set.add(("/", "Trending"))
    return code_name_set


def fetch_trending_page(html):
    items = html.xpath('//*[@class="Box-row"]')
    item_dicts = []
    rank_counter = 1
    for item in items:
        name_author = item.xpath('string(./h1/a/@href)').strip()
        assert name_author[0] == "/"
        name_author = name_author[1:]
        assert name_author.count("/") == 1
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
                assert False is True
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
        # print(article_dict)
        item_dicts.append(article_dict)
        rank_counter += 1
    return len(item_dicts), item_dicts


if __name__ == '__main__':
    pass
