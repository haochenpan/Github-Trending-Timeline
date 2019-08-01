from collections import defaultdict
from queue import Queue, Empty
import redis
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import dict_factory, named_tuple_factory

from utilities import *


class Conn:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.redis_pipeline = self.redis.pipeline()

        self.cluster = Cluster()
        self.session = self.cluster.connect()  # named_tuple_factory

        self.prep_page = self.session.prepare(
            """ INSERT INTO trending.page (page_code, page_date, page_name, update_time)
                VALUES (?, ?, ?, ?)
            """
        )

        self.prep_repo = self.session.prepare(
            """ INSERT INTO trending.repo (repo_name, author, description, lang)
                VALUES (?, ?, ?, ?)
            """
        )

        self.prep_record = self.session.prepare(
            """ INSERT INTO trending.record (
                repo_name, author, page_code, page_date, time, rank, star, fork)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def count_repos(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM trending.repo
            """
        )
        return int(rows.one()[0])

    def count_records(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM trending.record
            """
        )

        return int(rows.one()[0])

    def count_pages(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM trending.page
            """
        )
        return int(rows.one()[0])

    def insert_pages(self, code_and_name: set):
        rows = self.session.execute(
            """ SELECT * FROM trending.page
            """
        )
        code_date_dict = defaultdict(lambda: -1)
        row_ctr = 0
        for row in rows:
            row_ctr += 1
            code_date_dict[(row[0], row[1])] = row[3]

        print("inserting pages, set size: ", len(code_and_name))
        print("expect db record count: ", len(code_and_name) * 3)
        print("previous db record count: ", row_ctr)
        for code, name in code_and_name:
            batch = BatchStatement()
            for date in allowable_date_range:
                batch.add(self.prep_page, (code, date, name, code_date_dict[(code, date)]))
            self.session.execute(batch)

    def load_pages(self, queue: Queue):
        print("before loading, qsize=", queue.qsize())
        self.session.row_factory = dict_factory
        rows = self.session.execute(
            """ SELECT * FROM trending.page ALLOW FILTERING;
            """
        )
        for row in rows:
            row["url"] = f'https://github.com/trending/{row["page_code"]}?since={row["page_date"]}'
            queue.put(row)

        self.session.row_factory = named_tuple_factory
        print("after loading, qsize=", queue.qsize())

    def select_sorted_pages(self, by_time=False):

        if not by_time:
            self.session.row_factory = dict_factory
            rows = self.session.execute(
                """ SELECT * FROM trending.page
                """
            )
            self.session.row_factory = named_tuple_factory
            return sorted([row for row in rows], key=lambda e: (e["page_name"], page_date_sort(e['page_date'])))
        else:
            self.session.row_factory = dict_factory
            rows = self.session.execute(
                """ SELECT * FROM trending.page
                """
            )
            self.session.row_factory = named_tuple_factory
            return sorted([row for row in rows], key=lambda e: e["update_time"])

    def select_repos(self):
        rows = self.session.execute(
            """ SELECT * FROM trending.repo
            """
        )
        for row in rows:
            print(row)

    def select_records(self):
        rows = self.session.execute(
            """ SELECT * FROM trending.record
            """
        )
        for row in rows:
            print(row)


if __name__ == '__main__':
    pass

    code_name_set = download_languages()
    conn = Conn()
    conn.insert_pages(code_name_set)
    print("conn.count_pages()", conn.count_pages())

    targets = {
        'Trending': '/trending/?since=daily',
        'C': '/trending/c?since=daily',
        'C++': '/trending/c++?since=daily',
        'Go': '/trending/go?since=daily',
        'HTML': '/trending/html?since=daily',
        'Java': '/trending/java?since=daily',
        'JavaScript': '/trending/javascript?since=daily',
        'Python': '/trending/python?since=daily',
        'Unknown languages': '/trending/unknown?since=daily'
    }
