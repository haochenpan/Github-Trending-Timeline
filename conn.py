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

from collections import defaultdict
from queue import Queue
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import dict_factory
from redis import Redis
from conf import *

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


class Conn:
    def __init__(self):
        self.redis = Redis(host='localhost', port=6379, db=0)
        self.redis_pipeline = self.redis.pipeline()

        self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace=KEYSPACE)
        self.session.row_factory = dict_factory

        self.prep_page = self.session.prepare(
            """ INSERT INTO page (page_code, page_date, page_name, update_time)
                VALUES (?, ?, ?, ?)
            """
        )

        self.prep_repo = self.session.prepare(
            """ INSERT INTO repo (repo_name, author, page_code, page_date, description, lang)
                VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        self.prep_record = self.session.prepare(
            """ INSERT INTO record (repo_name, author, page_code, page_date, seq, epoch, start, end, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def count_repos(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM repo
            """
        )
        return int(rows.one()["count"])

    def count_records(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM record
            """
        )
        return int(rows.one()["count"])

    def count_pages(self):
        rows = self.session.execute(
            """ SELECT COUNT(*) FROM page
            """
        )
        return int(rows.one()["count"])

    def init_pages(self, code_and_name: set):
        rows = self.session.execute(
            """ SELECT * FROM page
            """
        )
        code_date_dict = defaultdict(lambda: -1)
        row_ctr = 0
        for row in rows:
            row_ctr += 1
            code_date_dict[(row["page_code"], row["page_date"])] = row["update_time"]

        logging.warning("previous db record count: " + str(row_ctr))
        logging.warning("new db record count: " + str(len(code_and_name) * 3))
        for code, name in code_and_name:
            batch = BatchStatement()
            for date in ALLOWABLE_DATE_RANGE:
                batch.add(self.prep_page, (code, date, name, code_date_dict[(code, date)]))
            self.session.execute(batch)

    def load_pages(self, queue: Queue):
        rows = self.session.execute(
            """ SELECT * FROM page;
            """
        )

        rows = sorted(rows, key=lambda e: (e["update_time"], e["page_code"], page_date_sort(e['page_date'])))
        rows = rows[:CASS_DB_FETCH_SIZE]

        for row in rows:
            row["url"] = f'https://github.com/trending/{row["page_code"]}?since={row["page_date"]}'
            queue.put(row)

        logging.warning("after loading, qsize= " + str(queue.qsize()))

    def select_sorted_pages(self, by_name=True):
        rows = self.session.execute(
            """ SELECT * FROM page
            """
        )
        if by_name:
            return sorted(rows, key=lambda e: (e["page_code"], page_date_sort(e['page_date'])))
        else:
            return sorted(rows, key=lambda e: e["update_time"])

    def select_page_index_by_name(self):
        rows = self.select_sorted_pages(True)
        code_name_list = []
        assert len(rows) % 3 == 0
        for i, row in enumerate(rows):
            if i % 3 == 0:
                code_name_list.append((row["page_code"], row["page_name"]))
        return code_name_list

    def select_repo_author_indices(self):
        repo_author_dict = defaultdict(lambda: [])
        author_repo_dict = defaultdict(lambda: [])
        repo_author_set = set()
        rows = self.session.execute(
            """SELECT repo_name, author FROM repo;
            """
        )
        for row in rows:
            repo_author_dict[row["repo_name"]].append(row["author"])
            author_repo_dict[row["author"]].append(row["repo_name"])
            repo_author_set.add((row["repo_name"], row["author"]))
        return repo_author_dict, author_repo_dict, repo_author_set

    def select_repo_or_author_index(self):
        repo_or_author_dict = defaultdict(lambda: [])
        rows = self.session.execute(
            """SELECT * FROM repo;
            """
        )
        for row in rows:
            repo = row["repo_name"].lower()
            author = row["author"].lower()
            if row not in repo_or_author_dict[repo]:
                repo_or_author_dict[repo].append(row)
            if row not in repo_or_author_dict[author]:
                repo_or_author_dict[author].append(row)
        return repo_or_author_dict

    def batch_update_new_records(self, epoch: int, batch, page, items):
        """
        seq: start from 1, then 2, 3, ...
        epoch: int(time())
        item: item dict collected
        page: page dict from db
        row: record row fetched from db
        """
        pass

    def batch_update_old_records(self, epoch: int, keys: set):
        batch = BatchStatement()
        rows = self.session.execute(
            """ SELECT * FROM record PER PARTITION LIMIT 1;
            """
        )
        for i, row in enumerate(rows):
            if row["epoch"] < epoch and row["rank"] != 0 and (row["page_code"], row["page_date"]) in keys:
                new_seq = row["seq"] + 1
                old_end = row["end"]
                pk = (row["repo_name"], row["author"], row["page_code"], row["page_date"])
                logging.warning(f"item disappeared {pk}")
                batch.add(self.prep_record, (*pk, new_seq, epoch, old_end, int(time()), 0))
            if len(batch) > CASS_BATCH_SIZE:
                self.session.execute(batch)
                batch = BatchStatement()
        self.session.execute(batch)


if __name__ == '__main__':
    code_name_set = download_languages()
    conn = Conn()
    conn.init_pages(code_name_set)
