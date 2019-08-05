from collections import defaultdict
from queue import Queue
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import dict_factory, named_tuple_factory
from redis import Redis
from conf import *


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
            """ INSERT INTO repo (repo_name, author, description, lang)
                VALUES (?, ?, ?, ?)
            """
        )

        self.prep_record = self.session.prepare(
            """ INSERT INTO record (repo_name, author, page_code, page_date, time, star, fork, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

        print("previous db record count: ", row_ctr)
        print("new db record count: ", len(code_and_name) * 3)
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

        print("after loading, qsize=", queue.qsize())

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
        print("# of repos", self.count_repos())
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

    # def read_modify_write(self, page, items):
    #     batch = BatchStatement()
    #     for item in items:
    #         row = self.session.execute(
    #             """ SELECT * FROM record
    #                 WHERE repo_name=%s AND author=%s AND page_code=%s AND page_date=%s
    #                 ORDER BY seq DESC
    #                 LIMIT 1
    #             """, [item["repo_name"], item["author"], page["page_code"], page["page_date"]]
    #         ).one()
    #         if row is None:
    #             batch.add(self.prep_record, (item["repo_name"], item["author"], page["page_code"], page["page_date"],
    #                                          0, item["time"], item["time"], item["rank"]))
    #         else:
    #             old_rank = row["rank"]
    #             new_rank = item["rank"]
    #             old_seq = row["seq"]
    #
    #             if old_rank == new_rank:
    #                 old_start = row["start"]
    #                 batch.add(self.prep_record,
    #                           (item["repo_name"], item["author"], page["page_code"], page["page_date"],
    #                            old_seq, old_start, item["time"], item["rank"]))
    #             else:
    #                 new_seq = old_seq + 1
    #                 old_end = row["end"]
    #                 batch.add(self.prep_record,
    #                           (item["repo_name"], item["author"], page["page_code"], page["page_date"],
    #                            new_seq, old_end, item["time"], item["rank"]))
    #     self.session.execute(batch)


if __name__ == '__main__':
    code_name_set = download_languages()
    conn = Conn()
    conn.init_pages(code_name_set)
