from requests import Session
from time import sleep
from conn import Conn
from queue import Queue, Empty
from threading import Thread, Event
from cassandra.query import BatchStatement
from signal import signal, SIGINT, SIGTERM
from conf import *


class Fetcher:

    def __init__(self):
        self.conn = Conn()
        self.should_exit = Event()
        self.fetch_on_going = Event()
        self.url_queue = Queue()
        self.cass_write_queue = Queue()
        self.redis_write_queue = Queue()

        signal(SIGINT, self.signal_handler)
        signal(SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.should_exit.set()

    def fetcher_routine(self):
        while not self.should_exit.is_set():

            # prepare
            self.fetch_on_going.set()
            self.conn.load_pages(self.url_queue)
            threads, sessions = [], [Session() for _ in range(THREAD_COUNT)]

            # fork
            for session in sessions:
                t = Thread(target=self.thread_perform_fetch, args=(session,))
                t.start()
                threads.append(t)
            cass_thread = Thread(target=self.store_to_cass)
            cass_thread.start()
            redis_thread = Thread(target=self.store_to_redis)
            redis_thread.start()

            # join
            for t in threads:
                t.join()
            self.fetch_on_going.clear()
            cass_thread.join()
            redis_thread.join()

            print("# of repos = ", self.conn.count_repos())
            print("# of records = ", self.conn.count_records())

            if not self.should_exit.is_set():
                assert self.url_queue.qsize() == 0
            assert self.cass_write_queue.qsize() == 0
            assert self.redis_write_queue.qsize() == 0
            print("sleep", FETCH_SLEEP_TIME)
            self.should_exit.wait(FETCH_SLEEP_TIME)

    def thread_perform_fetch(self, session):
        while not self.should_exit.is_set():
            # get from queue
            try:
                page_dict = self.url_queue.get(False)
            except Empty:
                break

            # fetch the web page
            try:
                resp = session.get(page_dict["url"])
                html = etree.HTML(resp.text)
                cnt, items = fetch_trending_page(html)
                page_dict["update_time"] = int(time())
                self.cass_write_queue.put((page_dict, items))
            except Exception as e:
                print("error", repr(e), page_dict)

            # do sleep
            sleep(THREAD_SLEEP_TIME)

    def store_to_cass(self):

        while self.fetch_on_going.is_set() or self.cass_write_queue.qsize():
            batch = BatchStatement()
            batch_cnt = 0
            while batch_cnt < CASS_BATCH_SIZE:
                try:
                    pd, items = self.cass_write_queue.get(True, 3)
                    batch_cnt += (1 + len(items))
                    for ar in items:
                        batch.add(self.conn.prep_record,
                                  (ar["repo_name"], ar["author"], pd["page_code"], pd["page_date"],
                                   ar["time"], ar["rank"], ar["star"], ar["fork"]))
                        batch.add(self.conn.prep_repo,
                                  (ar["repo_name"], ar["author"], ar["description"], ar["lang"]))
                    batch.add(self.conn.prep_page,
                              (pd["page_code"], pd["page_date"], pd["page_name"], pd["update_time"]))

                    key = (pd["page_code"], pd["page_date"])
                    items = sorted(items, key=lambda e: e["rank"])
                    self.redis_write_queue.put((key, items))

                except Empty:
                    if not self.fetch_on_going.is_set():
                        batch_cnt = CASS_BATCH_SIZE
            self.conn.session.execute(batch)

    def store_to_redis(self):
        while self.fetch_on_going.is_set() or self.redis_write_queue.qsize():
            batch_cnt = 0
            while batch_cnt < REDIS_BATCH_SIZE:
                try:
                    key, items = self.redis_write_queue.get(True, 3)
                    batch_cnt += 1
                    self.conn.redis_pipeline.set(str(key), str(items))

                except Empty:
                    if not self.fetch_on_going.is_set():
                        batch_cnt = REDIS_BATCH_SIZE
            self.conn.redis_pipeline.execute()
        self.conn.redis_pipeline.set("time_index_by_name", str(self.conn.select_sorted_pages(True)))
        self.conn.redis_pipeline.set("time_index_by_time", str(self.conn.select_sorted_pages(False)))
        self.conn.redis_pipeline.set("page_index_by_name", str(self.conn.select_distinct_pages()))
        self.conn.redis_pipeline.execute()

        repo_author_dict, author_repo_dict, repo_author_set = self.conn.search_indices()
        for k, v in repo_author_dict.items():
            self.conn.redis.hset("repo_index", k, str(v))
        for k, v in author_repo_dict.items():
            self.conn.redis.hset("author_index", k, str(v))
        for e in repo_author_set:
            self.conn.redis.sadd("repo_author_index", str(e))


if __name__ == '__main__':
    pass
    f = Fetcher()
    f.store_to_redis()
    # f.fetcher_routine()

    # resp = Session().get(pd1["url"])
    # html = etree.HTML(resp.text)
    # fetch_trending_page(html, pd1)
