from requests import Session
from time import time, sleep
from conn import Conn
from queue import Queue, Empty
from threading import Thread, Event
from cassandra.query import BatchStatement
from collections import defaultdict
from signal import signal, SIGINT, SIGTERM
from utilities import *


class Fetcher:

    def __init__(self, thread_count=1):
        self.thread_count = thread_count
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
            threads, sessions = [], [Session() for _ in range(self.thread_count)]

            # fork
            for session in sessions:
                t = Thread(target=self.thread_perform_fetch, args=(session,))
                t.start()
                threads.append(t)
            cass_thread = Thread(target=self.store_to_cass)
            cass_thread.start()

            # join
            for t in threads:
                t.join()
            print("th joined")
            self.fetch_on_going.clear()
            cass_thread.join()
            print("cass_thread joined")

            # update DBs
            print("self.conn.count_repos()", self.conn.count_repos())
            print("self.conn.count_records()", self.conn.count_records())
            self.store_to_redis()

            if not self.should_exit.is_set():
                assert self.url_queue.qsize() == 0
            assert self.cass_write_queue.qsize() == 0
            assert self.redis_write_queue.qsize() == 0
            print("sleep", fetch_sleep_time)
            self.should_exit.wait(fetch_sleep_time)

    def thread_perform_fetch(self, session):
        while not self.should_exit.is_set():
            # get from queue
            try:
                page_dict = self.url_queue.get(False)
            except Empty:
                break
            # print("fetching", page_dict["url"])

            # fetch the web page
            try:
                resp = session.get(page_dict["url"])
                html = etree.HTML(resp.text)
                cnt, items = fetch_trending_page(html, page_dict)
                page_dict["update_time"] = int(time())
                self.cass_write_queue.put((page_dict, items))
            except Exception as e:
                cnt, error = -1, repr(e)
                print("error", error, page_dict)

            # do sleep
            sleep(thread_sleep_time)

    def store_to_cass(self, batch_size=50):

        while self.fetch_on_going.is_set() or self.cass_write_queue.qsize():
            batch = BatchStatement()
            batch_cnt = 0
            while batch_cnt < batch_size:
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
                        batch_cnt = batch_size
            self.conn.session.execute(batch)

    def store_to_redis(self, batch_size=100):
        print("self.redis_write_queue.qsize()", self.redis_write_queue.qsize())
        while self.fetch_on_going.is_set() or self.redis_write_queue.qsize():
            batch_cnt = 0
            while batch_cnt < batch_size:
                try:
                    key, items = self.redis_write_queue.get(True, 3)
                    batch_cnt += 1
                    self.conn.redis_pipeline.set(str(key), str(items))

                except Empty:
                    if not self.fetch_on_going.is_set():
                        batch_cnt = batch_size
            self.conn.redis_pipeline.execute()
        self.conn.redis_pipeline.set("index", str(self.conn.select_sorted_pages()))
        self.conn.redis_pipeline.set("index_by_time", str(self.conn.select_sorted_pages(True)))
        self.conn.redis_pipeline.execute()


if __name__ == '__main__':
    pass
    f = Fetcher(5)
    f.fetcher_routine()
    # f.store_to_redis()

    # resp = Session().get(pd1["url"])
    # html = etree.HTML(resp.text)
    # fetch_trending_page(html, pd1)
