from requests import Session
from time import sleep
from conn import Conn
from queue import Queue, Empty
from threading import Thread, Event
from cassandra.query import BatchStatement
from signal import signal, SIGINT, SIGTERM
from conf import *

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


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
                if resp.status_code != 200:
                    logging.warning(f"{resp.url} not 200!")
                    continue
                logging.warning(f"fetching {resp.url}")
                html = etree.HTML(resp.text)
                cnt, items = fetch_trending_page(html)
                page_dict["update_time"] = int(time())
                self.cass_write_queue.put((page_dict, items))
            except Exception as e:
                logging.error(f"Exception occurred {page_dict}", exc_info=True)

            # do sleep
            sleep(THREAD_SLEEP_TIME)

    def store_to_cass(self):
        epoch = int(time())
        page_visited_key_set = set()
        batch = BatchStatement()

        while self.fetch_on_going.is_set() or self.cass_write_queue.qsize():
            try:
                page, items = self.cass_write_queue.get(True, 3)
            except Empty:
                if not self.fetch_on_going.is_set():
                    break
                else:
                    continue

            batch.add(self.conn.prep_page,
                      (page["page_code"], page["page_date"], page["page_name"], page["update_time"]))
            if len(batch) > CASS_BATCH_SIZE:
                self.conn.session.execute(batch)
                batch = BatchStatement()

            for item in items:

                pk = [item["repo_name"], item["author"], page["page_code"], page["page_date"]]
                batch.add(self.conn.prep_repo, (*pk, item["description"], item["lang"]))
                if len(batch) > CASS_BATCH_SIZE:
                    self.conn.session.execute(batch)
                    batch = BatchStatement()

                row = self.conn.session.execute(
                    """ SELECT * FROM record
                        WHERE repo_name=%s AND author=%s AND page_code=%s AND page_date=%s
                        LIMIT 1
                    """, pk).one()
                if row is None:
                    logging.warning(f"new record found {pk}")
                    batch.add(self.conn.prep_record, (*pk, 1, epoch, item["time"], item["time"], item["rank"]))
                elif row["rank"] == item["rank"]:  # stats not changed
                    batch.add(self.conn.prep_record,
                              (*pk, row["seq"], epoch, row["start"], item["time"], item["rank"]))
                elif row["rank"] != 0:  # stats changed
                    batch.add(self.conn.prep_record,
                              (*pk, row["seq"] + 1, epoch, row["end"], item["time"], item["rank"]))
                else:
                    logging.warning(f"item reappeared {pk}")
                    batch.add(self.conn.prep_record,
                              (*pk, row["seq"], epoch, row["start"], item["time"], 0))
                    batch.add(self.conn.prep_record,
                              (*pk, row["seq"] + 1, epoch, item["time"], item["time"], item["rank"]))
                if len(batch) > CASS_BATCH_SIZE:
                    self.conn.session.execute(batch)
                    batch = BatchStatement()

            key = (page["page_code"], page["page_date"])
            page_visited_key_set.add(key)
            self.redis_write_queue.put((key, items))

        self.conn.session.execute(batch)
        self.conn.batch_update_old_records(epoch, page_visited_key_set)

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
        self.conn.redis_pipeline.set("page_index_by_name", str(self.conn.select_page_index_by_name()))
        self.conn.redis_pipeline.execute()

        repo_or_author = self.conn.select_repo_or_author_index()
        for k, v in repo_or_author.items():
            self.conn.redis.hset("repo_or_author", k, str(list(v)))


if __name__ == '__main__':
    f = Fetcher()
    # f.store_to_cass()
    f.fetcher_routine()
