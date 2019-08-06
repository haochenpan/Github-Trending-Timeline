from conf import *
from conn import *
from fetch import *
from queue import Queue
from requests import Session
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, named_tuple_factory

# cass_session = Cluster().connect()
# cass_session.row_factory = dict_factory

fetcher = Fetcher()
page = {'page_code': '/', 'page_date': 'weekly', 'page_name': 'Any Language', 'update_time': 1565065327, 'url': 'https://github.com/trending//?since=weekly'}
fetcher.url_queue.put(page)
fetcher.thread_perform_fetch(Session())
fetcher.store_to_cass()
