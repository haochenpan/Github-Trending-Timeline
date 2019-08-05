# from conf import *
# from conn import *
# from queue import Queue
# from requests import Session
# from cassandra.query import BatchStatement
# from cassandra.cluster import Cluster
# from cassandra.query import dict_factory, named_tuple_factory
#
# cass_session = Cluster().connect()
# cass_session.row_factory = dict_factory
#
# prep_record = cass_session.prepare(
#     """ INSERT INTO trending.record (repo_name, author, page_code, page_date, seq, start, end, rank)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#     """
# )
#
#
# def read_modify_write(page, items):
#
#
#
# c = Conn()
# q = Queue()
# c.load_pages(q)
# pd = q.get()
# session = Session()
# resp = session.get(pd["url"])
# html = etree.HTML(resp.text)
# cnt, items = fetch_trending_page(html)
# read_modify_write(pd, items)
