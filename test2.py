if __name__ == '__main__':
    pass
    # TODO: think about distributed fetching and storing
    # TODO: exception detections
    # TODO: log in views

    # ALTER TABLE trending.page ADD is_fetching text;

    # season 0: major languages repos daily, log out view， cass + redis
    # season 0.5: all repos logout views, daily, weekly, and monthly, cass + redis
    # season 1: go online and go distributed
    # season 2: + all developers trending, daily, weekly, and monthly

    # time_index_by_name
    # time_index_by_time
    # page_index_by_name (used as index selector)
    # code, date tuple
    # repo_index hset
    # author_index hset
    # repo_author_index set
