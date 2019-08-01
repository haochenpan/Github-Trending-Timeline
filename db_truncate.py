from cassandra.cluster import Cluster

if __name__ == '__main__':
    pass
    cluster = Cluster()
    session = cluster.connect()
    session.execute(
        "TRUNCATE TABLE trending.page"
    )
    session.execute(
        "TRUNCATE TABLE trending.repo"
    )
    session.execute(
        "TRUNCATE TABLE trending.record"
    )