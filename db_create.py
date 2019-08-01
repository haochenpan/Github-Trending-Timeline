from cassandra.cluster import Cluster

if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect()
    session.execute(
        """ DROP KEYSPACE IF EXISTS trending
        """
    )
    session.execute(
        """ CREATE KEYSPACE IF NOT EXISTS trending
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """
    )

    session.execute(
        """ DROP TABLE IF EXISTS trending.page
        """
    )

    session.execute(
        """ DROP TABLE IF EXISTS trending.repo
        """
    )

    session.execute(
        """ DROP TABLE IF EXISTS trending.record
        """
    )

    session.execute(
        """ CREATE TABLE IF NOT EXISTS trending.page (
            page_code text,
            page_date text,
            page_name text,
            update_time int,
            PRIMARY KEY (page_code, page_date)
            )
        """
    )

    session.execute(
        """ CREATE TABLE IF NOT EXISTS trending.repo (
            repo_name text,
            author text,
            description text,
            lang text,
            PRIMARY KEY (repo_name, author)
            )
        """
    )

    session.execute(
        """ CREATE TABLE IF NOT EXISTS trending.record (
            repo_name text,
            author text,
            page_code text,
            page_date text,
            time int,
            rank int,
            star int,
            fork int,
            PRIMARY KEY (repo_name, author, page_code, page_date)
            )
        """
    )

    rows = session.execute(
        """ SELECT * FROM system_schema.tables where keyspace_name='trending'
        """
    )

    for row in rows:
        print(row)
