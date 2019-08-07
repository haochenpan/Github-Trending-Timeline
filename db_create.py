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
            page_code text,
            page_date text,
            description text,
            lang text,
            PRIMARY KEY ((repo_name, author), page_code, page_date)
            )
        """
    )

    session.execute(
        """ CREATE TABLE IF NOT EXISTS trending.record (
            repo_name text,
            author text,
            page_code text,
            page_date text,
            seq int,
            epoch int,
            start int,
            end int,
            rank int,
            PRIMARY KEY ((repo_name, author, page_code, page_date), seq) 
            ) WITH CLUSTERING ORDER BY (seq DESC)
        """
    )

    rows = session.execute(
        """ SELECT * FROM system_schema.tables where keyspace_name='trending'
        """
    )

    for row in rows:
        print(row)
