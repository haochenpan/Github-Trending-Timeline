## GTT - Github Trending Timeline

Github Trending Timeline is a python project initiated and maintained by Haochen Pan (phchcc@gmail.com).

### Important Project Directory
- gtt/web_app: Django templates, views, and routes
- conf.py: project-wide configurations and helper functions
- conn.py: Cassandra and Redis connectivity
- db_create.py: Cassandra Keyspace ("trending") creation
- db_truncate.py: Cassandra Keyspace ("trending") truncation
- fetch.py: Crawler routine, Cassandra & Redis threads routines
- requirements.txt: python libraries required by this project:  `pip3 install -r requirements.txt`

### AWS Configuration

### Previous Works

### Future Work
- Start/Stop bash script with failure detection
- Go distributed in all layers: Cassandra, Redis, Django, and VM
- Log-in Views
- Better search functionality

