## GTT - Github Trending Timeline

### Introduction

Github Trending Timeline (GTT) is a python project that aims to track the history of Github Trending pages, i.e. what rank a repository has at what time on which trending page.

This project not only showcases how Django would work with Cassandra and Redis, but also provides json APIs for current trending information and the history of a repository's trending. 

GTT is initiated and maintained by Haochen Roger Pan (phchcc@gmail.com). It is always open to your suggestions and feedback!

[GTT home page is here](https://www.githubtr.com/)

API usage see [here](https://github.com/haochenpan/Github-Trending-Timeline/blob/master/gtt/web_app/README.md)

### Technical Stack (software, hardware, and services relied on)
- Crawler Libraries: lxml, requests
- Storage Backend: Cassandra 3.11
- Caching Layer: Redis 5.0
- Web Backend: Django 2.2
- Virtual Machine: AWS EC2 (machine spec: t2.small with Ubuntu 18.04 LTS) 
- Load Balancing: AWS Application Load Balancer 
- CDN: AWS CloudFront; Routing: AWS Route 53, AWS S3

### Important Project Directories/Files
- gtt/gtt_main: Django settings
- gtt/web_app: Django templates, views, and routes
- conf.py: project-wide configurations and helper functions
- conn.py: Cassandra and Redis connectivity, url seed loading
- db_create.py: Cassandra Keyspace ("trending") creation
- db_truncate.py: Cassandra Keyspace ("trending") truncation
- fetch.py: Crawler routine, Cassandra & Redis threads routines
- requirements.txt: python libraries required by this project

### Deployment

- Clone this repository and install dependencies: `pip3 install -r requirements.txt`, preferably in a virtual environment
- Start Cassandra and Redis at the background (maybe you need `setup.sh` to install them)
- To build table (column family) schemas and load trending page seeds(urls), run: `python3 db_create.py` and `python3 conn.py`.
- to start the main crawler program in foreground, run: `python3 fetch.py`
- Alternatively, to run at the background, run `nohup python3 -u fetch.py &`
- to start the Django web backend, run `python3 manage.py runserver 0.0.0.0:8000` at folder `gtt`
- Again, alternatively, you can run it at the background by `nohup python3 -u manage.py runserver 0.0.0.0:8000 &`

### Related Projects
- [/vitalets/github-trending-repos](https://github.com/vitalets/github-trending-repos) open-sourced a Javascript program to grab Github Trending pages. 
Users need to subscribe to an issue on Github to receive new repo notifications through Github web notification or Github's email. 
Also, it only supports a limited set of languages.
- [/huchenme/github-trending-api](https://github.com/huchenme/github-trending-api) is written in TypeScript and provides trending API for many programming languages so as trending developers.
However, it does not support searching the history of a trending.
- [/ecrmnn/trending-github](https://github.com/ecrmnn/trending-github) is a npm package that fetches trending information on the fly.

### Discussion
This project provides sufficient API of Github Trending pages of all languages, which can play well with Github native API that provides star and fork information of a repo of your interest.
The novelty of this project is that it records the history of Trending pages so users can see when a repo was on trending (of what language), for how long, and how the rank is increased/decreased in that interval.
For a repository owner, GTT can be a proof of his/her repo was on Trending :) . For a data analyst, GTT tells when and how well people appreciate a repo.

### Future Work
- Start/Stop bash script with failure detection
- Go distributed in all layers: Cassandra, Redis, Django, and VM
- Log-in Views
- Better search functionality
- trending developers

