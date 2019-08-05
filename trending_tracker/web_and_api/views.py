from django.shortcuts import render, redirect
from django.http import HttpResponse
from cassandra.cluster import Cluster
from redis import Redis

r = Redis(host='localhost', port=6379, db=0)
c = Cluster().connect()  # named_tuple_factory


def cli_trending(request, pk="/"):
    selector = eval(r.get("page_index_by_name"))
    since = request.GET.get('since', 'daily')
    print(str((pk, since)))
    trending = eval(r.get(str((pk, since))))
    context = {"selector": selector, "trending": trending,
               "page": pk, "since": since}
    return render(request, "trending.html", context)


def cli_api(request, pk="name"):
    if pk == "name":
        index = eval(r.get("time_index_by_name"))
        context = {"index": index}
        return render(request, "api.html", context)
    elif pk == "time":
        index = eval(r.get("time_index_by_time"))
        context = {"index": index}
        return render(request, "api.html", context)
    else:
        context = {}
        return render(request, "api_info.html", context)


def cli_search(request):
    pass
    repo = request.GET.get('r', '')
    author = request.GET.get('a', '')
    data_pairs = []
    if repo != '' and author != '':
        pair = (repo, author)
        if r.sismember("repo_author_index", str(pair)):
            data_pairs = [(author, repo)]
    elif repo != '' and author == '':
        authors = r.hget("repo_index", repo)
        if authors is not None:
            authors = eval(authors)
            data_pairs = [(author, repo) for author in authors]
    elif repo == '' and author != '':
        repos = r.hget("author_index", author)
        if repos is not None:
            repos = eval(repos)
            data_pairs = [(author, repo) for repo in repos]
    context = {"data": data_pairs}
    return render(request, "search.html", context)


def cli_record(request):
    repo = request.GET.get('r', '')
    author = request.GET.get('a', '')
    pair = (repo, author)

    if repo == '' or author == '':
        return redirect("/search")

    elif r.sismember("repo_author_index", str(pair)):
        rows = c.execute(
            """ SELECT * FROM trending.record where repo_name=%s and author=%s;
            """, [repo, author]
        )
        context = {"data": [row for row in rows]}
        rows = c.execute(
            """ SELECT * FROM trending.repo where repo_name=%s and author=%s LIMIT 1;
            """, [repo, author]
        )
        row = rows.one()
        context["info_name"] = row[0]
        context["info_auth"] = row[1]
        context["info_desc"] = row[2]
        context["info_lang"] = row[3]
        return render(request, "record.html", context)
    else:
        return redirect("/search")
