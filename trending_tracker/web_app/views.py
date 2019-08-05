from django.shortcuts import render, redirect
from django.http import HttpResponse
from cassandra.cluster import Cluster
from cassandra.query import dict_factory, named_tuple_factory
from redis import Redis
from json import loads, dumps

r = Redis(host='localhost', port=6379, db=0)
c = Cluster().connect("trending")
c.row_factory = dict_factory


def cli_trending(request, pk="/"):
    selector = eval(r.get("page_index_by_name"))
    since = request.GET.get('since', 'daily')
    trending = eval(r.get(str((pk, since))))
    context = {"selector": selector, "trending": trending,
               "page": pk, "since": since}
    return render(request, "trending.html", context)


def cli_search(request):
    repo = request.GET.get('r', '')
    author = request.GET.get('a', '')
    type = request.GET.get('type', '')
    data_pairs = []
    if repo != '' and author != '':
        pair = (repo, author)
        if r.sismember("repo_author_index", str(pair)):
            data_pairs.append(pair)
    elif repo != '' and author == '':
        authors = r.hget("repo_index", repo)
        if authors is not None:
            authors = eval(authors)
            data_pairs.extend([(repo, author) for author in authors])
    elif repo == '' and author != '':
        repos = r.hget("author_index", author)
        if repos is not None:
            repos = eval(repos)
            print("repos", repos)
            data_pairs.extend([(repo, author) for repo in repos])
        else:
            print("is None")
    if type == "json":
        return HttpResponse(dumps(data_pairs))
    else:
        context = {"data": data_pairs}
        return render(request, "search.html", context)


def cli_api(request, pk="name"):
    typ = request.GET.get('type', '')
    if pk == "name":
        index = eval(r.get("time_index_by_name"))
        if typ == "json":
            return HttpResponse(dumps(index))
        else:
            context = {"index": index}
            return render(request, "api.html", context)
    elif pk == "time":
        index = eval(r.get("time_index_by_time"))
        if typ == "json":
            return HttpResponse(dumps(index))
        else:
            context = {"index": index}
            return render(request, "api.html", context)
    else:
        context = {}
        return render(request, "api_info.html", context)


def cli_record(request):
    repo = request.GET.get('r', '')
    author = request.GET.get('a', '')
    typ = request.GET.get('type', '')

    pair = (repo, author)

    if repo == '' or author == '':
        return redirect("/search")

    elif r.sismember("repo_author_index", str(pair)):
        rows = c.execute(
            """ SELECT * FROM trending.record where repo_name=%s and author=%s;
            """, [repo, author]
        )
        context = {"data": [row for row in rows]}
        # context = {}
        rows = c.execute(
            """ SELECT * FROM repo where repo_name=%s and author=%s LIMIT 1;
            """, [repo, author]
        )
        row = rows.one()
        for k, v in row.items():
            context[k] = v
        if typ == "json":
            return HttpResponse(dumps(context))
        else:
            return render(request, "record.html", context)
    else:
        return redirect("/search")
