from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from redis import Redis
from json import dumps

r = Redis(host='localhost', port=6379, db=0)
c = Cluster().connect("trending")
c.row_factory = dict_factory


def cli_trending(request, pk="/"):
    selector = eval(r.get("page_index_by_name"))
    since = request.GET.get('since', 'daily')
    type = request.GET.get('type', '')
    trending = eval(r.get(str((pk, since))))
    if type == 'json':
        return HttpResponse(dumps(trending))
    else:
        context = {"selector": selector, "trending": trending,
                   "page": pk, "since": since}
        return render(request, "trending.html", context)


def cli_search_query(request):
    query = request.GET.get('q', '').lower()
    type = request.GET.get('type', '')
    if query != '':
        dicts = r.hget("repo_or_author", query)
        if dicts is None and type == 'json':
            return HttpResponse(dumps([]))
        elif dicts is None:
            return render(request, "search.html", dict())
        else:
            dicts = eval(dicts)
            if type == 'json':
                return HttpResponse(dumps(dicts))
            else:
                context = {"data": dicts}
                print(context)
                return render(request, "search.html", context)
    else:
        return render(request, "search.html", dict())


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
        readme = "https://github.com/haochenpan/Github-Trending-Timeline/blob/master/gtt/web_app/README.md"
        return HttpResponseRedirect(readme)


def cli_record_query(request):
    repo = request.GET.get('r', '')
    author = request.GET.get('a', '')
    code = request.GET.get('c', '')
    date = request.GET.get('d', '')
    pk = [repo, author, code, date]
    typ = request.GET.get('type', '')
    if repo == '' or author == '' or code == '' or date == '':
        return redirect("/search")
    else:
        context = {}
        repo = c.execute(
            """ SELECT * FROM repo
                WHERE repo_name=%s AND author=%s AND page_code=%s AND page_date=%s
                LIMIT 1
            """, pk
        ).one()
        if repo is None:
            return redirect("/search")

        for k, v in repo.items():
            context[k] = v

        rows = c.execute(
            """ SELECT * FROM record
                WHERE repo_name=%s AND author=%s AND page_code=%s AND page_date=%s
            """, pk
        )
        context["data"] = [row for row in rows]

        if typ == "json":
            return HttpResponse(dumps(context))
        else:
            return render(request, "record.html", context)


def cli_about(request):
    readme = "https://github.com/haochenpan/Github-Trending-Timeline/blob/master/README.md"
    return HttpResponseRedirect(readme)
