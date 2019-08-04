from django.shortcuts import render
from django.http import HttpResponse

from redis import Redis

r = Redis(host='localhost', port=6379, db=0)


def cli_index(request, pk="/"):
    selector = eval(r.get("page_index_by_name"))
    since = request.GET.get('since', 'daily')
    print(pk, since)
    trending = eval(r.get(str((pk, since))))
    context = {"selector": selector, "trending": trending,
               "page": pk, "since": since}
    return render(request, "index.html", context)
