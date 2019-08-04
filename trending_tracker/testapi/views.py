from django.shortcuts import render
from django.http import HttpResponse

from redis import Redis

r = Redis(host='localhost', port=6379, db=0)


def cli_index(request, pk="/"):
    selector = eval(r.get("page_index_by_name"))
    since = request.GET.get('since', 'daily')
    trending = eval(r.get(str((pk, since))))
    context = {"selector": selector,
               "trending": trending}
    return render(request, "index.html", context)


def cli_cards(request, pk="/"):
    since = request.GET.get('since', 'daily')
    trending = eval(r.get(str((pk, since))))
    context = {"trending": trending}
    return render(request, "cards.html", context)
