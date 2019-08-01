from django.shortcuts import render
from django.http import HttpResponse

from redis import Redis

r = Redis(host='localhost', port=6379, db=0)


def api_index(request):
    index = r.get("index")
    return HttpResponse(index)


def api_index_by_time(request):
    index = r.get("index_by_time")
    return HttpResponse(index)


def cli_index(request):
    index = eval(r.get("index"))
    context = {"index": index}
    return render(request, "index.html", context)


def cli_index_by_time(request):
    index = eval(r.get("index_by_time"))
    context = {"index": index}
    return render(request, "index.html", context)


def api_trending(request, pk="/"):
    since = request.GET.get('since', 'daily')
    page = eval(r.get(str((pk, since))))
    return HttpResponse(page)


def cli_trending(request, pk="/"):
    since = request.GET.get('since', 'daily')
    page = eval(r.get(str((pk, since))))
    context = {"page": page, "lang": pk, "date": since}
    return render(request, "trending_page.html", context)
