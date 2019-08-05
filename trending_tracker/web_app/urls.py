from django.urls import path
from . import views

urlpatterns = [
    path("", views.cli_trending),
    path("trending/", views.cli_trending),
    path("trending/<str:pk>/", views.cli_trending),

    path("search/", views.cli_search),
    path("record/", views.cli_record),

    path("api/", views.cli_api),
    path("api/<str:pk>/", views.cli_api),

]
