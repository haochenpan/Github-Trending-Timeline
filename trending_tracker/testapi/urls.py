from django.urls import path
from . import views

urlpatterns = [
    path("", views.cli_index, name="cli_index"),
    path("tindex", views.cli_index_by_time, name="cli_index_by_time"),
    path("api/", views.api_index, name="api_index"),
    path("tapi/", views.api_index_by_time, name="api_index_by_time"),
    path("trending/", views.cli_trending, name="cli_trending"),
    path("trending/<str:pk>/", views.cli_trending, name="cli_trending"),
    path("api/trending/<str:pk>/", views.api_trending, name="api_trending"),
]
