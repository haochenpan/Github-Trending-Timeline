from django.urls import path
from . import views

urlpatterns = [
    path("", views.cli_index),
    path("trending/", views.cli_index),
    path("trending/<str:pk>/", views.cli_index),
]
