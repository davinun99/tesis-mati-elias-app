from django.urls import path, include
from rest_framework import routers
from django.urls import re_path

from . import views, viewsets

urlpatterns = [
	path('v1/release/<path:pk>/', viewsets.GetRelease.as_view()),
	path('v1/release/', viewsets.Releases.as_view()),
	path('v1/record/<path:pk>/', viewsets.GetRecord.as_view()),
	path('v1/record/', viewsets.Records.as_view()),
	path('v1/descargas/<path:pk>/', viewsets.Descargar.as_view()),
	path('v1/descargas/', viewsets.Descargas.as_view()),
	path('v1/', viewsets.PublicAPI.as_view()),

	path('record/<path:pk>/', viewsets.RecordDetail.as_view()),
	path('record/', viewsets.RecordAPIView.as_view()),

	path('inicio/', viewsets.Index.as_view()),
	path('buscador/', viewsets.Buscador.as_view()),

	path('compradores/', viewsets.Compradores.as_view()),
	path('compradores/<path:partieId>/procesos/', viewsets.ProcesosDelComprador.as_view()),
	path('compradores/<path:partieId>/contratos/', viewsets.ContratosDelComprador.as_view()),
	path('compradores/<path:partieId>/pagos/', viewsets.PagosDelComprador.as_view()),
	path('compradores/<path:partieId>/', viewsets.Comprador.as_view()),



]