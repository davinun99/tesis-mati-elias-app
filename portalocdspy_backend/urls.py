from django.urls import path, include
from rest_framework import routers
from django.urls import re_path

from . import views, viewsets

urlpatterns = [
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