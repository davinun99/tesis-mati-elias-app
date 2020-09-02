from django.urls import path, include
from portalocdspy_frontend import views as frontend_views

urlpatterns = [
	path('', frontend_views.Inicio),
    path('proceso/<str:ocid>/', frontend_views.Proceso),
    path('proceso/', frontend_views.Proceso),
    path('servicio/', frontend_views.Api),
    path('manual_api/', frontend_views.ManualApi),
    path('acerca/', frontend_views.Acerca),
    path('comprador/<str:id>/', frontend_views.Comprador),
    path('comprador/', frontend_views.Comprador),
    path('compradores/', frontend_views.Compradores),
    path('busqueda/', frontend_views.Busqueda),
    path('descargas/', frontend_views.Descargas),
    path('preguntas/', frontend_views.Preguntas),
    path('visualizaciones/', frontend_views.Visualizaciones),
    path('tableroProcesosContratacion/', frontend_views.TableroProcesosContratacion),
    path('indicadoresProcesosContratacion/', frontend_views.IndicadoresProcesosContratacion),
]