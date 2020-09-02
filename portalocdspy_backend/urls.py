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

	path('dashboardoncae/filtros/', viewsets.FiltrosDashboardONCAE.as_view()),
	path('dashboardoncae/procesosporcategoria/', viewsets.GraficarProcesosPorCategorias.as_view()),
	path('dashboardoncae/procesospormodalidad/', viewsets.GraficarProcesosPorModalidad.as_view()),
	path('dashboardoncae/cantidaddeprocesos/', viewsets.GraficarCantidadDeProcesosMes.as_view()),
	path('dashboardoncae/estadisticacantidaddeprocesos/', viewsets.EstadisticaCantidadDeProcesos.as_view()),
	path('dashboardoncae/procesosporetapa/', viewsets.GraficarProcesosPorEtapa.as_view()),
	path('dashboardoncae/montosdecontratos/', viewsets.GraficarMontosDeContratosMes.as_view()),
	path('dashboardoncae/estadisticacantidaddecontratos/', viewsets.EstadisticaCantidadDeContratos.as_view()),
	path('dashboardoncae/estadisticamontosdecontratos/', viewsets.EstadisticaMontosDeContratos.as_view()),
	path('dashboardoncae/contratosporcategoria/', viewsets.GraficarContratosPorCategorias.as_view()),
	path('dashboardoncae/contratospormodalidad/', viewsets.GraficarContratosPorModalidad.as_view()),
	path('dashboardoncae/topcompradores/', viewsets.TopCompradoresPorMontoContratado.as_view()),
	path('dashboardoncae/topproveedores/', viewsets.TopProveedoresPorMontoContratado.as_view()),
	path('dashboardoncae/tiemposporetapa/', viewsets.GraficarProcesosTiposPromediosPorEtapa.as_view()),

	path('indicadoresoncae/filtros/', viewsets.FiltrosDashboardONCAE.as_view()),
	path('indicadoresoncae/montoporcategoria/', viewsets.IndicadorMontoContratadoPorCategoria.as_view()),
	path('indicadoresoncae/cantidadcontratosporcategoria/', viewsets.IndicadorCantidadProcesosPorCategoria.as_view()),
	path('indicadoresoncae/topcompradores/', viewsets.IndicadorTopCompradores.as_view()),
	path('indicadoresoncae/catalogos/', viewsets.IndicadorCatalogoElectronico.as_view()),
	path('indicadoresoncae/comprasconjuntas/', viewsets.IndicadorCompraConjunta.as_view()),
	path('indicadoresoncae/contratospormodalidad/', viewsets.IndicadorContratosPorModalidad.as_view()),

	path('visualizacionesoncae/filtros/', viewsets.FiltrosVisualizacionesONCAE.as_view()),
	path('visualizacionesoncae/instituciones/', viewsets.CompradoresPorCantidadDeContratos.as_view()),

]