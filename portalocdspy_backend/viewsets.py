from django.shortcuts import get_object_or_404
from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework.decorators import detail_route
from rest_framework.views import APIView
from rest_framework import pagination
from rest_framework import status
from django.db import connections
from django.db.models import Avg, Count, Min, Sum
from decimal import Decimal 
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from .serializers import *
from .functions import *
from .pagination import PaginationHandlerMixin
from django.core.paginator import Paginator, Page, EmptyPage, PageNotAnInteger
from urllib.request import urlretrieve
import json, copy, urllib.parse, datetime, operator, statistics, csv
import pandas as pd 
import mimetypes, os.path, math

from django_elasticsearch_dsl_drf.viewsets import DocumentViewSet
from portalocdspy_backend import documents as articles_documents
from portalocdspy_backend import serializers as articles_serializers

from django.utils.functional import LazyObject
from django.conf import settings
from django.http import Http404, StreamingHttpResponse, HttpResponse, HttpResponseServerError
from itertools import chain

import ocds_bulk_download


OCDS_INDEX = 'ocds'
CONTRACT_INDEX = 'contracts'
TRANSACTION_INDEX = 'transactions'

def ElasticSearchDefaultConnection():
	url = settings.ELASTICSEARCH_DSL_HOST
	usuario = settings.ELASTICSEARCH_USER
	contrasena = settings.ELASTICSEARCH_PASS
	tiempo = settings.ELASTICSEARCH_TIMEOUT
	cliente = Elasticsearch(url, timeout=tiempo, http_auth=(usuario, contrasena))

	return cliente

class BasicPagination(pagination.PageNumberPagination):
    page_size_query_param = 'limit'

class SearchResults(LazyObject):
    def __init__(self, search_object):
        self._wrapped = search_object

    def __len__(self):
        return self._wrapped.count()

    def __getitem__(self, index):
        search_results = self._wrapped[index]
        if isinstance(index, slice):
            search_results = list(search_results)
        return search_results

# API releases y records

class PublicAPI(APIView, PaginationHandlerMixin):

	def get(self, request, format=None, *args, **kwargs):
		urlAPI = '/api/v1/'

		endpoints = {}
		endpoints["release"] =  request.build_absolute_uri(urlAPI + "release/")
		endpoints["record"] =  request.build_absolute_uri(urlAPI + "record/")

		return Response(endpoints)

class Releases(APIView, PaginationHandlerMixin):
	pagination_class = BasicPagination
	serializer_class = ReleaseSerializer

	def get(self, request, format=None, *args, **kwargs):
		
		respuesta = {}
		currentPage = request.GET.get('page', "1")
		publisher = request.GET.get('publisher', "")
		oncae = 'Oficina Normativa de Contratación y Adquisiciones del Estado (ONCAE) / Honduras'
		sefin = 'Secretaria de Finanzas de Honduras'

		if publisher == 'oncae':
			instance = Release.objects.filter(package_data__data__publisher__name=oncae)
		elif publisher == 'sefin':
			instance = Release.objects.filter(package_data__data__publisher__name=sefin)
		else:
			instance = Release.objects.all()

		page = self.paginate_queryset(instance)

		if page is not None:
			serializer = self.get_paginated_response(self.serializer_class(page, many=True).data)
		else:
			content = {'error': 'Internal Server Error'}
			return Response(content, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

		results = []
		paquetesIds = {}

		paquetesIds = set()

		for d in serializer.data["results"]:
			results.append(d["data"])

			if d["package_data_id"] not in paquetesIds:
				paquetesIds.add(d["package_data_id"])

		paquetes = PackageData.objects.filter(id__in=list(paquetesIds))

		metadataPaquete = generarMetaDatosPaquete(paquetes, request)

		respuesta["releases"] = serializer.data["count"]
		respuesta["pages"] = math.ceil(serializer.data["count"] / 10)
		respuesta["page"] = currentPage
		respuesta["next"] = serializer.data["next"]
		respuesta["previous"] = serializer.data["previous"]
		if serializer.data["count"] > 0:
			respuesta["releasePackage"] = metadataPaquete
			respuesta["releasePackage"]["releases"] = results
		else:
			respuesta["releasePackage"] = {}


		return Response(respuesta, status=status.HTTP_200_OK)

class GetRelease(APIView):
	def get(self, request, pk=None, format=None):
		queryset = Release.objects.filter(release_id=pk)

		if queryset.exists():
			release = queryset[0]
			serializer = ReleaseSerializer(release)

			data = serializer.data
			paquetes = PackageData.objects.filter(id__in=[data["package_data_id"],])
			releasePackage = generarMetaDatosPaquete(paquetes, request)
			
			releasePackage["releases"] = [data["data"],]

			return Response(releasePackage)
		else:
			raise Http404

class Records(APIView, PaginationHandlerMixin):
	pagination_class = BasicPagination
	serializer_class = RecordSerializer

	def get(self, request, format=None, *args, **kwargs):
		
		respuesta = {}
		currentPage = request.GET.get('page', "1")

		instance = Record.objects.all()

		page = self.paginate_queryset(instance)

		if page is not None:
			serializer = self.get_paginated_response(self.serializer_class(page, many=True).data)
		else:
			content = {'error': 'Internal Server Error'}
			return Response(content, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

		results = []
		paquetes = []

		results = serializer.data["results"]

		for d in results:
			for r in d["releases"]:
				release = Release.objects.filter(release_id=r["id"])
				paquete = release[0].package_data
				paquetes.append(paquete)

		metadataPaquete = paqueteRegistros(paquetes, request)

		respuesta["records"] = serializer.data["count"]
		respuesta["pages"] = math.ceil(serializer.data["count"] / 10)
		respuesta["page"] = currentPage
		respuesta["next"] = serializer.data["next"]
		respuesta["previous"] = serializer.data["previous"]

		if serializer.data["count"] > 0:
			respuesta["recordPackage"] = metadataPaquete
			respuesta["recordPackage"]["records"] = results
		else:
			respuesta["recordPackage"] = {}

		return Response(respuesta, status=status.HTTP_200_OK)

class GetRecord(APIView):

	def get(self, request, pk=None, format=None):
		queryset = Record.objects.filter(ocid=pk)

		if queryset.exists():
			record = queryset[0]
			serializer = RecordSerializer(record)

			data = serializer.data

			paquetes = []

			for r in data["releases"]:
				release = Release.objects.filter(release_id=r["id"])
				paquete = release[0].package_data
				paquetes.append(paquete)

			recordPackage = paqueteRegistros(paquetes, request)
			
			recordPackage["records"] = [data,]

			return Response(recordPackage)
		else:
			raise Http404

# Buscador de contrataciones. 

class Index(APIView):

	def get(self, request, format=None):

		precision = 40000
		sourceSEFIN = 'HN.SIAFI2'

		cliente = ElasticSearchDefaultConnection()

		oncae = Search(using=cliente, index=OCDS_INDEX)
		sefin = Search(using=cliente, index=OCDS_INDEX)
		todos = Search(using=cliente, index=OCDS_INDEX)
		sp = Search(using=cliente, index=OCDS_INDEX)

		oncae = oncae.exclude('match_phrase', doc__compiledRelease__sources__id=sourceSEFIN)

		sefin = sefin.filter('match_phrase', doc__compiledRelease__sources__id=sourceSEFIN)

		oncae.aggs.metric(
			'contratos', 
			'nested', 
			path='doc.compiledRelease.contracts'
		)

		sefin.aggs.metric(
			'contratos', 
			'nested', 
			path='doc.compiledRelease.contracts'
		)

		sefin.aggs.metric(
			'procesos_pagos', 
			'value_count', 
			field='doc.compiledRelease.ocid.keyword'
		)

		todos.aggs.metric(
			'contratos', 
			'nested', 
			path='doc.compiledRelease.contracts'
		)

		# todos.aggs["contratos"].metric(
		# 	'distinct_suppliers', 
		# 	'cardinality', 
		# 	precision_threshold=precision, 
		# 	field='doc.compiledRelease.contracts.suppliers.id.keyword'
		# )
		
		todos.aggs.metric(
			'distinct_buyers', 
			'cardinality', 
			precision_threshold=precision, 
			field='doc.compiledRelease.buyer.id.keyword' 
		)
		
		oncae.aggs["contratos"].metric(
			'distinct_contracts', 
			'cardinality',
			precision_threshold=precision, 
			field='doc.compiledRelease.contracts.id.keyword'
		)
		
		oncae.aggs.metric(
			'procesos_contratacion', 
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		#Proveedores terms
		oncae.aggs["contratos"].metric(
			'proveedores_oncae',
			'terms',
			field='doc.compiledRelease.contracts.suppliers.id.keyword',
			size=100000
		)

		sefin.aggs["contratos"].metric(
			'proveedores_sefin',
			'terms',
			field='doc.compiledRelease.contracts.implementation.transactions.payee.id.keyword',
			size=100000
		)

		resultsONCAE = oncae.execute()
		resultsSEFIN = sefin.execute()
		resultsTODOS = todos.execute()

		diccionario_proveedores = []
		dfProveedores = pd.DataFrame(resultsSEFIN.aggregations.contratos.proveedores_sefin.to_dict()["buckets"])
		
		if resultsONCAE.aggregations.contratos.proveedores_oncae.to_dict()["buckets"]:
			dfProveedores = dfProveedores.append(resultsONCAE.aggregations.contratos.proveedores_oncae.to_dict()["buckets"])
		
		if not dfProveedores.empty:
			cantidad_proveedores = dfProveedores['key'].nunique()
		else:
			cantidad_proveedores = 0

		# dfProveedores.to_csv(r'proveedores.csv', sep='\t', encoding='utf-8')

		context = {
			# "elasticsearch": cantidad_proveedores,
			"contratos": resultsONCAE.aggregations.contratos.distinct_contracts.value,
			"procesos": resultsONCAE.aggregations.procesos_contratacion.value,
			"pagos": resultsSEFIN.aggregations.procesos_pagos.value,
			"compradores": resultsTODOS.aggregations.distinct_buyers.value,
			"proveedores": cantidad_proveedores
		}

		return Response(context)

class Buscador(APIView):

	def get(self, request, format=None):
		precision = 40000
		sourceSEFIN = 'HN.SIAFI2'
		noMoneda = 'Sin monto de contrato'
		noMonedaPago = 'Sin monto pagado'

		page = int(request.GET.get('pagina', '1'))
		metodo = request.GET.get('metodo', 'proceso')
		moneda = request.GET.get('moneda', '')
		metodo_seleccion = request.GET.get('metodo_seleccion', '')
		institucion = request.GET.get('institucion', '')
		categoria = request.GET.get('categoria', '')
		year = request.GET.get('year', '')
		organismo = request.GET.get('organismo', '')

		ordenarPor = request.GET.get('ordenarPor','')

		term = request.GET.get('term', '')
		start = (page-1) * settings.PAGINATE_BY
		end = start + settings.PAGINATE_BY

		if metodo not in ['proceso', 'contrato', 'pago']:
			metodo = 'proceso'

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		#Source
		campos = ['doc.compiledRelease', 'extra']
		s = s.source(campos)

		#Filtros
		s.aggs.metric('contratos', 'nested', path='doc.compiledRelease.contracts')

		s.aggs["contratos"].metric('monedas', 'terms', field='doc.compiledRelease.contracts.value.currency.keyword')

		s.aggs["contratos"]["monedas"].metric("nProcesos", "reverse_nested")

		s.aggs.metric('metodos_de_seleccion', 'terms', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')

		s.aggs.metric('instituciones', 'terms', field='extra.parentTop.name.keyword', size=10000)

		s.aggs.metric('categorias', 'terms', field='doc.compiledRelease.tender.localProcurementCategory.keyword')

		s.aggs.metric('organismosFinanciadores', 'terms', field='doc.compiledRelease.planning.budget.budgetBreakdown.classifications.organismo.keyword', size=2000)

		if metodo == 'pago' or metodo == 'contrato':
			s.aggs.metric('años', 'date_histogram', field='doc.compiledRelease.date', interval='year', format='yyyy', min_doc_count=1)
		else:
			s.aggs.metric('años', 'date_histogram', field='doc.compiledRelease.tender.tenderPeriod.startDate', interval='year', format='yyyy', min_doc_count=1)

		#resumen
		s.aggs["contratos"].metric(
			'promedio_montos_contrato', 
			'avg', 
			field='doc.compiledRelease.contracts.value.amount'
		)

		s.aggs["contratos"].metric(
			'promedio_montos_pago', 
			'avg', 
			field='doc.compiledRelease.contracts.implementation.transactions.value.amount'
		)

		s.aggs["contratos"].metric(
			'distinct_proveedores_contratos', 
			'cardinality', 
			precision_threshold=precision, 
			field='doc.compiledRelease.contracts.suppliers.id.keyword'
		)

		s.aggs["contratos"].metric(
			'distinct_proveedores_pagos', 
			'cardinality', 
			precision_threshold=precision, 
			field='doc.compiledRelease.contracts.implementation.transactions.payee.id.keyword'
		)

		s.aggs.metric(
			'compradores_total', 
			'cardinality', 
			precision_threshold=precision, 
			field='doc.compiledRelease.buyer.id.keyword'
		)

		if metodo == 'proceso':
			s = s.filter('exists', field='doc.compiledRelease.tender.id')

			# Temporal
			s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

			s.aggs.metric(
				'procesos_total', 
				'value_count', 
				field='doc.compiledRelease.ocid.keyword'
			)

		if metodo == 'contrato':
			filtro_contrato = Q('exists', field='doc.compiledRelease.contracts.id')
			s = s.exclude('match_phrase', doc__compiledRelease__sources__id=sourceSEFIN)
			s = s.query('nested', path='doc.compiledRelease.contracts', query=filtro_contrato)

			s.aggs["contratos"].metric(
				'procesos_total', 
				'cardinality', 
				precision_threshold=precision, 
				field='doc.compiledRelease.contracts.id.keyword'
			)

		if metodo == 'pago':
			s = s.filter('match_phrase', doc__compiledRelease__sources__id=sourceSEFIN)

			s.aggs.metric(
				'procesos_total', 
				'value_count', 
				field='doc.compiledRelease.ocid.keyword'
			)

		if moneda.replace(' ', ''): 
			if urllib.parse.unquote(moneda) == noMoneda or urllib.parse.unquote(moneda) == noMonedaPago:
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)				
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if metodo_seleccion.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails=metodo_seleccion)

		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if categoria.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory=categoria)

		if year.replace(' ', ''):
			if metodo == 'pago' or metodo == 'contrato':
				s = s.filter('range', doc__compiledRelease__date={'gte': datetime.date(int(year), 1, 1), 'lt': datetime.date(int(year)+1, 1, 1)})
			else:
				s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(year), 1, 1), 'lt': datetime.date(int(year)+1, 1, 1)})

		if term: 
			if metodo == 'proceso':
				s = s.filter('match', doc__compiledRelease__tender__description=term)

			if metodo in  ['contrato', 'pago']:
				qDescripcion = Q("wildcard", doc__compiledRelease__contracts__description='*'+term+'*')
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qDescripcion)

		if organismo.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__planning__budget__budgetBreakdown__classifications__organismo=organismo)

		search_results = SearchResults(s)

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		mappingSort = {
			"year":"doc.compiledRelease.date",
			"institucion":"doc.compiledRelease.buyer.name.keyword",
			"categoria": "doc.compiledRelease.tender.localProcurementCategory.keyword",
			"modalidad": "doc.compiledRelease.tender.procurementMethodDetails.keyword",
			"proveedor": "doc.compiledRelease.contracts.implementation.transactions.payee.name.keyword" if metodo == 'pago' else 'doc.compiledRelease.contracts.suppliers.name.keyword',
			"monto": "doc.compiledRelease.contracts.extra.sumTransactions" if metodo == 'pago' else 'doc.compiledRelease.contracts.value.amount',
			"organismo":"doc.compiledRelease.planning.budget.budgetBreakdown.classifications.organismo.keyword",
		}

		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					if columna in ('proveedor', 'monto'):
						ordenarES[mappingSort[columna]] = {
							"order": orden, 
							'nested':{
								'path':'doc.compiledRelease.contracts'
							}
						}
					else:
						ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		results = s[start:end].execute()

		monedas = results.aggregations.contratos.monedas.buckets

		if results.hits.total > 0:

			if metodo == 'proceso' or metodo == 'pago':
				conMoneda = 0
				for m in monedas:
					m["doc_count"] = m["nProcesos"]["doc_count"]
					conMoneda += m["nProcesos"]["doc_count"]

				sinMoneda = results.hits.total - conMoneda

				if sinMoneda > 0:
					keyMoneda = noMoneda
					
					if metodo == 'pago':
						keyMoneda = noMonedaPago

					monedas.append({"key": keyMoneda, "doc_count":sinMoneda})

		paginator = Paginator(search_results, settings.PAGINATE_BY)

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		filtros = {}
		filtros["monedas"] = results.aggregations.contratos.monedas.to_dict()
		filtros["años"] = results.aggregations.años.to_dict()
		filtros["categorias"] = results.aggregations.categorias.to_dict()
		filtros["instituciones"] = results.aggregations.instituciones.to_dict()
		filtros["metodos_de_seleccion"] = results.aggregations.metodos_de_seleccion.to_dict()
		filtros["organismosFinanciadores"] = results.aggregations.organismosFinanciadores.to_dict()

		total_compradores = results.aggregations.compradores_total.value

		if metodo == 'pago':
			monto_promedio = results.aggregations.contratos.promedio_montos_pago.value
			total_procesos = results.aggregations.procesos_total.value
			total_proveedores = results.aggregations.contratos.distinct_proveedores_pagos.value
		elif metodo == 'contrato':
			monto_promedio = results.aggregations.contratos.promedio_montos_contrato.value
			total_procesos = results.aggregations.contratos.procesos_total.value
			total_proveedores = results.aggregations.contratos.distinct_proveedores_contratos.value
		elif metodo == 'proceso':
			monto_promedio = results.aggregations.contratos.promedio_montos_contrato.value
			total_procesos = results.aggregations.procesos_total.value
			total_proveedores = results.aggregations.contratos.distinct_proveedores_contratos.value
		else:
			monto_promedio = 0
			total_procesos = 0
			total_proveedores = 0 

		resumen = {}
		resumen["proveedores_total"] = total_proveedores
		resumen["compradores_total"] = total_compradores
		resumen["procesos_total"] = total_procesos
		resumen["monto_promedio"] = monto_promedio

		parametros = {}
		parametros["term"] = term
		parametros["metodo"] = metodo
		parametros["pagina"] = page
		parametros["moneda"] = moneda
		parametros["metodo_seleccion"] = metodo_seleccion
		parametros["institucion"] = institucion
		parametros["categoria"] = categoria
		parametros["year"] = year
		parametros["organismo"] = organismo
		parametros["ordenarPor"] = ordenarPor

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resumen": resumen,
			"filtros": filtros,
			"resultados": results.hits.hits
			# "agregados": results.aggregations.to_dict(),
		}

		return Response(context)

class RecordAPIView(APIView):

	def get(self, request, format=None):
		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)
		results = s[0:10].execute()

		context = results.hits.hits

		return Response(context)

class RecordDetail(APIView):

	def get(self, request, pk=None, format=None):
		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)
		s = s.filter('match_phrase', doc__ocid__keyword=pk)

		results = s[0:1].execute()

		context = results.hits.hits

		if context:
			response = context[0]["_source"]["doc"]
			return Response(response)
		else:
			raise Http404

# Proveedores

class FiltroAniosProveedoresSEFIN(APIView):

	def get(self, request, format=None):

		cliente = ElasticSearchDefaultConnection()

		ss = Search(using=cliente, index=TRANSACTION_INDEX)

		ss.aggs.metric(
			'anios', 
			'date_histogram', 
			field='date', 
			interval='year', 
			format='yyyy'
		)

		resultsYears = ss.execute()

		yearsDict = resultsYears.aggregations.anios.to_dict()["buckets"]

		respuesta = []

		if yearsDict:
			for y in yearsDict:
				respuesta.append({"key_as_string": y["key_as_string"]})

		context = {"respuesta": respuesta}

		return Response(context)

class Proveedores(APIView):

	def get(self, request, format=None):
		page = int(request.GET.get('pagina', '1'))
		metodo = request.GET.get('metodo', None)
		nombre = request.GET.get('nombre', '')
		identificacion = request.GET.get('identificacion', '')
		procesos = request.GET.get('procesos', None)
		total = request.GET.get('total', None)
		promedio = request.GET.get('promedio', None)
		maximo = request.GET.get('maximo', None)
		minino = request.GET.get('minimo', None)
		term = request.GET.get('term', '')
		tmc = request.GET.get('tmc', '')
		pmc = request.GET.get('pmc', '')
		mamc = request.GET.get('mamc', '')
		memc = request.GET.get('memc', '')
		fua = request.GET.get('fua', '')
		cp = request.GET.get('cp', '')

		ordenarPor = request.GET.get('ordenarPor', '')
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index='supplier')

		s = s.filter("term", publicador__keyword='ONCAE')

		campos = [
			'id', 
			'name', 
			'procesos', 
			'total_monto_contratado', 
			'promedio_monto_contratado', 
			'mayor_monto_contratado', 
			'menor_monto_contratado', 
			'fecha_ultimo_proceso'
		]

		s = s.source(campos)

		filtros = []

		if nombre.replace(' ',''):
			filtro = Q("match", name=nombre)
			filtros.append(filtro)

		if identificacion.replace(' ',''):
			q_id = '*' + identificacion + '*'
			filtro = Q("wildcard", id__keyword=q_id)
			filtros.append(filtro)

		if tmc.replace(' ', ''):
			validarMonto = getOperator(tmc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', total_monto_contratado=valor)
				elif operador == "<":
					filtro = Q('range', total_monto_contratado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', total_monto_contratado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', total_monto_contratado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', total_monto_contratado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if pmc.replace(' ', ''):
			validarMonto = getOperator(pmc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', promedio_monto_contratado=valor)
				elif operador == "<":
					filtro = Q('range', promedio_monto_contratado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', promedio_monto_contratado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', promedio_monto_contratado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', promedio_monto_contratado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if mamc.replace(' ', ''):
			validarMonto = getOperator(mamc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', mayor_monto_contratado=valor)
				elif operador == "<":
					filtro = Q('range', mayor_monto_contratado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', mayor_monto_contratado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', mayor_monto_contratado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', mayor_monto_contratado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if memc.replace(' ', ''):
			validarMonto = getOperator(memc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', menor_monto_contratado=valor)
				elif operador == "<":
					filtro = Q('range', menor_monto_contratado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', menor_monto_contratado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', menor_monto_contratado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', menor_monto_contratado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if cp.replace(' ', ''):
			validarMonto = getOperator(cp)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', procesos=valor)
				elif operador == "<":
					filtro = Q('range', procesos={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', procesos={'lte': valor})
				elif operador == ">":
					filtro = Q('range', procesos={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', procesos={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fua.replace(' ', ''):
			validarFecha = getDateParam(fua)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', fecha_ultimo_proceso=valor)
				elif operador == "<":
					filtro = Q('range', fecha_ultimo_proceso={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', fecha_ultimo_proceso={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', fecha_ultimo_proceso={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', fecha_ultimo_proceso={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		mappingSort = {
			"id": "id.keyword",
			"name": "name.keyword",
			"procesos": "procesos",
			"total_monto_contratado": "total_monto_contratado",
			"promedio_monto_contratado": "promedio_monto_contratado",
			"mayor_monto_contratado": "mayor_monto_contratado",
			"menor_monto_contratado": "menor_monto_contratado",
			"fecha_ultimo_proceso": "fecha_ultimo_proceso",
		}

		if not ordenarPor:
			ordenarPor = 'asc(name)'

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)

		results = s[start:end].execute()
		paginator = Paginator(search_results, paginarPor)

		proveedores =  [hit["_source"] for hit in results.hits.hits]

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		parametros = {}
		parametros["pagina"] = page
		parametros["nombre"] = nombre
		parametros["identificacion"] = identificacion
		parametros["tmc"] = tmc 
		parametros["pmc"] = pmc 
		parametros["mamc"] = mamc 
		parametros["memc"] = memc 
		parametros["fua"] = fua 
		parametros["cp"] = cp
		parametros["orderBy"] = ordenarPor
		parametros["paginarPor"] = paginarPor

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": proveedores
		}

		return Response(context)

class ProveedoresSEFIN(APIView):

	def get(self, request, format=None):
		page = int(request.GET.get('pagina', '1'))
		metodo = request.GET.get('metodo', None)
		nombre = request.GET.get('nombre', '')
		identificacion = request.GET.get('identificacion', '')
		procesos = request.GET.get('procesos', None)
		total = request.GET.get('total', None)
		promedio = request.GET.get('promedio', None)
		maximo = request.GET.get('maximo', None)
		minino = request.GET.get('minimo', None)
		term = request.GET.get('term', '')
		tmc = request.GET.get('tmc', '')
		pmc = request.GET.get('pmc', '')
		mamc = request.GET.get('mamc', '')
		memc = request.GET.get('memc', '')
		fua = request.GET.get('fua', '')
		cp = request.GET.get('cp', '')
		ordenarPor = request.GET.get('ordenarPor', '')
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index='supplier')

		s = s.filter("term", publicador__keyword='SIAFI')

		campos = [
			'id', 
			'name', 
			'procesos', 
			'total_monto_pagado', 
			'promedio_monto_pagado', 
			'mayor_monto_pagado', 
			'menor_monto_pagado', 
			'fecha_ultimo_proceso'
		]

		s = s.source(campos)

		filtros = []

		if nombre.replace(' ',''):
			filtro = Q("match", name=nombre)
			filtros.append(filtro)

		if identificacion.replace(' ',''):
			q_id = '*' + identificacion + '*'
			filtro = Q("wildcard", id__keyword=q_id)
			filtros.append(filtro)

		if tmc.replace(' ', ''):
			validarMonto = getOperator(tmc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', total_monto_pagado=valor)
				elif operador == "<":
					filtro = Q('range', total_monto_pagado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', total_monto_pagado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', total_monto_pagado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', total_monto_pagado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if pmc.replace(' ', ''):
			validarMonto = getOperator(pmc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', promedio_monto_pagado=valor)
				elif operador == "<":
					filtro = Q('range', promedio_monto_pagado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', promedio_monto_pagado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', promedio_monto_pagado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', promedio_monto_pagado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if mamc.replace(' ', ''):
			validarMonto = getOperator(mamc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', mayor_monto_pagado=valor)
				elif operador == "<":
					filtro = Q('range', mayor_monto_pagado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', mayor_monto_pagado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', mayor_monto_pagado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', mayor_monto_pagado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if memc.replace(' ', ''):
			validarMonto = getOperator(memc)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', menor_monto_pagado=valor)
				elif operador == "<":
					filtro = Q('range', menor_monto_pagado={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', menor_monto_pagado={'lte': valor})
				elif operador == ">":
					filtro = Q('range', menor_monto_pagado={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', menor_monto_pagado={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if cp.replace(' ', ''):
			validarMonto = getOperator(cp)

			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', procesos=valor)
				elif operador == "<":
					filtro = Q('range', procesos={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', procesos={'lte': valor})
				elif operador == ">":
					filtro = Q('range', procesos={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', procesos={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fua.replace(' ', ''):
			validarFecha = getDateParam(fua)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', fecha_ultimo_proceso=valor)
				elif operador == "<":
					filtro = Q('range', fecha_ultimo_proceso={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', fecha_ultimo_proceso={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', fecha_ultimo_proceso={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', fecha_ultimo_proceso={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"id": "id.keyword",
			"name": "name.keyword",
			"procesos": "procesos",
			"total_monto_pagado": "total_monto_pagado",
			"promedio_monto_pagado": "promedio_monto_pagado",
			"mayor_monto_pagado": "mayor_monto_pagado",
			"menor_monto_pagado": "menor_monto_pagado",
			"fecha_ultimo_proceso": "fecha_ultimo_proceso",
		}

		if not ordenarPor:
			ordenarPor = 'asc(name)'

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()
		paginator = Paginator(search_results, paginarPor)

		proveedores =  [hit["_source"] for hit in results.hits.hits]

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		parametros = {}
		parametros["pagina"] = page
		parametros["nombre"] = nombre
		parametros["identificacion"] = identificacion
		parametros["tmc"] = tmc 
		parametros["pmc"] = pmc 
		parametros["mamc"] = mamc 
		parametros["memc"] = memc 
		parametros["fua"] = fua 
		parametros["cp"] = cp
		parametros["orderBy"] = ordenarPor
		parametros["paginarPor"] = paginarPor

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": proveedores
		}

		return Response(context)

class Proveedor(APIView):

	def get(self, request, partieId=None, format=None):

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)

		qPartieId = Q('match_phrase', doc__compiledRelease__parties__id__keyword=partieId) 
		s = s.query('nested', inner_hits={"size":1}, path='doc.compiledRelease.parties', query=qPartieId)
		s = s.sort({"doc.compiledRelease.date": {"order":"desc"}})
		s = s.source(False)

		results = s[0:1].execute()

		try:
			partie = results["hits"]["hits"][0]["inner_hits"]["doc.compiledRelease.parties"]["hits"]["hits"][0]["_source"].to_dict()
			
			return Response(partie)

		except Exception as e:
			raise Http404

class ContratosDelProveedor(APIView):

	def get(self, request, partieId=None, format=None):
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))
		comprador = request.GET.get('comprador', '')
		titulo = request.GET.get('titulo', '')
		descripcion = request.GET.get('descripcion', '')
		tituloLicitacion = request.GET.get('tituloLicitacion', '')
		categoriaCompra = request.GET.get('categoriaCompra', '')
		estado = request.GET.get('estado', '')
		monto = request.GET.get('monto', '')
		fechaFirma = request.GET.get('fechaFirma', '')
		fechaInicio = request.GET.get('fechaInicio', '')
		ordenarPor = request.GET.get('ordenarPor', '')

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		qPartieId = Q('match_phrase', suppliers__id=partieId) 
		s = s.query('nested', path='suppliers', query=qPartieId)

		# Sección de filtros
		filtros = []

		if comprador.replace(' ',''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if titulo.replace(' ',''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if descripcion.replace(' ',''):
			filtro = Q("match", description=descripcion)
			filtros.append(filtro)

		if tituloLicitacion.replace(' ',''):
			filtro = Q("match", extra__tenderTitle=tituloLicitacion)
			filtros.append(filtro)

		if categoriaCompra.replace(' ',''):
			filtro = Q("match_phrase", localProcurementCategory=categoriaCompra)
			filtros.append(filtro)

		if estado.replace(' ',''):
			filtro = Q("match", status=estado)
			filtros.append(filtro)

		if fechaInicio.replace(' ',''):
			validarFecha = getDateParam(fechaInicio)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', period__startDate=valor)
				elif operador == "<":
					filtro = Q('range', period__startDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', period__startDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', period__startDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', period__startDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaFirma.replace(' ',''):
			validarFecha = getDateParam(fechaFirma)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', dateSigned=valor)
				elif operador == "<":
					filtro = Q('range', dateSigned={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', dateSigned={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', dateSigned={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', dateSigned={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if monto.replace(' ',''):
			validarMonto = getOperator(monto)
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				print("validarMonto")
				print(validarMonto)

				if operador == "==":
					filtro = Q('match', value__amount=valor)
				elif operador == "<":
					filtro = Q('range', value__amount={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', value__amount={'lte': valor})
				elif operador == ">":
					filtro = Q('range', value__amount={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', value__amount={'gte': valor})
				else:
					filtro = None

			if filtro is not None:
				filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "buyer.name.keyword",
			"titulo": "title.keyword",
			"tituloLicitacion": "extra.tenderTitle.keyword",
			"categoriaCompra": "localProcurementCategory.keyword",
			"estado": "status.keyword",
			"monto": "value.amount",
			"fechaInicio": "period.startDate",
			"fechaFirma": "dateSigned",
		}

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()
		paginator = Paginator(search_results, paginarPor)

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		parametros = {}
		parametros["comprador"] = comprador
		parametros["titulo"] = titulo
		parametros["descripcion"] = descripcion
		parametros["tituloLicitacion"] = tituloLicitacion
		parametros["categoriaCompra"] = categoriaCompra
		parametros["estado"] = estado
		parametros["monto"] = monto
		parametros["fechaInicio"] = fechaInicio
		parametros["fechaFirma"] = fechaFirma
		parametros["ordenarPor"] = ordenarPor
		parametros["pagianrPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": results.hits.hits
		}

		return Response(context)

class PagosDelProveedor(APIView):

	def get(self, request, partieId=None, format=None):
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))
		comprador = request.GET.get('comprador', '')
		titulo = request.GET.get('titulo', '')
		monto = request.GET.get('monto', '')
		pagos = request.GET.get('pagos', '')
		fecha = request.GET.get('fecha', '')
		ordenarPor = request.GET.get('ordenarPor', '')

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		#Filtros
		filtros = []
		qPartieId = Q('match_phrase', implementation__transactions__payee__id=partieId) 
		s = s.query('nested', path='implementation.transactions', query=qPartieId)

		if comprador.replace(' ',''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if titulo.replace(' ',''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if fecha.replace(' ',''):
			validarFecha = getDateParam(fecha)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', extra__transactionLastDate=valor)
				elif operador == "<":
					filtro = Q('range', extra__transactionLastDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', extra__transactionLastDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', extra__transactionLastDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', extra__transactionLastDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if monto.replace(' ',''):
			validarMonto = getOperator(monto)
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', value__amount=valor)
				elif operador == "<":
					filtro = Q('range', value__amount={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', value__amount={'lte': valor})
				elif operador == ">":
					filtro = Q('range', value__amount={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', value__amount={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if pagos.replace(' ',''):
			validarMonto = getOperator(pagos)
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', extra__sumTransactions=valor)
				elif operador == "<":
					filtro = Q('range', extra__sumTransactions={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', extra__sumTransactions={'lte': valor})
				elif operador == ">":
					filtro = Q('range', extra__sumTransactions={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', extra__sumTransactions={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "extra.buyerFullName.keyword",
			"titulo": "title.keyword",
			"fecha": "extra.transactionLastDate",
			"monto": "value.amount",
			"pagos": "extra.sumTransactions",
		}

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()
		paginator = Paginator(search_results, paginarPor)

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		parametros = {}
		parametros["comprador"] = comprador
		parametros["titulo"] = titulo
		parametros["monto"] = monto
		parametros["pagos"] = pagos
		parametros["fecha"] = fecha
		parametros["ordenarPor"] = ordenarPor
		parametros["pagianrPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			# "elastic": results.to_dict(),
			"paginador": pagination,
			"parametros": parametros,
			"resultados": results.hits.hits
		}

		return Response(context)

class ProductosDelProveedor(APIView):

	def get(self, request, partieId=None, format=None):
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))

		clasificacion = request.GET.get('clasificacion', '')
		monto = request.GET.get('monto', '')
		cantidadContratos = request.GET.get('cantidadContratos', '')
		proceso = request.GET.get('proceso', 'contrato')

		ordenarPor = request.GET.get('ordenarPor', '')

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		s.aggs.metric('productos','nested', path='items')

		s.aggs['productos'].metric(
			'clasificacion',
			'terms',
			field='items.classification.description.keyword',
			size= 10000
		)

		s.aggs["productos"]["clasificacion"].metric(
			'monto_contratado',
			'sum',
			field='items.unit.value.amount'
		)

		#Filtros
		filtros = []
		qPartieId = Q('match_phrase', suppliers__id=partieId) 
		s = s.query('nested', path='suppliers', query=qPartieId)

		if cantidadContratos.replace(' ', ''):
			q_cc = 'params.cc' + cantidadContratos

			s.aggs["productos"]["clasificacion"].metric(
				'filtro_cantidad', 
				'bucket_selector', 
				buckets_path={"cc": "_count"}, 
				script=q_cc
			)

		if monto.replace(' ', ''):
			q_m = 'params.m' + monto
			s.aggs["productos"]["clasificacion"].metric(
				'filtro_monto', 
				'bucket_selector', 
				buckets_path={"m": "monto_contratado"}, 
				script=q_m
			)

		s = s.query('bool', filter=filtros)

		search_results = SearchResults(s)
		results = s.execute()
		paginator = Paginator(search_results, paginarPor)

		productosES = results.aggregations.productos.clasificacion.to_dict()
		productos = []

		for n in productosES["buckets"]:
			producto = {}
			producto["clasificacion"] = n["key"]
			producto["cantidadContratos"] = n["doc_count"]
			producto["monto"] = n["monto_contratado"]["value"]
			
			productos.append(copy.deepcopy(producto))

		dfProductos = pd.DataFrame(productos)
		ordenar = getSortBy(ordenarPor)

		for indice, columna in enumerate(ordenar["columnas"]):
			if not columna in dfProductos:
				ordenar["columnas"].pop(indice)
				ordenar["ascendentes"].pop(indice)

		if ordenar["columnas"]:
			dfProductos = dfProductos.sort_values(by=ordenar["columnas"], ascending=ordenar["ascendentes"])

		dfProductos = dfProductos.fillna('')

		if clasificacion.replace(' ', ''):
			dfProductos = dfProductos[dfProductos['clasificacion'].str.contains(clasificacion, case=False)]

		# dfProductos["contratos"] = None

		productos = dfProductos.to_dict('records')

		# campos = [
		# 	'extra.buyerFullName', 
		# 	'extra.ocid',
		# 	'value',
		# 	'title'
		# ]

		# for p in productos:
		# 	ss = Search(using=cliente, index=CONTRACT_INDEX)
		# 	ss = ss.filter('match_phrase', items__classification__description__keyword=producto["clasificacion"])
		# 	ss = ss.source(campos)
		# 	results = ss[0:100].execute()
		# 	p["contratos"] = copy.deepcopy(results.hits.hits)
		# 	print(len(results.hits.hits))

		paginator = Paginator(productos, paginarPor)

		try:
			if page < 1:
				page = settings.PAGINATE_BY

			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": len(productos)
		}

		parametros = {}
		parametros["clasificacion"] = clasificacion
		parametros["cantidadContratos"] = cantidadContratos
		parametros["monto"] = monto
		parametros["paginarPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			#"elasticsearch": results.to_dict(), 
			"parametros": parametros,
			"paginador": pagination,
			"resultados": posts.object_list,
		}

		return Response(context)

# Compradores

class Compradores(APIView):

	def get(self, request, format=None):
		page = int(request.GET.get('pagina', '1'))
		nombre = request.GET.get('nombre', '') #nombre
		identificacion = request.GET.get('identificacion', '') # identificacion
		dependencias = request.GET.get('dependencias', '0')
		term = request.GET.get('term', '') #palabra clave
		tmc = request.GET.get('tmc', '') # total monto contratado
		pmc = request.GET.get('pmc', '') # promedio monto contratado
		mamc = request.GET.get('mamc', '') # mayor monto contratado
		memc = request.GET.get('memc', '') # menor monto contratado
		fup = request.GET.get('fup', '') # fecha ultimo proceso
		cp = request.GET.get('cp', '') # cantidad de procesos

		ordenarPor = request.GET.get('ordenarPor', '')
		paginarPor = request.GET.get('paginarPor', settings.PAGINATE_BY)

		tipoIdentificador = request.GET.get('tid', 'nombre') #por id, nombre
		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page-1) * settings.PAGINATE_BY
		end = start + settings.PAGINATE_BY

		cliente = ElasticSearchDefaultConnection()
		
		s = Search(using=cliente, index=OCDS_INDEX)

		filtros = []
		if nombre.replace(' ',''):
			if dependencias == '1':
				filtro = Q("match", extra__buyerFullName=nombre)
			else:
				filtro = Q("match", extra__parentTop__name=nombre)

			filtros.append(filtro)

		if identificacion.replace(' ', ''):
			if dependencias == '1':
				filtro = Q("match", doc__compiledRelease__buyer__id__keyword=identificacion)
			else:
				filtro = Q("match", extra__parentTop__id=identificacion)

			filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		if tipoIdentificador == 'nombre':
			if dependencias == '1':
				campoParaAgrupar = 'extra.buyerFullName.keyword'
			else:
				campoParaAgrupar = 'extra.parentTop.name.keyword'

			s.aggs.metric('compradores', 'terms', field=campoParaAgrupar, size=10000)
			s.aggs['compradores'].metric('procesos', 'cardinality', field='doc.compiledRelease.ocid.keyword')
			
			s.aggs['compradores'].metric('contratos', 'nested', path='doc.compiledRelease.contracts')
			s.aggs['compradores']['contratos'].metric('suma', 'sum', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['contratos'].metric('promedio', 'avg', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['contratos'].metric('maximo', 'max', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['contratos'].metric('minimo', 'min', field='doc.compiledRelease.contracts.value.amount')

			s.aggs['compradores'].metric('fecha_ultimo_proceso', 'max', field='doc.compiledRelease.tender.tenderPeriod.startDate')

			# Filtros
			if tmc.replace(' ', ''):
				q_tmc = 'params.tmc' + tmc
				s.aggs['compradores']\
				.metric('filtro_totales_tmc', 'bucket_selector', buckets_path={"tmc": "contratos.suma"}, script=q_tmc)

			if pmc.replace(' ', ''):
				q_pmc = 'params.pmc' + pmc
				s.aggs['compradores']\
				.metric('filtro_totales_pmc', 'bucket_selector', buckets_path={"pmc": "contratos.promedio"}, script=q_pmc)

			if mamc.replace(' ', ''):
				q_mamc = 'params.mamc' + mamc
				s.aggs['compradores']\
				.metric('filtro_totales_mamc', 'bucket_selector', buckets_path={"mamc": "contratos.maximo"}, script=q_mamc)

			if memc.replace(' ', ''):
				q_memc = 'params.memc' + memc
				s.aggs['compradores']\
				.metric('filtro_totales_memc', 'bucket_selector', buckets_path={"memc": "contratos.minimo"}, script=q_memc)

			if cp.replace(' ', ''):
				q_cp = 'params.cp' + cp
				s.aggs['compradores']\
				.metric('filtro_totales_cp', 'bucket_selector', buckets_path={"cp": "procesos"}, script=q_cp)

			search_results = SearchResults(s)

			results = s[start:end].execute()

			compradoresES = results.aggregations.compradores.to_dict()
			compradores = []

			for n in compradoresES["buckets"]:
				comprador = {}
				# comprador["id"] = p["key"]
				comprador["name"] = n["key"]
				comprador["procesos"] = n["procesos"]["value"]
				comprador["total_monto_contratado"] = n["contratos"]["suma"]["value"]
				comprador["promedio_monto_contratado"] = n["contratos"]["promedio"]["value"]
				comprador["mayor_monto_contratado"] = n["contratos"]["maximo"]["value"]
				comprador["menor_monto_contratado"] = n["contratos"]["minimo"]["value"]

				if n["fecha_ultimo_proceso"]["value"] is None:
					comprador["fecha_ultimo_proceso"] = None
				else:
					comprador["fecha_ultimo_proceso"] = n["fecha_ultimo_proceso"]["value_as_string"]

				comprador["uri"] = urllib.parse.quote_plus(comprador["name"])
				compradores.append(copy.deepcopy(comprador))

			dfCompradores = pd.DataFrame(compradores)
			ordenar = getSortBy(ordenarPor)

			if 'fecha_ultimo_proceso' in dfCompradores:
				dfCompradores['fecha_ultimo_proceso'] = pd.to_datetime(dfCompradores['fecha_ultimo_proceso'], errors='coerce')

			# Ejemplo: fup==2018-03-02
			if fup.replace(' ', ''):
				if len(fup)>1:
					if fup[0:2] in ['!=', '>=', '<=', '==']:
						operador = fup[0:2]
						fecha = fup[2:len(fup)]
					elif fup[0:1] in ['>', '<']:
						operador = fup[0:1]
						fecha = fup[1:len(fup)]
					else:
						operador = ''
						fecha = ''	
				else:
					operador = ''
					fecha = ''

				if operador == "==":
					mask = (dfCompradores['fecha_ultimo_proceso'].dt.date.astype(str) == fecha) 
				elif operador == "!=":
					mask = (dfCompradores['fecha_ultimo_proceso'] != fecha)
				elif operador == "<":
					mask = (dfCompradores['fecha_ultimo_proceso'] < fecha)
				elif operador == "<=":
					mask = (dfCompradores['fecha_ultimo_proceso'] <= fecha)
				elif operador == ">":
					mask = (dfCompradores['fecha_ultimo_proceso'] > fecha)
				elif operador == ">=":
					mask = (dfCompradores['fecha_ultimo_proceso'] >= fecha)
				else:
					mask = None

				if mask is not None:
					dfCompradores = dfCompradores.loc[mask]

			for indice, columna in enumerate(ordenar["columnas"]):
				if not columna in dfCompradores:
					ordenar["columnas"].pop(indice)
					ordenar["ascendentes"].pop(indice)

			if ordenar["columnas"]:
				dfCompradores = dfCompradores.sort_values(by=ordenar["columnas"], ascending=ordenar["ascendentes"])

			dfCompradores = dfCompradores.fillna('')

			compradores = dfCompradores.to_dict('records')
		else:
			if dependencias == '1':
				campoParaAgrupar = 'doc.compiledRelease.buyer.id.keyword'
				nombreCapoAgrupar = 'extra.buyerFullName.keyword'
			else:
				campoParaAgrupar = 'extra.parentTop.id.keyword'
				nombreCapoAgrupar = 'extra.parentTop.name.keyword'

			s.aggs.metric('compradores', 'terms', field=campoParaAgrupar, size=10000)
			s.aggs['compradores'].metric('nombre', 'terms', field=nombreCapoAgrupar, size=10000)
			s.aggs['compradores']['nombre'].metric('procesos', 'cardinality', field='doc.compiledRelease.ocid.keyword')
			
			s.aggs['compradores']['nombre'].metric('contratos', 'nested', path='doc.compiledRelease.contracts')
			s.aggs['compradores']['nombre']['contratos'].metric('suma', 'sum', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('promedio', 'avg', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('maximo', 'max', field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('minimo', 'min', field='doc.compiledRelease.contracts.value.amount')

			s.aggs['compradores']['nombre'].metric('fecha_ultimo_proceso', 'max', field='doc.compiledRelease.tender.tenderPeriod.startDate')

			# Filtros
			if tmc.replace(' ', ''):
				q_tmc = 'params.tmc' + tmc
				s.aggs['compradores']['nombre']\
				.metric('filtro_totales_tmc', 'bucket_selector', buckets_path={"tmc": "contratos.suma"}, script=q_tmc)

			if pmc.replace(' ', ''):
				q_pmc = 'params.pmc' + pmc
				s.aggs['compradores']['nombre']\
				.metric('filtro_totales_pmc', 'bucket_selector', buckets_path={"pmc": "contratos.promedio"}, script=q_pmc)

			if mamc.replace(' ', ''):
				q_mamc = 'params.mamc' + mamc
				s.aggs['compradores']['nombre']\
				.metric('filtro_totales_mamc', 'bucket_selector', buckets_path={"mamc": "contratos.maximo"}, script=q_mamc)

			if memc.replace(' ', ''):
				q_memc = 'params.memc' + memc
				s.aggs['compradores']['nombre']\
				.metric('filtro_totales_memc', 'bucket_selector', buckets_path={"memc": "contratos.minimo"}, script=q_memc)

			if cp.replace(' ', ''):
				q_cp = 'params.cp' + cp
				s.aggs['compradores']['nombre']\
				.metric('filtro_totales_cp', 'bucket_selector', buckets_path={"cp": "procesos"}, script=q_cp)

			search_results = SearchResults(s)

			results = s[start:end].execute()

			compradoresES = results.aggregations.compradores.to_dict()
			compradores = []

			for n in compradoresES["buckets"]:
				for nombreAgg in n["nombre"]["buckets"]:
					comprador = {}
					comprador["id"] = n["key"]
					comprador["name"] = nombreAgg["key"]
					comprador["procesos"] = nombreAgg["procesos"]["value"]
					comprador["total_monto_contratado"] = nombreAgg["contratos"]["suma"]["value"]
					comprador["promedio_monto_contratado"] = nombreAgg["contratos"]["promedio"]["value"]
					comprador["mayor_monto_contratado"] = nombreAgg["contratos"]["maximo"]["value"]
					comprador["menor_monto_contratado"] = nombreAgg["contratos"]["minimo"]["value"]

					if nombreAgg["fecha_ultimo_proceso"]["value"] is None:
						comprador["fecha_ultimo_proceso"] = None
					else:
						comprador["fecha_ultimo_proceso"] = nombreAgg["fecha_ultimo_proceso"]["value_as_string"]

					comprador["uri"] = urllib.parse.quote_plus(comprador["name"])
					compradores.append(copy.deepcopy(comprador))

			dfCompradores = pd.DataFrame(compradores)
			ordenar = getSortBy(ordenarPor)

			if 'fecha_ultimo_proceso' in dfCompradores:
				dfCompradores['fecha_ultimo_proceso'] = pd.to_datetime(dfCompradores['fecha_ultimo_proceso'], errors='coerce')

			# Ejemplo: fup==2018-03-02
			if fup.replace(' ', ''):
				if len(fup)>1:
					if fup[0:2] in ['!=', '>=', '<=', '==']:
						operador = fup[0:2]
						fecha = fup[2:len(fup)]
					elif fup[0:1] in ['>', '<']:
						operador = fup[0:1]
						fecha = fup[1:len(fup)]
					else:
						operador = ''
						fecha = ''	
				else:
					operador = ''
					fecha = ''

				if operador == "==":
					mask = (dfCompradores['fecha_ultimo_proceso'].dt.date.astype(str) == fecha) 
				elif operador == "!=":
					mask = (dfCompradores['fecha_ultimo_proceso'] != fecha)
				elif operador == "<":
					mask = (dfCompradores['fecha_ultimo_proceso'] < fecha)
				elif operador == "<=":
					mask = (dfCompradores['fecha_ultimo_proceso'] <= fecha)
				elif operador == ">":
					mask = (dfCompradores['fecha_ultimo_proceso'] > fecha)
				elif operador == ">=":
					mask = (dfCompradores['fecha_ultimo_proceso'] >= fecha)
				else:
					mask = None

				if mask is not None:
					dfCompradores = dfCompradores.loc[mask]

			for indice, columna in enumerate(ordenar["columnas"]):
				if not columna in dfCompradores:
					ordenar["columnas"].pop(indice)
					ordenar["ascendentes"].pop(indice)

			if ordenar["columnas"]:
				dfCompradores = dfCompradores.sort_values(by=ordenar["columnas"], ascending=ordenar["ascendentes"])

			dfCompradores = dfCompradores.fillna('')

			compradores = dfCompradores.to_dict('records')

		paginator = Paginator(compradores, paginarPor)

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": len(compradores)
		}

		parametros = {}
		parametros["pagina"] = page
		parametros["nombre"] = nombre
		parametros["identificacion"] = identificacion
		parametros["tmc"] = tmc 
		parametros["pmc"] = pmc 
		parametros["mamc"] = mamc 
		parametros["memc"] = memc 
		parametros["fup"] = fup 
		parametros["cp"] = cp
		parametros["dependencias"] = dependencias
		parametros["ordenarPor"] = ordenarPor
		parametros["paginarPor"] = paginarPor

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": posts.object_list,
			# "elastic": results.aggregations.to_dict(),
		}

		return Response(context)

class Comprador(APIView):

	def get(self, request, partieId=None, format=None):

		tipoIdentificador = request.GET.get('tid', 'nombre') #por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)

		partieId = urllib.parse.unquote_plus(partieId)

		if tipoIdentificador == 'nombre':
			qPartieId = Q('match_phrase', doc__compiledRelease__parties__name__keyword=partieId)
			s = s.query('nested', inner_hits={"size":1}, path='doc.compiledRelease.parties', query=qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order":"desc"}})
			s = s.source(False)
		else:
			qPartieId = Q('match_phrase', doc__compiledRelease__parties__id__keyword=partieId)
			s = s.query('nested', inner_hits={"size":1}, path='doc.compiledRelease.parties', query=qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order":"desc"}})
			s = s.source(False)

		results = s[0:1].execute()

		if len(results) == 0:
			s = Search(using=cliente, index=OCDS_INDEX)
			qPartieId = Q('match_phrase', extra__buyerFullName__keyword=partieId)
			s = s.query(qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order":"desc"}})
	
			results2 = s[0:1].execute()

			if len(results2) > 0:
				buyerId = results2.hits.hits[0]["_source"]["doc"]["compiledRelease"]["buyer"]["id"]
				
				qPartieId = Q('match_phrase', doc__compiledRelease__parties__id__keyword=buyerId)
				s = s.query('nested', inner_hits={"size":1}, path='doc.compiledRelease.parties', query=qPartieId)
				s = s.sort({"doc.compiledRelease.date": {"order":"desc"}})
				s = s.source(False)

				results3 = s[0:1].execute()

				dependencias = 1
			else:
				dependencias = 0
		else:
			dependencias = 0

		try:
			if dependencias != 1:
				partie = results["hits"]["hits"][0]["inner_hits"]["doc.compiledRelease.parties"]["hits"]["hits"][0]["_source"].to_dict()
			else:
				partie = results3["hits"]["hits"][0]["inner_hits"]["doc.compiledRelease.parties"]["hits"]["hits"][0]["_source"].to_dict()

			return Response(partie)

		except Exception as e:
			raise Http404

		return Response(results.hits.hits[0]["_source"]["doc"]["compiledRelease"]["buyer"])

class ProcesosDelComprador(APIView):

	def get(self, request, partieId=None, format=None):
		sourceSEFIN = 'HN.SIAFI2'
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))
		
		comprador = request.GET.get('comprador', '')
		ocid = request.GET.get('ocid', '')
		titulo = request.GET.get('titulo', '')
		categoriaCompra = request.GET.get('categoriaCompra', '')
		fechaPublicacion = request.GET.get('fechaPublicacion', '')
		fechaInicio = request.GET.get('fechaInicio', '')
		fechaRecepcion = request.GET.get('fechaRecepcion', '')
		montoContratado = request.GET.get('montoContratado', '')
		estado = request.GET.get('estado', '')

		ordenarPor = request.GET.get('ordenarPor', '')
		dependencias = request.GET.get('dependencias', '0')
		tipoIdentificador = request.GET.get('tid', 'nombre') #por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)

		#Mostrando 
		campos = [
			'doc.ocid', 
			'doc.compiledRelease.date',
			'doc.compiledRelease.tender',
			'doc.compiledRelease.contracts',
			'doc.compiledRelease.buyer',
			'extra'
		]

		s = s.source(campos)

		# Filtrando por nombre del comprador
		partieId = urllib.parse.unquote_plus(partieId)

		if tipoIdentificador == 'id':
			qPartieId1 = Q('match_phrase', doc__compiledRelease__buyer__id__keyword=partieId)
			qPartieId2 = Q('match_phrase', extra__parent1__id__keyword=partieId)
			qPartieId3 = Q('match_phrase', extra__parent2__id__keyword=partieId)

			qPartieId = Q('bool', should=[qPartieId1, qPartieId2, qPartieId3])

			s = s.filter(qPartieId)
		else:
			if dependencias == '1':
				s = s.filter('match_phrase', extra__buyerFullName__keyword=partieId)
			else:
				s = s.filter('match_phrase', extra__parentTop__name__keyword=partieId)

		# Sección de filtros
		filtros = []

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id__keyword=sourceSEFIN)
		s = s.filter('exists', field='doc.compiledRelease.tender')

		if comprador.replace(' ',''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if ocid.replace(' ',''):
			q_ocid = '*{0}*'.format(ocid)
			filtro = Q("wildcard", doc__compiledRelease__ocid__keyword=q_ocid)
			filtros.append(filtro)

		if titulo.replace(' ',''):
			filtro = Q("match", doc__compiledRelease__tender__title__keyword=titulo)
			filtros.append(filtro)

		if categoriaCompra.replace(' ',''):
			filtro = Q("match", doc__compiledRelease__tender__procurementMethodDetails=categoriaCompra)
			filtros.append(filtro)

		if estado.replace(' ',''):
			filtro = Q("match", extra__lastSection__keyword=estado)
			filtros.append(filtro)

		if montoContratado.replace(' ',''):
			validarMonto = getOperator(montoContratado)
			filtro = None
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__tender__extra__sumContracts=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__tender__extra__sumContracts={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__tender__extra__sumContracts={'lte': valor})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__tender__extra__sumContracts={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__tender__extra__sumContracts={'gte': valor})
				else:
					filtro = None

			if filtro is not None:
				filtros.append(filtro)

		if fechaInicio.replace(' ',''):
			validarFecha = getDateParam(fechaInicio)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__tender__tenderPeriod__startDate=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaRecepcion.replace(' ',''):
			validarFecha = getDateParam(fechaRecepcion)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__tender__tenderPeriod__endDate=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaPublicacion.replace(' ',''):
			validarFecha = getDateParam(fechaPublicacion)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__date=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__date={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__date={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__date={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__date={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "extra.buyerFullName.keyword",
			"ocid": "doc.ocid.keyword",
			"titulo": "doc.compiledRelease.tender.title.keyword",
			"categoriaCompra": "doc.compiledRelease.tender.procurementMethodDetails.keyword",
			"estado": "extra.lastSection.keyword",
			"montoContratado": "doc.compiledRelease.tender.extra.sumContracts",
			"fechaInicio": "doc.compiledRelease.tender.tenderPeriod.startDate",
			"fechaRecepcion": "doc.compiledRelease.tender.tenderPeriod.endDate",
			"fechaPublicacion": "doc.compiledRelease.date",
		}

		# ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			if ordenar is not None:
				for parametro in ordenar:
					columna = parametro["valor"]
					orden = parametro["orden"]

					if columna in mappingSort:
						ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()

		if paginarPor < 1:
			paginarPor = settings.PAGINATE_BY

		paginator = Paginator(search_results, paginarPor)

		try:
			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		parametros = {}
		parametros["comprador"] = comprador
		parametros["ocid"] = ocid
		parametros["titulo"] = titulo
		parametros["categoriaCompra"] = categoriaCompra
		parametros["estado"] = estado
		parametros["montoContratado"] = montoContratado
		parametros["fechaInicio"] = fechaInicio
		parametros["fechaRecepcion"] = fechaRecepcion
		parametros["fechaPublicacion"] = fechaPublicacion
		parametros["dependencias"] = dependencias
		parametros["ordenarPor"] = ordenarPor
		parametros["pagianrPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": results.hits.hits
		}

		return Response(context)

class ContratosDelComprador(APIView):

	def get(self, request, partieId=None, format=None):
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))
		proveedor = request.GET.get('proveedor', '')
		titulo = request.GET.get('titulo', '')
		descripcion = request.GET.get('descripcion', '')
		tituloLicitacion = request.GET.get('tituloLicitacion', '')
		categoriaCompra = request.GET.get('categoriaCompra', '')
		modalidad = request.GET.get('modalidad', '')
		estado = request.GET.get('estado', '')
		monto = request.GET.get('monto', '')
		fechaFirma = request.GET.get('fechaFirma', '')
		fechaInicio = request.GET.get('fechaInicio', '')
		ordenarPor = request.GET.get('ordenarPor', '')
		dependencias = request.GET.get('dependencias', '0')
		tipoIdentificador = request.GET.get('tid', 'id') #por id, nombre
		anio = request.GET.get('year', '')

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		# Filtrando por nombre del comprador
		partieId = urllib.parse.unquote_plus(partieId)

		if tipoIdentificador == 'id':
			qPartieId1 = Q('match_phrase', extra__buyer__id__keyword=partieId)
			qPartieId2 = Q('match_phrase', extra__parent1__id__keyword=partieId)
			qPartieId3 = Q('match_phrase', extra__parent2__id__keyword=partieId)

			qPartieId = Q('bool', should=[qPartieId1, qPartieId2, qPartieId3])

			s = s.filter(qPartieId)
		else:
			if dependencias == '1':
				s = s.filter('match_phrase', extra__buyerFullName__keyword=partieId)
			else:
				s = s.filter('match_phrase', extra__parentTop__name__keyword=partieId)

		# Sección de filtros
		filtros = []

		if proveedor.replace(' ',''):
			qProveedor = Q('match', suppliers__name=proveedor) 
			filtro = Q('nested', path='suppliers', query=qProveedor)
			filtros.append(filtro)

		if titulo.replace(' ',''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if descripcion.replace(' ',''):
			filtro = Q("match", description=descripcion)
			filtros.append(filtro)

		if tituloLicitacion.replace(' ',''):
			filtro = Q("match", extra__tenderTitle=tituloLicitacion)
			filtros.append(filtro)

		if categoriaCompra.replace(' ',''):
			filtro = Q("match_phrase", localProcurementCategory=categoriaCompra)
			filtros.append(filtro)

		if modalidad.replace(' ',''):
			filtro = Q("match_phrase", extra__tenderProcurementMethodDetails__keyword=modalidad)
			filtros.append(filtro)

		if estado.replace(' ',''):
			filtro = Q("match", status=estado)
			filtros.append(filtro)

		if fechaInicio.replace(' ',''):
			validarFecha = getDateParam(fechaInicio)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', period__startDate=valor)
				elif operador == "<":
					filtro = Q('range', period__startDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', period__startDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', period__startDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', period__startDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaFirma.replace(' ',''):
			validarFecha = getDateParam(fechaFirma)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', dateSigned=valor)
				elif operador == "<":
					filtro = Q('range', dateSigned={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', dateSigned={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', dateSigned={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', dateSigned={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if monto.replace(' ',''):
			validarMonto = getOperator(monto)
			filtro = None
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', value__amount=valor)
				elif operador == "<":
					filtro = Q('range', value__amount={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', value__amount={'lte': valor})
				elif operador == ">":
					filtro = Q('range', value__amount={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', value__amount={'gte': valor})
				else:
					filtro = None

			if filtro is not None:
				filtros.append(filtro)

		if anio.replace(' ', ''):
			# Or Statement 
			dateFilter = {'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)}
			filtroFechaFirma = Q('range', dateSigned=dateFilter)
			filtroFechaInicio =  Q('range', period__startDate=dateFilter)
			filtrFecha = Q(filtroFechaFirma | filtroFechaInicio)
			filtros.append(filtrFecha)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "extra.buyerFullName.keyword",
			"titulo": "title.keyword",
			"tituloLicitacion": "extra.tenderTitle.keyword",
			"categoriaCompra": "localProcurementCategory.keyword",
			"estado": "status.keyword",
			"monto": "value.amount",
			"fechaFirma": "period.startDate",
			"fechaInicio": "dateSigned",
		}

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			if ordenar is not None:
				for parametro in ordenar:
					columna = parametro["valor"]
					orden = parametro["orden"]

					if columna in mappingSort:
						ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()

		if paginarPor < 1:
			paginarPor = settings.PAGINATE_BY

		paginator = Paginator(search_results, paginarPor)

		try:
			if page < 1:
				page = settings.PAGINATE_BY

			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		parametros = {}
		parametros["proveedor"] = proveedor
		parametros["titulo"] = titulo
		parametros["descripcion"] = descripcion
		parametros["tituloLicitacion"] = tituloLicitacion
		parametros["categoriaCompra"] = categoriaCompra
		parametros["modalidad"] = modalidad
		parametros["estado"] = estado
		parametros["monto"] = monto
		parametros["fechaInicio"] = fechaInicio
		parametros["fechaFirma"] = fechaInicio
		parametros["dependencias"] = dependencias
		parametros["year"] = anio
		parametros["ordenarPor"] = ordenarPor
		parametros["pagianrPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": results.hits.hits
		}

		return Response(context)

class PagosDelComprador(APIView):

	def get(self, request, partieId=None, format=None):
		page = int(request.GET.get('pagina', '1'))
		paginarPor = int(request.GET.get('paginarPor', settings.PAGINATE_BY))
		comprador = request.GET.get('comprador', '')
		proveedor = request.GET.get('proveedor', '')
		titulo = request.GET.get('titulo', '')
		monto = request.GET.get('monto', '')
		pagos = request.GET.get('pagos', '')
		fecha = request.GET.get('fecha', '')
		ordenarPor = request.GET.get('ordenarPor', '')
		dependencias = request.GET.get('dependencias', '0')

		tipoIdentificador = request.GET.get('tid', 'nombre') #por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page-1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		#Filtros
		filtros = []

		s = s.filter('exists', field='implementation')

		partieId = urllib.parse.unquote_plus(partieId)

		if tipoIdentificador == 'id':
			s = s.filter('match_phrase', extra__buyer__id__keyword=partieId)
		else:
			if dependencias == '1':
				s = s.filter('match_phrase', extra__buyerFullName__keyword=partieId)
			else:
				s = s.filter('match_phrase', extra__parentTop__name__keyword=partieId)

		if comprador.replace(' ',''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if proveedor.replace(' ',''):
			qProveedor = Q('match', implementation__transactions__payee__name=proveedor) 
			s = s.query('nested', path='implementation.transactions', query=qProveedor)

		if titulo.replace(' ',''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if fecha.replace(' ',''):
			validarFecha = getDateParam(fecha)
			
			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', extra__transactionLastDate=valor)
				elif operador == "<":
					filtro = Q('range', extra__transactionLastDate={'lt': valor, "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', extra__transactionLastDate={'lte': valor, "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', extra__transactionLastDate={'gt': valor, "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', extra__transactionLastDate={'gte': valor, "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if monto.replace(' ',''):
			validarMonto = getOperator(monto)
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', value__amount=valor)
				elif operador == "<":
					filtro = Q('range', value__amount={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', value__amount={'lte': valor})
				elif operador == ">":
					filtro = Q('range', value__amount={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', value__amount={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if pagos.replace(' ',''):
			validarMonto = getOperator(pagos)
			if validarMonto is not None:
				operador = validarMonto["operador"]
				valor = validarMonto["valor"]

				if operador == "==":
					filtro = Q('match', extra__sumTransactions=valor)
				elif operador == "<":
					filtro = Q('range', extra__sumTransactions={'lt': valor})
				elif operador == "<=":
					filtro = Q('range', extra__sumTransactions={'lte': valor})
				elif operador == ">":
					filtro = Q('range', extra__sumTransactions={'gt': valor})
				elif operador == ">=":
					filtro = Q('range', extra__sumTransactions={'gte': valor})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "extra.buyerFullName.keyword",
			"titulo": "title.keyword",
			"fecha": "extra.transactionLastDate",
			"monto": "value.amount",
			"pagos": "extra.sumTransactions",
		}

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ',''):
			ordenar = getSortES(ordenarPor)

			for parametro in ordenar:
				columna = parametro["valor"]
				orden = parametro["orden"]

				if columna in mappingSort:
					ordenarES[mappingSort[columna]] = {"order": orden}

		s = s.sort(ordenarES)

		search_results = SearchResults(s)
		results = s[start:end].execute()

		if paginarPor < 1:
			 paginarPor = settings.PAGINATE_BY

		paginator = Paginator(search_results, paginarPor)

		try:
			if page < 1:
				page = settings.PAGINATE_BY

			posts = paginator.page(page)
		except PageNotAnInteger:
			posts = paginator.page(1)
		except EmptyPage:
			posts = paginator.page(paginator.num_pages)

		pagination = {
			"has_previous": posts.has_previous(),
			"has_next": posts.has_next(),
			"previous_page_number": posts.previous_page_number() if posts.has_previous() else None,
			"page": posts.number,
			"next_page_number": posts.next_page_number() if posts.has_next() else None,
			"num_pages": paginator.num_pages,
			"total.items": results.hits.total
		}

		parametros = {}
		parametros["comprador"] = comprador
		parametros["proveedor"] = proveedor
		parametros["titulo"] = titulo
		parametros["monto"] = monto
		parametros["pagos"] = pagos
		parametros["fecha"] = fecha
		parametros["ordenarPor"] = ordenarPor
		parametros["pagianrPor"] = paginarPor
		parametros["pagina"] = page

		context = {
			"paginador": pagination,
			"parametros": parametros,
			"resultados": results.hits.hits
		}

		return Response(context)

# Dashboard SEFIN

class FiltrosDashboardSEFIN(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')
		masinstituciones = request.GET.get('masinstituciones', '')
		masproveedores = request.GET.get('masproveedores', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		ss = Search(using=cliente, index=TRANSACTION_INDEX)

		cantidadInstituciones = 50
		cantidadProveedores = 50

		if masinstituciones.replace(' ', '') == '1':
			cantidadInstituciones = 5000

		if masproveedores.replace(' ', '') == '1':
			cantidadProveedores = 30000

		# Filtros

		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente

		# Resumen	

		s.aggs.metric(
			'instituciones', 
			'terms', 
			field='extra.buyerFullName.keyword', 
			size=cantidadInstituciones
		)

		s.aggs.metric(
			'proveedores', 
			'terms', 
			field='payee.name.keyword', 
			size=cantidadProveedores
		)

		s.aggs.metric(
			'años', 
			'date_histogram', 
			field='date', 
			interval='year', 
			format='yyyy'
		)

		ss.aggs.metric(
			'años', 
			'date_histogram', 
			field='date', 
			interval='year', 
			format='yyyy'
		)

		s.aggs.metric(
			'monedas', 
			'terms', 
			field='value.currency.keyword', 
			size=10000
		)

		s.aggs.metric(
			'fuentes', 
			'terms', 
			field='extra.fuentes.keyword', 
			size=10000
		)

		s.aggs.metric(
			'objetosGasto', 
			'terms',
			field='extra.objetosGasto.keyword', 
			size=10000
		)

		results = s.execute()
		resultsYears = ss.execute()

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor
		parametros["masinstituciones"] = masinstituciones
		parametros["masproveedores"] = masproveedores

		resultados = results.aggregations.to_dict()

		years = resultsYears.aggregations.to_dict()

		if "años" in resultados:
			if 'buckets' in years["años"]:
				years["años"]['buckets'] = sorted(years["años"]['buckets'], key=lambda k: k['key_as_string'], reverse=True)
			resultados["años"] = years["años"] 

		context = {
			"parametros": parametros,
			"respuesta": resultados
		}

		return Response(context)

class GraficarCantidadDePagosMes(APIView):

	def get(self, request, format=None):
		pagos_mes = []
		promedios_mes = []
		lista_meses = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"cantidad_pagos": 0,
				"promedio_pagos": 0
			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'total_pagos',
			'value_count',
			field='id.keyword'
		)

		s.aggs.metric(
			'pagos_por_mes', 
			'date_histogram', 
			field='date',
			interval= "month",
			format= "MM"
		)

		s.aggs["pagos_por_mes"].metric(
			'suma_montos',
			'sum',
			field = 'value.amount'
		)
		
		results = s.execute()

		aggs = results.aggregations.pagos_por_mes.to_dict()

		for bucket in aggs["buckets"]:
			if bucket["key_as_string"] in meses:

				total = results.aggregations.total_pagos["value"]
				
				if 'cantidad_pagos' in meses[bucket["key_as_string"]]:
					count = meses[bucket["key_as_string"]]["cantidad_pagos"] + bucket["doc_count"]
				else:
					count = bucket["doc_count"]

				if total != 0:
					promedio = count / total
				else:
					promedio = 0

				meses[bucket["key_as_string"]] = {
					"cantidad_pagos": count,
					"promedio_pagos": promedio
				}

		for mes in meses:
			pagos_mes.append(meses[mes]["cantidad_pagos"])
			promedios_mes.append(meses[mes]["promedio_pagos"])
			lista_meses.append(NombreDelMes(mes))

		resultados = {
			"cantidadpagos": pagos_mes,
			"promediopagos": promedios_mes,
			"meses": lista_meses,
			"totalpagos": results.aggregations.total_pagos["value"],
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarMontosDePagosMes(APIView):

	def get(self, request, format=None):
		pagos_mes = []
		promedios_mes = []
		lista_meses = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"montos_pagos": 0,
				"promedio_pagos": 0
			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'montos_pagos',
			'sum',
			field='value.amount'
		)

		s.aggs.metric(
			'pagos_por_mes', 
			'date_histogram', 
			field='date',
			interval= "month",
			format= "MM"
		)

		s.aggs["pagos_por_mes"].metric(
			'suma_montos',
			'sum',
			field = 'value.amount'
		)
		
		results = s.execute()

		aggs = results.aggregations.pagos_por_mes.to_dict()

		for bucket in aggs["buckets"]:
			if bucket["key_as_string"] in meses:

				total = results.aggregations.montos_pagos["value"]

				if 'montos_pagos' in meses[bucket["key_as_string"]]:
					count = meses[bucket["key_as_string"]]["montos_pagos"] + bucket["suma_montos"]["value"]
				else:
					count = bucket["suma_montos"]["value"]

				if total != 0:
					promedio = count / total
				else:
					promedio = 0

				meses[bucket["key_as_string"]] = {
					"montos_pagos": count,
					"promedio_pagos": promedio
				}

		for mes in meses:
			pagos_mes.append(meses[mes]["montos_pagos"])
			promedios_mes.append(meses[mes]["promedio_pagos"])
			lista_meses.append(NombreDelMes(mes))

		resultados = {
			"montopagos": pagos_mes,
			"promediopagos": promedios_mes,
			"meses": lista_meses,
			"totalpagos": results.aggregations.montos_pagos["value"],
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EstadisticaMontoDePagos(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)
		ss = Search(using=cliente, index=TRANSACTION_INDEX)
		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			ss = ss.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)
			ss = ss.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)
			ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			ss = ss.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			ss = ss.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
		
		# Agregados
		s.aggs.metric(
			'total_pagado',
			'sum',
			field='value.amount'
		)

		s.aggs.metric(
			'promedio_pagado',
			'avg',
			field='value.amount'
		)

		s.aggs.metric(
			'maximo_pagado',
			'max',
			field='value.amount'
		)

		# Filtrando montos de transacciones > 0
		ss = ss.filter('range', value__amount={'gt': 0})
		
		ss.aggs.metric(
			'minimo_pagado', 
			'min', 
			field='value.amount'
		)

		results = s.execute()
		results2 = ss.execute()

		resultados = {
			"promedio": results.aggregations.promedio_pagado["value"],
			"mayor": results.aggregations.maximo_pagado["value"],
			"menor": results2.aggregations.minimo_pagado["value"],
			"total": results.aggregations.total_pagado["value"],
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EstadisticaCantidadDePagos(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'total_pagos',
			'value_count',
			field='id.keyword'
		)

		s.aggs.metric(
			'pagos_por_mes', 
			'date_histogram', 
			field='date',
			interval= "month",
			format= "MM"
		)

		results = s.execute()

		aggs = results.aggregations.pagos_por_mes.to_dict()

		cantidad_por_meses = []

		for bucket in aggs["buckets"]:
			cantidad_por_meses.append(bucket["doc_count"])

		resultados = {
			"promedio": statistics.mean(cantidad_por_meses) if len(cantidad_por_meses) > 0 else 0,
			"mayor": max(cantidad_por_meses) if len(cantidad_por_meses) > 0 else 0,
			"menor": min(cantidad_por_meses) if len(cantidad_por_meses) > 0 else 0,
			"total": results.aggregations.total_pagos.value,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class TopCompradoresPorMontoPagado(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'compradores',
			'terms',
			field='extra.buyerFullName.keyword',
			order={'total_pagado': 'desc'},
			size=10
		)

		s.aggs["compradores"].metric(
			'total_pagado',
			'sum',
			field='value.amount'
		)

		results = s.execute()

		buckets = results.aggregations.compradores.to_dict()

		compradores = []
		montos = []

		for bucket in buckets["buckets"]:
			compradores.append(bucket["key"])
			montos.append(bucket["total_pagado"]["value"])

		compradores.reverse()
		montos.reverse()

		resultados = {
			"compradores": compradores,
			"montos": montos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class TopProveedoresPorMontoPagado(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'proveedores',
			'terms',
			field='payee.name.keyword',
			order={'total_pagado': 'desc'},
			size=10
		)

		s.aggs["proveedores"].metric(
			'total_pagado',
			'sum',
			field='value.amount'
		)

		results = s.execute()

		buckets = results.aggregations.proveedores.to_dict()

		proveedores = []
		montos = []

		for bucket in buckets["buckets"]:
			proveedores.append(bucket["key"])
			montos.append(bucket["total_pagado"]["value"])

		proveedores.reverse()
		montos.reverse()

		resultados = {
			"proveedores": proveedores,
			"montos": montos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class TopObjetosDeGastoPorMontoPagado(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		objetogasto = request.GET.get('objetogasto', '')
		fuentefinanciamiento = request.GET.get('fuentefinanciamiento', '')
		proveedor = request.GET.get('proveedor', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=TRANSACTION_INDEX)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__buyerFullName=institucion)
			# compiledRelease.buyer.name

		if proveedor.replace(' ', ''):
			s = s.filter('match_phrase', payee__name__keyword=proveedor)

		if anio.replace(' ', ''):
			s = s.filter('range', date={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			s = s.filter('match_phrase', value__currency__keyword=moneda)

		if objetogasto.replace(' ', ''):
			s = s.filter('match_phrase', extra__objetosGasto__keyword=objetogasto)
			# planning.budget.budgetBreakdown.n.classifications.objeto

		if fuentefinanciamiento.replace(' ', ''):
			s = s.filter('match_phrase', extra__fuentes=fuentefinanciamiento)
			# planning.budget.budgetBreakdown.n.classifications.fuente
			
		# Agregados
		s.aggs.metric(
			'objetos',
			'terms',
			field='extra.objetosGasto.keyword',
			order={'total_pagado': 'desc'},
			size=10
		)


		s.aggs["objetos"].metric(
			'total_pagado',
			'cardinality',
			field='extra.ocid.keyword',
			precision_threshold=40000
		)

		results = s.execute()

		buckets = results.aggregations.objetos.to_dict()

		objetos = []
		montos = []

		for bucket in buckets["buckets"]:
			objetos.append(bucket["key"])
			montos.append(bucket["total_pagado"]["value"])

		objetos.reverse()			
		montos.reverse()

		resultados = {
			"objetosGasto": objetos,
			"montos": montos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["objetogasto"] = objetogasto
		parametros["fuentefinanciamiento"] = fuentefinanciamiento
		parametros["proveedor"] = proveedor

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EtapasPagoProcesoDeCompra(APIView):

	def get(self, request, format=None):
		sourceSEFIN = 'HN.SIAFI2'
		institucion = request.GET.get('institucion', '')
		anio = request.GET.get('año', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		# Filtros
		s = s.filter('match_phrase', doc__compiledRelease__sources__id=sourceSEFIN)

		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__buyer__name=institucion)

		if anio.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__planning__budget__budgetBreakdown__classifications__gestion=anio)
			
		# Agregados
		s.aggs.metric(
			'nested_contratos', 
			'nested', 
			path='doc.compiledRelease.contracts'
		)

		s.aggs["nested_contratos"].metric(
			'monto_transacciones',
			'sum',
			field='doc.compiledRelease.contracts.implementation.transactions.value.amount'
		)

		s.aggs["nested_contratos"].metric(
			'monto_devengado',
			'sum',
			field='doc.compiledRelease.contracts.implementation.financialObligations.bill.amount.amount'
		)

		s.aggs.metric(
			'monto_precomprometido',
			'sum',
			field='doc.compiledRelease.planning.budget.budgetBreakdown.measures.precomprometido'
		)

		s.aggs.metric(
			'monto_comprometido',
			'sum',
			field='doc.compiledRelease.planning.budget.budgetBreakdown.measures.comprometido'
		)

		s.aggs.metric(
			'monto_adjudicado',
			'sum',
			field='doc.compiledRelease.awards.value.amount'
		)

		results = s.execute()

		buckets = results.aggregations.to_dict()

		precomprometido = buckets["monto_precomprometido"]["value"] 
		comprometido = buckets["monto_comprometido"]["value"]
		# adjudicado = buckets["monto_adjudicado"]["value"]
		devengado = buckets["nested_contratos"]["monto_devengado"]["value"]
		transacciones = buckets["nested_contratos"]["monto_transacciones"]["value"]

		if precomprometido != 0:
			porcentaje_precomprometido = (precomprometido / precomprometido) * 100
			porcentaje_comprometido = (comprometido / precomprometido) * 100
			porcentaje_devengado = (devengado / precomprometido) * 100
			porcentaje_transacciones = (transacciones / precomprometido) * 100
		else:
			porcentaje_precomprometido = 0
			porcentaje_comprometido = 0
			porcentaje_devengado = 0
			porcentaje_transacciones = 0

		series = ['Precomprometido', 'Comprometido', 'Devengado', 'Pagado']
		montos = [precomprometido, comprometido, devengado, transacciones]
		porcentajes = [porcentaje_precomprometido, porcentaje_comprometido, porcentaje_devengado, porcentaje_transacciones]

		resultados = {
			"series": series,
			"montos": montos,
			"porcentajes": porcentajes
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio

		context = {
			# "elastic": buckets
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

# Dashboard ONCAE

class FiltrosDashboardONCAE(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')
		masinstituciones = request.GET.get('masinstituciones', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)
		sss = Search(using=cliente, index=CONTRACT_INDEX)
		ssss = Search(using=cliente, index=OCDS_INDEX)

		sFecha = Search(using=cliente, index=OCDS_INDEX)
		ssFecha = Search(using=cliente, index=CONTRACT_INDEX)
		sssFecha = Search(using=cliente, index=CONTRACT_INDEX)

		# Excluyendo procesos de SEFIN
		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		sss = sss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ssss = ssss.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)

		sFecha = sFecha.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		ssFecha = ssFecha.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		sssFecha = sssFecha.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		sss = sss.exclude(sistemaCE & estadoOC)
		sssFecha = sssFecha.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		sss = sss.exclude(sistemaDC & estadoContrato)
		sssFecha = sssFecha.exclude(sistemaDC & estadoContrato)
			
		#Temporal
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')
		sFecha = sFecha.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')
		ssss = ssss.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			sss = sss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

			sFecha = sFecha.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ssFecha = ssFecha.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			sssFecha = sssFecha.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			sss = sss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

			sFecha = sFecha.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ssFecha = ssFecha.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			sssFecha = sssFecha.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			sss = sss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':			
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword') 
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
				sss = sss.filter('bool', must_not=qqCategoria)

				sFecha = sFecha.filter('bool', must_not=qCategoria)
				ssFecha = ssFecha.filter('bool', must_not=qqCategoria)
				sssFecha = sssFecha.filter('bool', must_not=qqCategoria)
			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)
				sss = sss.filter('match_phrase', localProcurementCategory__keyword=categoria)

				sFecha = sFecha.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)
				ssFecha = ssFecha.filter('match_phrase', localProcurementCategory__keyword=categoria)
				sssFecha = sssFecha.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':			
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword') 
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
				sss = sss.filter('bool', must_not=qqModalidad)

				sFecha = sFecha.filter('bool', must_not=qModalidad)
				ssFecha = ssFecha.filter('bool', must_not=qqModalidad)
				sssFecha = sssFecha.filter('bool', must_not=qqModalidad)

			else:			
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				sss = sss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

				sFecha = sFecha.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)
				ssFecha = ssFecha.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				sssFecha = sssFecha.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qMoneda = Q('exists', field='value.currency.keyword') 
				ss = ss.filter('bool', must_not=qMoneda)
				sss = sss.filter('bool', must_not=qMoneda)

				ssFecha = ssFecha.filter('bool', must_not=qMoneda)
				sssFecha = sssFecha.filter('bool', must_not=qMoneda)
			
			else:
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
				sss = sss.filter('match_phrase', value__currency__keyword=moneda)

				ssFecha = ssFecha.filter('match_phrase', value__currency__keyword=moneda)
				sssFecha = sssFecha.filter('match_phrase', value__currency__keyword=moneda)

		if sistema.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__sources__id__keyword=sistema)
			ss = ss.filter('match_phrase', extra__sources__id__keyword=sistema)
			sss = sss.filter('match_phrase', extra__sources__id__keyword=sistema)

		cantidadInstituciones = 50

		if masinstituciones.replace(' ', '') == '1':
			cantidadInstituciones = 5000

		# Resumen
		s.aggs.metric(
			'instituciones', 
			'terms', 
			field='extra.parentTop.id.keyword', 
			size=cantidadInstituciones
		)

		s.aggs["instituciones"].metric(
			'nombre', 
			'terms', 
			field='extra.parentTop.name.keyword', 
			size=cantidadInstituciones
		)

		ss.aggs.metric(
			'instituciones',
			'terms',
			field='extra.parentTop.id.keyword', 
			size=cantidadInstituciones
		)

		ss.aggs["instituciones"].metric(
			'nombre',
			'terms',
			field='extra.parentTop.name.keyword', 
			size=cantidadInstituciones
		)

		sss.aggs.metric(
			'instituciones',
			'terms',
			field='extra.parentTop.id.keyword', 
			size=cantidadInstituciones
		)

		sss.aggs["instituciones"].metric(
			'nombre',
			'terms',
			field='extra.parentTop.name.keyword', 
			size=cantidadInstituciones
		)

		# s.aggs.metric(
		# 	'aniosProcesos', 
		# 	'date_histogram', 
		# 	field='doc.compiledRelease.tender.datePublished', 
		# 	interval='year', 
		# 	format='yyyy',
		# 	min_doc_count=1
		# )

		# ss.aggs.metric(
		# 	'aniosContratoFechaFirma', 
		# 	'date_histogram', 
		# 	field='dateSigned', 
		# 	interval='year', 
		# 	format='yyyy',
		# 	min_doc_count=1
		# )

		# sss.aggs.metric(
		# 	'aniosContratoFechaInicio', 
		# 	'date_histogram', 
		# 	field='period.startDate', 
		# 	interval='year', 
		# 	format='yyyy',
		# 	min_doc_count=1
		# )

		s.aggs.metric(
			'categoriasProcesos', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.tender.localProcurementCategory.keyword', 
			size=10000
		)

		ss.aggs.metric(
			'categoriasContratosFechaFirma', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword', 
			size=10000
		)

		sss.aggs.metric(
			'categoriasContratosFechaInicio', 
			'terms',
			missing='No Definido', 
			field='localProcurementCategory.keyword', 
			size=10000
		)

		s.aggs.metric(
			'modalidadesProcesos', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.tender.procurementMethodDetails.keyword', 
			size=10000
		)

		ss.aggs.metric(
			'modalidadesContratosFechaFirma', 
			'terms', 
			missing='No Definido',
			field='extra.tenderProcurementMethodDetails.keyword', 
			size=10000
		)

		sss.aggs.metric(
			'modalidadesContratosFechaInicio', 
			'terms',
			missing='No Definido', 
			field='extra.tenderProcurementMethodDetails.keyword', 
			size=10000
		)

		ss.aggs.metric(
			'monedasContratoFechaFirma', 
			'terms',
			field='value.currency.keyword', 
			# missing='No Definido',
			size=10000
		)

		ss.aggs["monedasContratoFechaFirma"].metric(
			'procesos',
			'cardinality',
			field='extra.ocid.keyword',
			precision_threshold=40000
		)

		sss.aggs.metric(
			'monedasContratoFechaInicio', 
			'terms',
			field='value.currency.keyword',
			# missing='No Definido', 
			size=10000
		)

		sss.aggs["monedasContratoFechaInicio"].metric(
			'procesos',
			'cardinality',
			field='extra.ocid.keyword',
			precision_threshold=40000
		)

		ssss.aggs.metric(
			'sources', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.sources.id.keyword', 
			size=10000
		)

		ssss.aggs["sources"].metric(
			'name', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.sources.name.keyword', 
			size=10000
		)

		sFecha.aggs.metric(
			'aniosProcesos', 
			'date_histogram', 
			field='doc.compiledRelease.tender.tenderPeriod.startDate', 
			interval='year', 
			format='yyyy',
			min_doc_count=1
		)

		ssFecha.aggs.metric(
			'aniosContratoFechaFirma', 
			'date_histogram', 
			field='dateSigned', 
			interval='year', 
			format='yyyy',
			min_doc_count=1
		)

		sssFecha.aggs.metric(
			'aniosContratoFechaInicio', 
			'date_histogram', 
			field='period.startDate', 
			interval='year', 
			format='yyyy',
			min_doc_count=1
		)

		procesos = s.execute()
		contratosPC = ss.execute()
		contratosDD = sss.execute()
		filtroSource = ssss.execute()
		
		sFechaResultados = sFecha.execute()
		ssFechaResultados = ssFecha.execute()
		sssFechaResultados = sssFecha.execute()

		categoriasProcesos = procesos.aggregations.categoriasProcesos.to_dict()
		categoriasContratosPC = contratosPC.aggregations.categoriasContratosFechaFirma.to_dict()
		categoriasContratosDD = contratosDD.aggregations.categoriasContratosFechaInicio.to_dict()

		modalidadesProcesos = procesos.aggregations.modalidadesProcesos.to_dict()
		modalidadesContratosPC = contratosPC.aggregations.modalidadesContratosFechaFirma.to_dict()
		modalidadesContratosDD = contratosDD.aggregations.modalidadesContratosFechaInicio.to_dict()

		aniosProcesos = sFechaResultados.aggregations.aniosProcesos.to_dict()
		aniosFechaFirma = ssFechaResultados.aggregations.aniosContratoFechaFirma.to_dict()
		aniosFechaInicio = sssFechaResultados.aggregations.aniosContratoFechaInicio.to_dict()

		institucionesProcesos = procesos.aggregations.instituciones.to_dict()
		institucionesContratosPC = contratosPC.aggregations.instituciones.to_dict()
		institucionesContratosDD = contratosDD.aggregations.instituciones.to_dict()

		monedasContratosPC = contratosPC.aggregations.monedasContratoFechaFirma.to_dict()
		monedasContratosDD = contratosDD.aggregations.monedasContratoFechaInicio.to_dict()

		sourcesES = filtroSource.aggregations.sources.to_dict()

		#Valores para filtros por anio
		anios = {}

		for value in aniosProcesos["buckets"]:
			anios[value["key_as_string"]] = {}
			anios[value["key_as_string"]]["key_as_string"] = value["key_as_string"]
			if "procesos" in anios[value["key_as_string"]]:
				anios[value["key_as_string"]]["procesos"] += value["doc_count"]
			else:
				anios[value["key_as_string"]]["procesos"] = value["doc_count"]

		for value in aniosFechaFirma["buckets"]:
			if value["key_as_string"] in anios:
				if "contratos" in anios[value["key_as_string"]]:
					anios[value["key_as_string"]]["contratos"] += value["doc_count"]
				else:
					anios[value["key_as_string"]]["contratos"] = value["doc_count"]
			else:
				anios[value["key_as_string"]] = {}
				anios[value["key_as_string"]]["key_as_string"] = value["key_as_string"]
				anios[value["key_as_string"]]["contratos"] = value["doc_count"]

		for value in aniosFechaInicio["buckets"]:
			if value["key_as_string"] in anios:
				if "contratos" in anios[value["key_as_string"]]:
					anios[value["key_as_string"]]["contratos"] += value["doc_count"]
				else:
					anios[value["key_as_string"]]["contratos"] = value["doc_count"]
			else:
				anios[value["key_as_string"]] = {}
				anios[value["key_as_string"]]["key_as_string"] = value["key_as_string"]
				anios[value["key_as_string"]]["procesos"] = 0
				anios[value["key_as_string"]]["contratos"] = value["doc_count"]

		years = []
		annioActual = int(datetime.datetime.now().year)

		for key, value in anios.items():
			if int(value["key_as_string"]) <= annioActual and int(value["key_as_string"]) >= 1980:
				years.append(value)

		years = sorted(years, key=lambda k: k['key_as_string'], reverse=True) 

		#Valores para filtros por institucion padre
		instituciones = []

		for codigo in institucionesProcesos["buckets"]:

			for nombre in codigo["nombre"]["buckets"]:
				instituciones.append({
					"codigo": codigo["key"],
					"nombre": nombre["key"],
					"procesos": nombre["doc_count"],
					"contratos": 0
				})
		
		if anio.replace(' ', ''):	
			for codigo in institucionesContratosPC["buckets"]:

				for nombre in codigo["nombre"]["buckets"]:
					instituciones.append({
						"codigo": codigo["key"],
						"nombre": nombre["key"],
						"procesos": 0,
						"contratos": nombre["doc_count"]
					})

			for codigo in institucionesContratosDD["buckets"]:

				for nombre in codigo["nombre"]["buckets"]:
					instituciones.append({
						"codigo": codigo["key"],
						"nombre": nombre["key"],
						"procesos": 0,
						"contratos": nombre["doc_count"]
					})

		if instituciones:
			dfInstituciones = pd.DataFrame(instituciones)

			agregaciones = {
				"nombre": 'first',
				"procesos": 'sum',
				"contratos": 'sum'
			}

			dfInstituciones = dfInstituciones.groupby('codigo', as_index=True).agg(agregaciones).reset_index().sort_values("procesos", ascending=False)

			instituciones = dfInstituciones.to_dict('records')

			instituciones = instituciones[0:cantidadInstituciones]

		#Valores para filtros por moneda del contrato.
		monedas = [] 
		for valor in monedasContratosPC["buckets"]:
			monedas.append({
				"moneda": valor["key"],
				"contratos": valor["doc_count"],
				"procesos": valor["procesos"]["value"]
			})

		if anio.replace(' ', ''):
			for valor in monedasContratosDD["buckets"]:
				monedas.append({
					"moneda": valor["key"],
					"contratos": valor["doc_count"],
					"procesos": valor["procesos"]["value"]
				})

		if monedas:
			dfMonedas = pd.DataFrame(monedas)

			agregaciones = {
				"procesos": 'sum',
				"contratos": 'sum'
			}

			dfMonedas = dfMonedas.groupby('moneda', as_index=True).agg(agregaciones).reset_index().sort_values("procesos", ascending=False)

			monedas = dfMonedas.to_dict('records')

		#valores para filtros por categoria.
		categorias = []

		for valor in categoriasProcesos["buckets"]:
			categorias.append({
				"categoria": valor["key"],
				"procesos": valor["doc_count"],
				"contratos": 0
			})

		for valor in categoriasContratosPC["buckets"]:
			categorias.append({
				"categoria": valor["key"],
				"procesos": 0,
				"contratos": valor["doc_count"]
			})

		if anio.replace(' ', ''):
			for valor in categoriasContratosDD["buckets"]:
				categorias.append({
					"categoria": valor["key"],
					"procesos": 0,
					"contratos": valor["doc_count"]
				})

		if categorias:
			dfCategorias = pd.DataFrame(categorias)

			agregaciones = {
				"procesos": 'sum',
				"contratos": 'sum'
			}

			dfCategorias = dfCategorias.groupby('categoria', as_index=True).agg(agregaciones).reset_index().sort_values("procesos", ascending=False)

			categorias = dfCategorias.to_dict('records')

		#valores para filtros por modalidades.
		modalidades = []

		for valor in modalidadesProcesos["buckets"]:
			modalidades.append({
				"modalidad": valor["key"],
				"procesos": valor["doc_count"],
				"contratos": 0
			})

		for valor in modalidadesContratosPC["buckets"]:
			modalidades.append({
				"modalidad": valor["key"],
				"procesos": 0,
				"contratos": valor["doc_count"]
			})

		if anio.replace(' ', ''):
			for valor in modalidadesContratosDD["buckets"]:
				modalidades.append({
					"modalidad": valor["key"],
					"procesos": 0,
					"contratos": valor["doc_count"]
				})

		if modalidades:
			dfModalidades = pd.DataFrame(modalidades)

			agregaciones = {
				"procesos": 'sum',
				"contratos": 'sum'
			}

			dfModalidades = dfModalidades.groupby('modalidad', as_index=True).agg(agregaciones).reset_index().sort_values("procesos", ascending=False)

			modalidades = dfModalidades.to_dict('records')

		sources = []
		for valor in sourcesES["buckets"]:
			sources.append({
				'id': valor["key"],
				'ocids': valor["doc_count"]
			})

		resultados = {}
		resultados["años"] = years
		resultados["monedas"] = monedas
		resultados["instituciones"] = instituciones
		resultados["categorias"] = categorias
		resultados["modalidades"] = modalidades
		resultados["sistemas"] = sources

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = idinstitucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["masinstituciones"] = masinstituciones

		context = {
			"parametros": parametros,
			"respuesta": resultados
		}

		return Response(context)

class GraficarProcesosPorCategorias(APIView):

	def get(self, request, format=None):

		categorias = []
		procesosCategoria = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)

		# Agregados
		s.aggs.metric(
			'totalProcesos',
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		s.aggs.metric(
			'procesosPorEtapa', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.tender.localProcurementCategory.keyword' 
		)

		#Borrar estas lineas
		# print("Resultados")
		# return DescargarProcesosCSV(request, s)

		results = s.execute()

		totalProcesos = results.aggregations.totalProcesos["value"]

		aggs = results.aggregations.procesosPorEtapa.to_dict()

		for bucket in aggs["buckets"]:
			categorias.append(bucket["key"])
			procesosCategoria.append(bucket["doc_count"])

		resultados = {
			"categorias": categorias,
			"procesos": procesosCategoria,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarProcesosPorModalidad(APIView):

	def get(self, request, format=None):

		modalidades = []
		procesosModalidad = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')

		#Temporal
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)

		# Agregados
		s.aggs.metric(
			'totalProcesos',
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		s.aggs.metric(
			'procesosPorModalidad', 
			'terms', 
			missing='No Definido',
			field='doc.compiledRelease.tender.procurementMethodDetails.keyword' 
		)
		
		results = s.execute()

		totalProcesos = results.aggregations.totalProcesos["value"]

		aggs = results.aggregations.procesosPorModalidad.to_dict()

		for bucket in aggs["buckets"]:
			modalidades.append(bucket["key"])
			procesosModalidad.append(bucket["doc_count"])

		resultados = {
			"modalidades": modalidades,
			"procesos": procesosModalidad,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarCantidadDeProcesosMes(APIView):

	def get(self, request, format=None):
		procesos_mes = []
		promedios_mes = []
		lista_meses = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"cantidad_procesos": 0,
				"promedio_procesos": 0
			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')

		# Temporal 
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)

		# Agregados
		s.aggs.metric(
			'total_procesos',
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		s.aggs.metric(
			'procesos_por_mes', 
			'date_histogram', 
			field='doc.compiledRelease.date',
			interval= "month",
			format= "MM"
		)
		
		results = s.execute()

		total_procesos = results.aggregations.total_procesos["value"]

		aggs = results.aggregations.procesos_por_mes.to_dict()

		for bucket in aggs["buckets"]:
			if bucket["key_as_string"] in meses:

				count = bucket["doc_count"]

				meses[bucket["key_as_string"]] = {
					"cantidad_procesos": meses[bucket["key_as_string"]]["cantidad_procesos"] + count,
				}

		for mes in meses:
			procesos_mes.append(meses[mes]["cantidad_procesos"])
			lista_meses.append(NombreDelMes(mes))
			if total_procesos == 0:
				promedios_mes.append(0)
			else:
				promedios_mes.append(meses[mes]["cantidad_procesos"]/total_procesos)

		resultados = {
			"cantidadprocesos": procesos_mes,
			"promedioprocesos": promedios_mes,
			"meses": lista_meses,
			"totalprocesos": total_procesos,
			# "elastic": results.aggregations.to_dict()
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EstadisticaCantidadDeProcesos(APIView):

	def get(self, request, format=None):
		meses = {}
		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"cantidad_procesos": 0,
			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')

		# Temporal 
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)

		#Agregados
		s.aggs.metric(
			'procesos_por_mes', 
			'date_histogram', 
			field='doc.compiledRelease.tender.datePublished',
			interval= "month",
			format= "MM"
		)

		results = s.execute()

		aggs = results.aggregations.procesos_por_mes.to_dict()

		cantidad_por_meses = []
		total_procesos = 0

		for bucket in aggs["buckets"]:
			meses[bucket["key_as_string"]]["cantidad_procesos"] += bucket["doc_count"]
			total_procesos += bucket["doc_count"]

		for m in meses:
			cantidad_por_meses.append(meses[m]["cantidad_procesos"])			

		fechaActual = datetime.date.today()

		anioActual = str(fechaActual.year)
		mesActual = fechaActual.month

		if anio == anioActual:
			cantidad_por_meses = cantidad_por_meses[0:mesActual - 1]

		resultados = {
			"promedio": statistics.mean(cantidad_por_meses),
			"mayor": max(cantidad_por_meses),
			"menor": min(cantidad_por_meses),
			"total": total_procesos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarProcesosPorEtapa(APIView):

	def get(self, request, format=None):

		secciones = []
		procesosSeccion = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')

		# Temporal 
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)			
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=modalidad)

		# Agregados
		s.aggs.metric(
			'totalProcesos',
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		s.aggs.metric(
			'procesosPorSeccion', 
			'terms', 
			missing='No Definido',
			field='extra.lastSection.keyword' 
		)
		
		results = s.execute()

		totalProcesos = results.aggregations.totalProcesos["value"]

		aggs = results.aggregations.procesosPorSeccion.to_dict()

		for bucket in aggs["buckets"]:
			secciones.append(bucket["key"])
			procesosSeccion.append(bucket["doc_count"])

		resultados = {
			"etapas": secciones,
			"procesos": procesosSeccion
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarMontosDeContratosMes(APIView):

	def get(self, request, format=None):
		montos_contratos_mes = []
		cantidad_contratos_mes = []
		lista_meses = []
		porcentaje_montos_mes = []
		porcentaje_contratos_mes = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"monto_contratos": 0,
				"cantidad_contratos": 0,			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		# Agregados

		s.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		s.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		ss.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		s.aggs.metric(
			'procesosPorMesFechaFirma', 
			'date_histogram', 
			field='dateSigned',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		ss.aggs.metric(
			'procesosPorMesFechaInicio', 
			'date_histogram', 
			field='period.startDate',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		s.aggs["procesosPorMesFechaFirma"].metric(
			'suma_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["procesosPorMesFechaInicio"].metric(
			'suma_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		contratosPC = s.execute()
		contratosDD = ss.execute()

		total_contratos = 0

		total_monto_contratado = contratosPC.aggregations.suma_total_contratos["value"]
		total_cantidad_contratos = contratosPC.aggregations.cantidad_total_contratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.suma_total_contratos["value"]
			total_cantidad_contratos += contratosDD.aggregations.cantidad_total_contratos["value"]

		montosContratosPC = contratosPC.aggregations.procesosPorMesFechaFirma.to_dict()
		montosContratosDD = contratosDD.aggregations.procesosPorMesFechaInicio.to_dict()

		contratos = []
		for valor in montosContratosPC["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		for valor in montosContratosDD["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		if contratos:
			dfContratos = pd.DataFrame(contratos)

			agregaciones = {
				"cantidad": 'sum',
				"monto": 'sum'
			}

			dfContratos = dfContratos.groupby(['mes', 'mesNumero'], as_index=True).agg(agregaciones).reset_index().sort_values("mesNumero", ascending=True)

			contratos = dfContratos.to_dict('records')

		for m in contratos:
			montos_contratos_mes.append(m["monto"])
			cantidad_contratos_mes.append(m["cantidad"])
			lista_meses.append(m['mes'])

			if total_monto_contratado > 0:
				porcentaje_montos_mes.append(m["monto"]/total_monto_contratado)
			else:
				porcentaje_montos_mes.append(0)

			if total_cantidad_contratos > 0:
				porcentaje_contratos_mes.append(m["cantidad"]/total_cantidad_contratos)
			else:
				porcentaje_contratos_mes.append(0)

		resultados = {
			# "contratos": contratos,
			"meses": lista_meses,
			"monto_contratos_mes": montos_contratos_mes,
			"porcentaje_montos_mes": porcentaje_montos_mes,
			"cantidad_contratos_mes": cantidad_contratos_mes,
			"porcentaje_cantidad_contratos_mes": porcentaje_contratos_mes,
			"total_monto_contratado": total_monto_contratado,
			"total_cantidad_contratos": total_cantidad_contratos,
			# "elastic": contratosPC.aggregations.to_dict()
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EstadisticaCantidadDeContratos(APIView):

	def get(self, request, format=None):
		montos_contratos_mes = []
		cantidad_contratos_mes = []
		lista_meses = []
		porcentaje_montos_mes = []
		porcentaje_contratos_mes = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"monto_contratos": 0,
				"cantidad_contratos": 0,			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
		# Agregados

		s.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='value.amount'
		)

		ss.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='value.amount'
		)

		s.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		ss.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		s.aggs.metric(
			'procesosPorMesFechaFirma', 
			'date_histogram', 
			field='dateSigned',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		ss.aggs.metric(
			'procesosPorMesFechaInicio', 
			'date_histogram', 
			field='period.startDate',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		s.aggs["procesosPorMesFechaFirma"].metric(
			'suma_contratos',
			'sum',
			field='value.amount'
		)

		ss.aggs["procesosPorMesFechaInicio"].metric(
			'suma_contratos',
			'sum',
			field='value.amount'
		)

		# #Borrar esta linea. 
		# return DescargarContratosCSV(request, ss)

		contratosPC = s.execute()
		contratosDD = ss.execute()

		total_contratos = 0

		total_monto_contratado = contratosPC.aggregations.suma_total_contratos["value"]
		total_cantidad_contratos = contratosPC.aggregations.cantidad_total_contratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.suma_total_contratos["value"]
			total_cantidad_contratos += contratosDD.aggregations.cantidad_total_contratos["value"]

		montosContratosPC = contratosPC.aggregations.procesosPorMesFechaFirma.to_dict()
		montosContratosDD = contratosDD.aggregations.procesosPorMesFechaInicio.to_dict()

		contratos = []
		for valor in montosContratosPC["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		for valor in montosContratosDD["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		if contratos:
			dfContratos = pd.DataFrame(contratos)

			agregaciones = {
				"cantidad": 'sum',
				"monto": 'sum'
			}

			dfContratos = dfContratos.groupby(['mes', 'mesNumero'], as_index=True).agg(agregaciones).reset_index().sort_values("mesNumero", ascending=True)

			contratos = dfContratos.to_dict('records')

		for m in contratos:
			montos_contratos_mes.append(m["monto"])
			cantidad_contratos_mes.append(m["cantidad"])
			lista_meses.append(m['mes'])

			if total_monto_contratado > 0:
				porcentaje_montos_mes.append(m["monto"]/total_monto_contratado)
			else:
				porcentaje_montos_mes.append(0)

			if total_cantidad_contratos > 0:
				porcentaje_contratos_mes.append(m["cantidad"]/total_cantidad_contratos)
			else:
				porcentaje_contratos_mes.append(0)

		resultados = {
			"promedio": statistics.mean(cantidad_contratos_mes) if len(cantidad_contratos_mes) > 0 else 0,
			"mayor": max(cantidad_contratos_mes) if len(cantidad_contratos_mes) > 0 else 0,
			"menor": min(cantidad_contratos_mes) if len(cantidad_contratos_mes) > 0 else 0,
			"total": total_cantidad_contratos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class EstadisticaMontosDeContratos(APIView):

	def get(self, request, format=None):
		montos_contratos_mes = []
		cantidad_contratos_mes = []
		lista_meses = []
		porcentaje_montos_mes = []
		porcentaje_contratos_mes = []
		meses = {}

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		mm = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"] 

		for x in mm:
			meses[str(x)] = {
				"monto_contratos": 0,
				"cantidad_contratos": 0,			}

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id__keyword=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='extra.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
		
		# Agregados

		s.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'suma_total_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		s.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		ss.aggs.metric(
			'cantidad_total_contratos',
			'value_count',
			field='id.keyword'
		)

		s.aggs.metric(
			'procesosPorMesFechaFirma', 
			'date_histogram', 
			field='dateSigned',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		ss.aggs.metric(
			'procesosPorMesFechaInicio', 
			'date_histogram', 
			field='period.startDate',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		s.aggs["procesosPorMesFechaFirma"].metric(
			'suma_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["procesosPorMesFechaInicio"].metric(
			'suma_contratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		contratosPC = s.execute()
		contratosDD = ss.execute()

		total_contratos = 0

		total_monto_contratado = contratosPC.aggregations.suma_total_contratos["value"]
		total_cantidad_contratos = contratosPC.aggregations.cantidad_total_contratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.suma_total_contratos["value"]
			total_cantidad_contratos += contratosDD.aggregations.cantidad_total_contratos["value"]

		montosContratosPC = contratosPC.aggregations.procesosPorMesFechaFirma.to_dict()
		montosContratosDD = contratosDD.aggregations.procesosPorMesFechaInicio.to_dict()

		contratos = []
		for valor in montosContratosPC["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		for valor in montosContratosDD["buckets"]:
			contratos.append({
				"mesNumero": valor["key_as_string"],
				"mes": NombreDelMes(valor["key_as_string"]),
				"cantidad": valor["doc_count"],
				"monto": valor["suma_contratos"]["value"]
			})


		if contratos:
			dfContratos = pd.DataFrame(contratos)

			agregaciones = {
				"cantidad": 'sum',
				"monto": 'sum'
			}

			dfContratos = dfContratos.groupby(['mes', 'mesNumero'], as_index=True).agg(agregaciones).reset_index().sort_values("mesNumero", ascending=True)

			contratos = dfContratos.to_dict('records')

		for m in contratos:
			montos_contratos_mes.append(m["monto"])
			cantidad_contratos_mes.append(m["cantidad"])
			lista_meses.append(m['mes'])

			if total_monto_contratado > 0:
				porcentaje_montos_mes.append(m["monto"]/total_monto_contratado)
			else:
				porcentaje_montos_mes.append(0)

			if total_cantidad_contratos > 0:
				porcentaje_contratos_mes.append(m["cantidad"]/total_cantidad_contratos)
			else:
				porcentaje_contratos_mes.append(0)

		resultados = {
			"promedio": statistics.mean(montos_contratos_mes) if len(montos_contratos_mes) > 0 else 0,
			"mayor": max(montos_contratos_mes) if len(montos_contratos_mes) > 0 else 0,
			"menor": min(montos_contratos_mes) if len(montos_contratos_mes) > 0 else 0,
			"total": total_monto_contratado
		}
		
		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarContratosPorCategorias(APIView):

	def get(self, request, format=None):

		categorias = []
		procesosCategoria = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
		
		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		ss.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		s.aggs["contratosPorCategorias"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorCategorias"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		#Borrar estas lineas
		# print("Resultados")
		# return DescargarContratosCSV(request, s)

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorCategorias.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorCategorias.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		categorias = []

		for valor in montosContratosPC["buckets"]:
			categorias.append({
				"name": valor["key"],
				"value": valor["sumaContratos"]["value"],
			})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				categorias.append({
					"name": valor["key"],
					"value": valor["sumaContratos"]["value"],
				})

		if categorias:
			dfCategorias = pd.DataFrame(categorias)

			agregaciones = {
				"value": 'sum',
			}

			dfCategorias = dfCategorias.groupby('name', as_index=True).agg(agregaciones).reset_index().sort_values("value", ascending=False)

			categorias = dfCategorias.to_dict('records')

		resultados = {
			"categorias": categorias,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["`modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarContratosPorModalidad(APIView):

	def get(self, request, format=None):

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorModalidades', 
			'terms', 
			missing='No Definido',
			field='extra.tenderProcurementMethodDetails.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorModalidades', 
			'terms', 
			missing='No Definido',
			field='extra.tenderProcurementMethodDetails.keyword',
			size=10000
		)

		s.aggs["contratosPorModalidades"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorModalidades"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorModalidades.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorModalidades.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		modalidades = []

		for valor in montosContratosPC["buckets"]:
			modalidades.append({
				"name": valor["key"],
				"value": valor["sumaContratos"]["value"],
			})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				modalidades.append({
					"name": valor["key"],
					"value": valor["sumaContratos"]["value"],
				})

		if modalidades:
			dfModalidades = pd.DataFrame(modalidades)

			agregaciones = {
				"value": 'sum',
			}

			dfModalidades = dfModalidades.groupby('name', as_index=True).agg(agregaciones).reset_index().sort_values("value", ascending=False)

			modalidades = dfModalidades.to_dict('records')

		resultados = {
			"modalidades": modalidades,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["`modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class TopCompradoresPorMontoContratado(APIView):

	def get(self, request, format=None):

		codigoCompradores = []
		nombreCompradores = []
		totalContratado = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		ss.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorComprador.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorComprador.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		compradores = []

		for valor in montosContratosPC["buckets"]:
			for comprador in valor["nombreComprador"]["buckets"]:
				compradores.append({
					"codigo": valor["key"],
					"nombre": comprador["key"],
					"montoContratado": comprador["sumaContratos"]["value"],
				})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				for comprador in valor["nombreComprador"]["buckets"]:
					compradores.append({
						"codigo": valor["key"],
						"nombre": comprador["key"],
						"montoContratado": comprador["sumaContratos"]["value"]
					})

		if compradores:
			dfCompradores = pd.DataFrame(compradores)

			agregaciones = {
				"nombre": 'first',
				"montoContratado": 'sum',
			}

			dfCompradores = dfCompradores.groupby('codigo', as_index=True).agg(agregaciones).reset_index().sort_values("montoContratado", ascending=False)

			compradores = dfCompradores[0:10].to_dict('records')

			for c in compradores:
				codigoCompradores.append(c["codigo"])
				nombreCompradores.append(c["nombre"])
				totalContratado.append(c["montoContratado"])

		codigoCompradores.reverse()
		nombreCompradores.reverse()
		totalContratado.reverse()

		resultados = {
			"codigoCompradores": codigoCompradores,
			"nombreCompradores": nombreCompradores,
			"montoContratado": totalContratado,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["`modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class TopProveedoresPorMontoContratado(APIView):

	def get(self, request, format=None):

		codigoProveedores = []
		nombreProveedores = []
		totalContratado = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='suppliers.id.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='suppliers.id.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='suppliers.name.keyword',
			size=10000
		)

		ss.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='suppliers.name.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorComprador.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorComprador.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		compradores = []

		for valor in montosContratosPC["buckets"]:
			for comprador in valor["nombreComprador"]["buckets"]:
				compradores.append({
					"codigo": valor["key"],
					"nombre": comprador["key"],
					"montoContratado": comprador["sumaContratos"]["value"],
				})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				for comprador in valor["nombreComprador"]["buckets"]:
					compradores.append({
						"codigo": valor["key"],
						"nombre": comprador["key"],
						"montoContratado": comprador["sumaContratos"]["value"]
					})

		if compradores:
			dfCompradores = pd.DataFrame(compradores)

			agregaciones = {
				"nombre": 'first',
				"montoContratado": 'sum',
			}

			dfCompradores = dfCompradores.groupby('codigo', as_index=True).agg(agregaciones).reset_index().sort_values("montoContratado", ascending=False)

			compradores = dfCompradores[0:10].to_dict('records')

			for c in compradores:
				codigoProveedores.append(c["codigo"])
				nombreProveedores.append(c["nombre"])
				totalContratado.append(c["montoContratado"])

		codigoProveedores.reverse()
		nombreProveedores.reverse()
		totalContratado.reverse()

		resultados = {
			"codigoProveedores": codigoProveedores,
			"nombreProveedores": nombreProveedores,
			"montoContratado": totalContratado,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["`modalidad"] = modalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class GraficarProcesosTiposPromediosPorEtapa(APIView):

	def get(self, request, format=None):

		categorias = []
		procesosCategoria = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		pcategoria = request.GET.get('categoria', '')
		pmodalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=OCDS_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', doc__compiledRelease__sources__id=settings.SOURCE_SEFIN_ID)
		s = s.filter('exists', field='doc.compiledRelease.tender.id')
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		# Temporal 
		s = s.filter('exists', field='doc.compiledRelease.tender.localProcurementCategory')

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='doc.compiledRelease.contracts.value.currency') 
				qqqMoneda = Q('nested', path='doc.compiledRelease.contracts', query=qqMoneda)
				qMoneda = Q('bool', must_not=qqqMoneda)
				s = s.query(qMoneda)

				qMoneda = Q('exists', field='value.currency.keyword')
				ss = ss.filter('bool', must_not=qMoneda)	
			else:
				qMoneda = Q('match', doc__compiledRelease__contracts__value__currency=moneda) 
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qMoneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		if pcategoria.replace(' ', ''):
			if pcategoria == 'No Definido':
				qCategoria = Q('exists', field='doc.compiledRelease.tender.localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qCategoria)

				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__localProcurementCategory__keyword=pcategoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=pcategoria)

		if pmodalidad.replace(' ', ''):
			if pmodalidad == 'No Definido':
				qModalidad = Q('exists', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qModalidad)	

				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', doc__compiledRelease__tender__procurementMethodDetails__keyword=pmodalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=pmodalidad)

		# Agregados

		s.aggs.metric(
			'categorias',
			'terms',
			field='doc.compiledRelease.tender.localProcurementCategory.keyword'
		)

		ss.aggs.metric(
			'categorias',
			'terms',
			field='localProcurementCategory.keyword'
		)

		s.aggs['categorias'].metric(
			'modalidades',
			'terms',
			field='doc.compiledRelease.tender.procurementMethodDetails.keyword'
		)

		ss.aggs['categorias'].metric(
			'modalidades',
			'terms',
			field='extra.tenderProcurementMethodDetails.keyword'
		)

		s.aggs['categorias']['modalidades'].metric(
			'promedioDiasLicitacion',
			'avg',
			field='extra.daysTenderPeriod'
		)

		ss.aggs['categorias']['modalidades'].metric(
			'promedioDiasIniciarContrato',
			'avg',
			field='extra.tiempoContrato'
		)
		
		results = s.execute()
		results2 = ss.execute()

		diasLicitacion = results.aggregations.categorias.to_dict()
		diasContrato = results2.aggregations.categorias.to_dict()

		tiempos = {}

		for categoria in diasLicitacion["buckets"]:
			tiempos[categoria["key"]] = {}

			for modalidad in categoria["modalidades"]["buckets"]:
				tiempos[categoria["key"]][modalidad["key"]] = {}

				if "promedioDiasLicitacion" in modalidad:
					tiempos[categoria["key"]][modalidad["key"]]["promedioDiasLicitacion"] = modalidad["promedioDiasLicitacion"]["value"]
				else:
					tiempos[categoria["key"]][modalidad["key"]]["promedioDiasLicitacion"] = None

				tiempos[categoria["key"]][modalidad["key"]]["promedioDiasIniciarContrato"] = None


		for categoria in diasContrato["buckets"]:
			
			if not categoria["key"] in tiempos:
				tiempos[categoria["key"]] = {}

			for modalidad in categoria["modalidades"]["buckets"]:
				
				if not modalidad["key"] in tiempos[categoria["key"]]:
					tiempos[categoria["key"]][modalidad["key"]] = {}

				if "promedioDiasIniciarContrato" in modalidad:
					tiempos[categoria["key"]][modalidad["key"]]["promedioDiasIniciarContrato"] = modalidad["promedioDiasIniciarContrato"]["value"]
				else:
					tiempos[categoria["key"]][modalidad["key"]]["promedioDiasIniciarContrato"] = None

				if not "promedioDiasIniciarContrato" in tiempos[categoria["key"]][modalidad["key"]]:
					tiempos[categoria["key"]][modalidad["key"]]["promedioDiasLicitacion"] = None

		resultados = tiempos

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = pcategoria
		parametros["modalidad"] = pmodalidad

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

# Indicadores de ONCAE

class IndicadorMontoContratadoPorCategoria(APIView):

	def get(self, request, format=None):

		categorias = []
		procesosCategoria = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		if sistema.replace(' ', ''):
			s = s.filter('match_phrase', extra__sources__id__keyword=sistema)
			ss = ss.filter('match_phrase', extra__sources__id__keyword=sistema)

		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		ss.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		s.aggs["contratosPorCategorias"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorCategorias"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorCategorias.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorCategorias.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		categorias = []

		for valor in montosContratosPC["buckets"]:
			categorias.append({
				"name": valor["key"],
				"value": valor["sumaContratos"]["value"],
			})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				categorias.append({
					"name": valor["key"],
					"value": valor["sumaContratos"]["value"],
				})

		if categorias:
			dfCategorias = pd.DataFrame(categorias)

			agregaciones = {
				"value": 'sum',
			}

			dfCategorias = dfCategorias.groupby('name', as_index=True).agg(agregaciones).reset_index().sort_values("value", ascending=False)

			categorias = dfCategorias.to_dict('records')

		resultados = {
			"categorias": categorias,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class IndicadorCantidadProcesosPorCategoria(APIView):

	def get(self, request, format=None):

		categorias = []
		procesosCategoria = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		if sistema.replace(' ', ''):
			s = s.filter('match_phrase', extra__sources__id__keyword=sistema)
			ss = ss.filter('match_phrase', extra__sources__id__keyword=sistema)

		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		s.aggs["contratosPorCategorias"].metric(		
			'conteoOCID', 
			'cardinality', 
			precision_threshold=1000, 
			field='extra.ocid.keyword'
		)


		ss.aggs.metric(
			'contratosPorCategorias', 
			'terms', 
			missing='No Definido',
			field='localProcurementCategory.keyword' 
		)

		ss.aggs["contratosPorCategorias"].metric(		
			'conteoOCID', 
			'cardinality', 
			precision_threshold=1000, 
			field='extra.ocid.keyword'
		)

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorCategorias.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorCategorias.to_dict()

		# total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		# if anio.replace(' ', ''):
		# 	total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		categorias = []

		for valor in montosContratosPC["buckets"]:
			categorias.append({
				"name": valor["key"],
				"value": valor["doc_count"],
				# "value": valor["conteoOCID"]["value"]
			})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				categorias.append({
					"name": valor["key"],
					"value": valor["doc_count"],
					# "value": valor["conteoOCID"]["value"],
				})

		if categorias:
			dfCategorias = pd.DataFrame(categorias)

			agregaciones = {
				"value": 'sum',
			}

			dfCategorias = dfCategorias.groupby('name', as_index=True).agg(agregaciones).reset_index().sort_values("value", ascending=False)

			categorias = dfCategorias.to_dict('records')

		resultados = {
			"categorias": categorias,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class IndicadorTopCompradores(APIView):

	def get(self, request, format=None):

		codigoCompradores = []
		nombreCompradores = []
		totalContratado = []
		cantidadProcesos = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)
				
		if sistema.replace(' ', ''):
			s = s.filter('match_phrase', extra__sources__id__keyword=sistema)
			ss = ss.filter('match_phrase', extra__sources__id__keyword=sistema)

		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		ss.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		s.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'contadorOCIDs',
			'cardinality',
			precision_threshold=1000,
			field='extra.ocid.keyword'
		)

		ss.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'contadorOCIDs',
			'cardinality',
			precision_threshold=1000,
			field='extra.ocid.keyword'
		)


		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorComprador.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorComprador.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		compradores = []

		for valor in montosContratosPC["buckets"]:
			for comprador in valor["nombreComprador"]["buckets"]:
				compradores.append({
					"codigo": valor["key"],
					"nombre": comprador["key"],
					"montoContratado": comprador["sumaContratos"]["value"],
					"ocids": comprador["contadorOCIDs"]["value"]
				})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				for comprador in valor["nombreComprador"]["buckets"]:
					compradores.append({
						"codigo": valor["key"],
						"nombre": comprador["key"],
						"montoContratado": comprador["sumaContratos"]["value"],
						"ocids": comprador["contadorOCIDs"]["value"]
					})

		if compradores:
			dfCompradores = pd.DataFrame(compradores)

			agregaciones = {
				"nombre": 'first',
				"montoContratado": 'sum',
				"ocids": 'sum'
			}

			dfCompradores = dfCompradores.groupby('codigo', as_index=True).agg(agregaciones).reset_index().sort_values("montoContratado", ascending=False)

			compradores = dfCompradores[0:10].to_dict('records')

			for c in compradores:
				codigoCompradores.append(c["codigo"])
				nombreCompradores.append(c["nombre"])
				totalContratado.append(c["montoContratado"])
				cantidadProcesos.append(c["ocids"])

		codigoCompradores.reverse()
		nombreCompradores.reverse()
		totalContratado.reverse()
		cantidadProcesos.reverse()

		resultados = {
			"codigoCompradores": codigoCompradores,
			"nombreCompradores": nombreCompradores,
			"montoContratado": totalContratado,
			"cantidadOCIDs": cantidadProcesos,
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class IndicadorCatalogoElectronico(APIView):

	def get(self, request, format=None):

		nombreCatalogo = []
		totalContratado = []
		cantidadProcesos = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		s = s.filter('match_phrase', extra__sources__id='catalogo-electronico')

		#Solo ordenes de compra en estado impreso
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		s = s.exclude(estadoOC)

		# Source 
		campos = ['items.unit','items.quantity', 'items.extra', 'items.attributes']
		# s = s.source(campos)

		# Excluir compra conjunta asd
		qTerm = Q('match', items__attributes__value='compra conjunta')
		s = s.exclude('nested', path='items', query=qTerm)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)

		# Agregados
		s.aggs.metric(
			'items', 
			'nested', 
			path='items'
		)

		s.aggs["items"].metric(
			'sumaTotalMontos',
			'sum',
			field='items.extra.total'
		)
		
		s.aggs["items"].metric(
			'porCatalogo', 
			'terms', 
			missing='CONVENIO MARCO',
			field='items.attributes.value.keyword',
			order={'montoContratado': 'desc'},
			size=10000
		)

		s.aggs["items"]["porCatalogo"].metric(
			'montoContratado', 
			'sum', 
			field='items.extra.total'
		)

		s.aggs["items"]["porCatalogo"].metric(
			CONTRACT_INDEX, 
			'reverse_nested'
		)

		s.aggs["items"]["porCatalogo"]["contract"].metric(
			'contadorOCIDs',
			'cardinality',
			precision_threshold=10000,
			field='extra.ocid.keyword'
		)

		#Borrar estas lineas
		# print("Resultados")
		# return descargar_contratos_csv(request, s)
		# return DescargarProductosCSV(request, s)

		contratosCE = s.execute()

		itemsCE = contratosCE.aggregations.items.porCatalogo.to_dict()

		# catalogos = []
		for c in itemsCE["buckets"]:

			nombreCatalogo.append(c["key"].upper())
			totalContratado.append(c["montoContratado"]["value"])
			cantidadProcesos.append(c["contract"]["contadorOCIDs"]["value"])

		nombreCatalogo.reverse()
		totalContratado.reverse()
		cantidadProcesos.reverse()

		resultados = {
			"nombreCatalogos": nombreCatalogo,
			"montoContratado": totalContratado,
			"cantidadProcesos": cantidadProcesos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class IndicadorCompraConjunta(APIView):

	def get(self, request, format=None):

		nombreCatalogo = []
		totalContratado = []
		cantidadProcesos = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		s = s.filter('match_phrase', extra__sources__id='catalogo-electronico')

		#Solo ordenes de compra en estado impreso
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		s = s.exclude(estadoOC)

		# Source 
		campos = ['items.unit','items.quantity', 'items.extra', 'items.attributes']
		# s = s.source(campos)

		# Solo compras conjuntas
		qTerm = Q('match', items__attributes__value='compra conjunta')
		s = s.query('nested', path='items', query=qTerm)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)

		# Agregados
		s.aggs.metric(
			'items', 
			'nested', 
			path='items'
		)

		s.aggs["items"].metric(
			'sumaTotalMontos',
			'sum',
			field='items.extra.total'
		)
		
		s.aggs["items"].metric(
			'porCatalogo', 
			'terms', 
			missing='CONVENIO MARCO',
			field='items.attributes.value.keyword',
			order={'montoContratado': 'desc'},
			size=10000
		)

		s.aggs["items"]["porCatalogo"].metric(
			'montoContratado', 
			'sum', 
			field='items.extra.total'
		)

		s.aggs["items"]["porCatalogo"].metric(
			CONTRACT_INDEX, 
			'reverse_nested'
		)

		s.aggs["items"]["porCatalogo"]["contract"].metric(
			'contadorOCIDs',
			'cardinality',
			precision_threshold=10000,
			field='extra.ocid.keyword'
		)

		contratosCE = s.execute()

		itemsCE = contratosCE.aggregations.items.porCatalogo.to_dict()

		# catalogos = []
		for c in itemsCE["buckets"]:

			nombreCatalogo.append(c["key"].upper())
			totalContratado.append(c["montoContratado"]["value"])
			cantidadProcesos.append(c["contract"]["contadorOCIDs"]["value"])

		nombreCatalogo.reverse()
		totalContratado.reverse()
		cantidadProcesos.reverse()

		resultados = {
			"nombreCatalogos": nombreCatalogo,
			"montoContratado": totalContratado,
			"cantidadProcesos": cantidadProcesos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

class IndicadorContratosPorModalidad(APIView):

	def get(self, request, format=None):

		nombreModalidades = []
		cantidadContratos = []
		montosContratos = []

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('año', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')
		sistema = request.GET.get('sistema', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		## Solo contratos de ordenes de compra en estado impreso. 
		sistemaCE = Q('match_phrase', extra__sources__id='catalogo-electronico')
		estadoOC = ~Q('match_phrase', statusDetails='Impreso')
		ss = ss.exclude(sistemaCE & estadoOC)

		## Quitando contratos cancelados en difusion directa. 
		sistemaDC = Q('match_phrase', extra__sources__id='difusion-directa-contrato')
		estadoContrato = Q('match_phrase', statusDetails='Cancelado')
		ss = ss.exclude(sistemaDC & estadoContrato)

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		if sistema.replace(' ', ''):
			s = s.filter('match_phrase', extra__sources__id__keyword=sistema)
			ss = ss.filter('match_phrase', extra__sources__id__keyword=sistema)

		# Agregados
		s.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs.metric(
			'sumaTotalContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)
		
		s.aggs.metric(
			'contratosPorModalidades', 
			'terms', 
			missing='No Definido',
			field='extra.tenderProcurementMethodDetails.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorModalidades', 
			'terms', 
			missing='No Definido',
			field='extra.tenderProcurementMethodDetails.keyword',
			size=10000
		)

		s.aggs["contratosPorModalidades"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)

		ss.aggs["contratosPorModalidades"].metric(
			'sumaContratos',
			'sum',
			field='extra.LocalCurrency.amount'
		)		

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorModalidades.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorModalidades.to_dict()

		total_monto_contratado = contratosPC.aggregations.sumaTotalContratos["value"]

		if anio.replace(' ', ''):
			total_monto_contratado += contratosDD.aggregations.sumaTotalContratos["value"]

		modalidades = []

		for valor in montosContratosPC["buckets"]:
			modalidades.append({
				"modalidad": valor["key"],
				"cantidadContratos": valor["doc_count"],
				"montosContratos": valor["sumaContratos"]["value"],
			})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				modalidades.append({
					"modalidad": valor["key"],
					"cantidadContratos": valor["doc_count"],
					"montosContratos": valor["sumaContratos"]["value"],
				})

		if modalidades:
			dfModalidades = pd.DataFrame(modalidades)

			agregaciones = {
				"cantidadContratos": 'sum',
				"montosContratos": 'sum',
			}

			dfModalidades = dfModalidades.groupby('modalidad', as_index=True).agg(agregaciones).reset_index().sort_values("montosContratos", ascending=False)

			modalidades = dfModalidades.to_dict('records')

		for m in modalidades:
			# print(m)
			nombreModalidades.append(m["modalidad"])
			cantidadContratos.append(m["cantidadContratos"])
			montosContratos.append(m["montosContratos"])

		resultados = {
			# "modalidades": modalidades,
			"nombreModalidades": nombreModalidades,
			"cantidadContratos": cantidadContratos,
			"montosContratos": montosContratos
		}

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["año"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad
		parametros["sistema"] = sistema

		context = {
			"resultados": resultados,
			"parametros": parametros
		}

		return Response(context)

# Visualizaciones de ONCAE

class CompradoresPorCantidadDeContratos(APIView):

	def get(self, request, format=None):
		anioActual = str(datetime.date.today().year)

		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		anio = request.GET.get('anio', anioActual)
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		s = Search(using=cliente, index=CONTRACT_INDEX)
		ss = Search(using=cliente, index=CONTRACT_INDEX)

		s = s.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		ss = ss.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		try:
			int(anio)
		except Exception as e:
			anio = anioActual

		# # Filtros
		if institucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			ss = ss.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			s = s.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			ss = ss.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if anio.replace(' ', ''):
			s = s.filter('range', dateSigned={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})
			ss = ss.filter('range', period__startDate={'gte': datetime.date(int(anio), 1, 1), 'lt': datetime.date(int(anio)+1, 1, 1)})

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqCategoria)
				ss = ss.filter('bool', must_not=qqCategoria)
			else:
				s = s.filter('match_phrase', localProcurementCategory__keyword=categoria)
				ss = ss.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':
				qqModalidad = Q('exists', field='localProcurementCategory.keyword')
				s = s.filter('bool', must_not=qqModalidad)
				ss = ss.filter('bool', must_not=qqModalidad)
			else:
				s = s.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				ss = ss.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qqMoneda = Q('exists', field='value.currency.keyword')
				s = s.filter('bool', must_not=qqMoneda)
				ss = ss.filter('bool', must_not=qqMoneda)			
			else:
				s = s.filter('match_phrase', value__currency__keyword=moneda)
				ss = ss.filter('match_phrase', value__currency__keyword=moneda)

		# Agregados

		s.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		ss.aggs.metric(
			'contratosPorComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.id.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		ss.aggs["contratosPorComprador"].metric(
			'nombreComprador', 
			'terms', 
			missing='No Definido',
			field='extra.parentTop.name.keyword',
			size=10000
		)

		s.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'procesosPorMes', 
			'date_histogram', 
			field='dateSigned',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		ss.aggs["contratosPorComprador"]["nombreComprador"].metric(
			'procesosPorMes', 
			'date_histogram', 
			field='period.startDate',
			interval= "month",
			format= "MM",
			min_doc_count=1
		)

		contratosPC = s.execute()
		contratosDD = ss.execute()

		montosContratosPC = contratosPC.aggregations.contratosPorComprador.to_dict()
		montosContratosDD = contratosDD.aggregations.contratosPorComprador.to_dict()

		compradores = []

		for valor in montosContratosPC["buckets"]:
			for comprador in valor["nombreComprador"]["buckets"]:
				for mes in comprador["procesosPorMes"]["buckets"]:
					compradores.append({
						"anio": anio,
						"mes": mes["key_as_string"],
						"codigo": valor["key"],
						"nombre": comprador["key"],
						"cantidad": mes["doc_count"],
					})

		if anio.replace(' ', ''):
			for valor in montosContratosDD["buckets"]:
				for comprador in valor["nombreComprador"]["buckets"]:
					for mes in comprador["procesosPorMes"]["buckets"]:
						compradores.append({
							"anio": anio,
							"mes": mes["key_as_string"],
							"codigo": valor["key"],
							"nombre": comprador["key"],
							"cantidad": mes["doc_count"],
						})

		if compradores:
			dfCompradores = pd.DataFrame(compradores)

			agregaciones = {
				"cantidad": 'sum',
			}

			groupBy = ['anio','mes','codigo','nombre']

			dfCompradores = dfCompradores.groupby(groupBy, as_index=True).agg(agregaciones).reset_index().sort_values("cantidad", ascending=False)

			compradores = dfCompradores[0:10].to_dict('records')

		resultados = dfCompradores.to_dict('records')

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = institucion
		parametros["anio"] = anio
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"parametros": parametros,
			"resultados": resultados,
		}

		return Response(context)

class FiltrosVisualizacionesONCAE(APIView):

	def get(self, request, format=None):
		institucion = request.GET.get('institucion', '')
		idinstitucion = request.GET.get('idinstitucion', '')
		moneda = request.GET.get('moneda', '')
		categoria = request.GET.get('categoria', '')
		modalidad = request.GET.get('modalidad', '')

		cliente = ElasticSearchDefaultConnection()

		ssFecha = Search(using=cliente, index=CONTRACT_INDEX)
		sssFecha = Search(using=cliente, index=CONTRACT_INDEX)

		# Excluyendo procesos de SEFIN
		ssFecha = ssFecha.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)
		sssFecha = sssFecha.exclude('match_phrase', extra__sources__id=settings.SOURCE_SEFIN_ID)

		# Filtros
		if institucion.replace(' ', ''):
			ssFecha = ssFecha.filter('match_phrase', extra__parentTop__name__keyword=institucion)
			sssFecha = sssFecha.filter('match_phrase', extra__parentTop__name__keyword=institucion)

		if idinstitucion.replace(' ', ''):
			ssFecha = ssFecha.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)
			sssFecha = sssFecha.filter('match_phrase', extra__parentTop__id__keyword=idinstitucion)

		if categoria.replace(' ', ''):
			if categoria == 'No Definido':			
				qqCategoria = Q('exists', field='localProcurementCategory.keyword')
				ssFecha = ssFecha.filter('bool', must_not=qqCategoria)
				sssFecha = sssFecha.filter('bool', must_not=qqCategoria)
			else:
				ssFecha = ssFecha.filter('match_phrase', localProcurementCategory__keyword=categoria)
				sssFecha = sssFecha.filter('match_phrase', localProcurementCategory__keyword=categoria)

		if modalidad.replace(' ', ''):
			if modalidad == 'No Definido':			
				qqModalidad = Q('exists', field='extra.tenderProcurementMethodDetails.keyword')

				ssFecha = ssFecha.filter('bool', must_not=qqModalidad)
				sssFecha = sssFecha.filter('bool', must_not=qqModalidad)
			else:
				ssFecha = ssFecha.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)
				sssFecha = sssFecha.filter('match_phrase', extra__tenderProcurementMethodDetails__keyword=modalidad)

		if moneda.replace(' ', ''):
			if moneda == 'No Definido':
				qMoneda = Q('exists', field='value.currency.keyword') 

				ssFecha = ssFecha.filter('bool', must_not=qMoneda)
				sssFecha = sssFecha.filter('bool', must_not=qMoneda)
			else:
				ssFecha = ssFecha.filter('match_phrase', value__currency__keyword=moneda)
				sssFecha = sssFecha.filter('match_phrase', value__currency__keyword=moneda)

		# Resumen
		ssFecha.aggs.metric(
			'aniosContratoFechaFirma', 
			'date_histogram', 
			field='dateSigned', 
			interval='year', 
			format='yyyy',
			min_doc_count=1
		)

		sssFecha.aggs.metric(
			'aniosContratoFechaInicio', 
			'date_histogram', 
			field='period.startDate', 
			interval='year', 
			format='yyyy',
			min_doc_count=1
		)
		
		ssFechaResultados = ssFecha.execute()
		sssFechaResultados = sssFecha.execute()

		aniosFechaFirma = ssFechaResultados.aggregations.aniosContratoFechaFirma.to_dict()
		aniosFechaInicio = sssFechaResultados.aggregations.aniosContratoFechaInicio.to_dict()

		#Valores para filtros por anio
		anios = {}

		for value in aniosFechaFirma["buckets"]:
			if value["key_as_string"] in anios:
				if "contratos" in anios[value["key_as_string"]]:
					anios[value["key_as_string"]]["contratos"] += value["doc_count"]
				else:
					anios[value["key_as_string"]]["contratos"] = value["doc_count"]
			else:
				anios[value["key_as_string"]] = {}
				anios[value["key_as_string"]]["key_as_string"] = value["key_as_string"]
				anios[value["key_as_string"]]["contratos"] = value["doc_count"]

		for value in aniosFechaInicio["buckets"]:
			if value["key_as_string"] in anios:
				if "contratos" in anios[value["key_as_string"]]:
					anios[value["key_as_string"]]["contratos"] += value["doc_count"]
				else:
					anios[value["key_as_string"]]["contratos"] = value["doc_count"]
			else:
				anios[value["key_as_string"]] = {}
				anios[value["key_as_string"]]["key_as_string"] = value["key_as_string"]
				anios[value["key_as_string"]]["contratos"] = value["doc_count"]

		years = []
		annioActual = int(datetime.datetime.now().year)

		for key, value in anios.items():
			if int(value["key_as_string"]) <= annioActual and int(value["key_as_string"]) >= 1980:
				years.append(value)

		years = sorted(years, key=lambda k: k['key_as_string'], reverse=True) 

		resultados = years

		parametros = {}
		parametros["institucion"] = institucion
		parametros["idinstitucion"] = idinstitucion
		parametros["moneda"] = moneda
		parametros["categoria"] = categoria
		parametros["modalidad"] = modalidad

		context = {
			"parametros": parametros,
			"respuesta": resultados
		}

		return Response(context)

# Descargas

class Descargas(APIView):

	def get(self, request, format=None):

		listaArchivos = []
		urlDescargas = '/api/v1/descargas/'

		with connections['portaledcahn_admin'].cursor() as cursor:
			cursor.execute("SELECT file FROM descargas ORDER BY createddate DESC LIMIT 1")
			row = cursor.fetchone()
		
		if row:
			for value in row[0].values():
				for extension in value["urls"]:
					value["urls"][extension] = request.build_absolute_uri(urlDescargas + value["urls"][extension])

				if 'finalizo' in value: 
					del value['finalizo']

				if 'md5_hash' in value: 
					del value['md5_hash']

				if 'md5_json' in value: 
					del value['md5_json']

				listaArchivos.append(value)
	
			return Response(listaArchivos)
		else:
			return Response([])

class Descargar(APIView):

	def get(self, request, pk=None, format=None):
		file_name = pk
		response = HttpResponse()
		response['Content-Type'] = ''
		response['Content-Disposition'] = 'attachment; filename="{}"'.format(file_name)
		response['X-Accel-Redirect'] = '/protectedMedia/' + file_name
		return response

def DescargarProcesosCSV(request, search):
	nombreArchivo = 'portalocdspy-procesos-'
	pseudo_buffer = Echo()
	writer = csv.writer(pseudo_buffer)
	response = StreamingHttpResponse((writer.writerow(row) for row in generador_proceso_csv(search)), content_type='text/csv')
	response['Content-Disposition'] = 'attachment; filename="'+ nombreArchivo +'-{0}.csv"'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
	return response

def DescargarContratosCSV(request, search):
	nombreArchivo = 'portalocdspy-contratos-'
	pseudo_buffer = Echo()
	writer = csv.writer(pseudo_buffer)
	response = StreamingHttpResponse((writer.writerow(row) for row in generador_contrato_csv(search)), content_type='text/csv')
	response['Content-Disposition'] = 'attachment; filename="'+ nombreArchivo +'{0}.csv"'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
	return response

def DescargarProductosCSV(request, search):
	nombreArchivo = 'portalocdspy-productos-'
	pseudo_buffer = Echo()
	writer = csv.writer(pseudo_buffer)
	response = StreamingHttpResponse((writer.writerow(row) for row in generador_producto_csv(search)), content_type='text/csv')
	response['Content-Disposition'] = 'attachment; filename="'+ nombreArchivo +'{0}.csv"'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
	return response
