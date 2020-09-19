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
		sourceDNCP = 'dncp-sicp'

		cliente = ElasticSearchDefaultConnection()

		dncp = Search(using=cliente, index=OCDS_INDEX)

		redFlagsQuery = Search(using=cliente, index=OCDS_INDEX)

		dncp = dncp.filter('match_phrase', doc__compiledRelease__sources__id=sourceDNCP)

		redFlagsQuery = redFlagsQuery.filter('exists', field='banderas')

		dncp.aggs.metric(
			'contratos',
			'nested',
			path='doc.compiledRelease.contracts'
		)
		
		dncp.aggs["contratos"].metric(
			'distinct_contracts', 
			'cardinality',
			precision_threshold=precision, 
			field='doc.compiledRelease.contracts.id.keyword'
		)

		dncp.aggs.metric(
			'distinct_buyers',
			'cardinality',
			precision_threshold=precision,
			field='doc.compiledRelease.buyer.id.keyword'
		)
		
		dncp.aggs.metric(
			'procesos_contratacion', 
			'value_count',
			field='doc.compiledRelease.ocid.keyword'
		)

		dncp.aggs.metric(
			'red_flags',
			'value_count',
			field='banderas.title.keyword'
		)

		dncp.aggs.metric(
			'proveedores_dncp',
			'terms',
			field='doc.compiledRelease.awards.suppliers.name.keyword',
			size=100000
		)

		redFlagsQuery.aggs.metric(
			'red_flags',
			'value_count',
			field='banderas'
		)

		resultsDNCP = dncp.execute()

		resultsRedFlags = redFlagsQuery.execute()

		diccionario_proveedores = []
		dfProveedores = pd.DataFrame(resultsDNCP.aggregations.proveedores_dncp.to_dict()["buckets"])

		if not dfProveedores.empty:
			cantidad_proveedores = dfProveedores['key'].nunique()
		else:
			cantidad_proveedores = 0

		#print('Cantidad de proveedores ' + cantidad_proveedores )

		# dfProveedores.to_csv(r'proveedores.csv', sep='\t', encoding='utf-8')

		context = {
			"contratos": resultsDNCP.aggregations.contratos.distinct_contracts.value,
			"procesos": resultsDNCP.aggregations.procesos_contratacion.value,
			"redFlags": resultsDNCP.aggregations.red_flags.value,
			"uniqueRedFlags": resultsRedFlags.hits.total,
			"compradores": resultsDNCP.aggregations.distinct_buyers.value,
			"proveedores": cantidad_proveedores
		}

		return Response(context)

class Buscador(APIView):

	def get(self, request, format=None):
		precision = 40000
		noMoneda = 'Sin monto de contrato'
		noMonedaPago = 'Sin monto pagado'

		page = int(request.GET.get('pagina', '1'))
		metodo = request.GET.get('metodo', 'proceso')
		moneda = request.GET.get('moneda', '')
		redFlag = request.GET.get('redFlag', '')
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
		campos = ['doc.compiledRelease', 'extra', 'banderas']
		s = s.source(campos)
		#Filtros

		s.aggs.metric('redFlags', 'terms', field='banderas.title.keyword')
		s.aggs.metric('contratos', 'nested', path='doc.compiledRelease.contracts')

		s.aggs["contratos"].metric('monedas', 'terms', field='doc.compiledRelease.contracts.value.currency.keyword')
		s.aggs["contratos"]["monedas"].metric("nProcesos", "reverse_nested")

		

		s.aggs.metric('metodos_de_seleccion', 'terms', field='doc.compiledRelease.tender.procurementMethodDetails.keyword')

		s.aggs.metric('instituciones', 'terms', field='extra.parentTop.name.keyword', size=10000)

		s.aggs.metric('categorias', 'terms', field='doc.compiledRelease.tender.mainProcurementCategory.keyword')


		s.aggs.metric('organismosFinanciadores', 'terms', field='doc.compiledRelease.planning.budget.budgetBreakdown.classifications.financiador.keyword', size=2000)

		if metodo == 'contrato':
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

		s.aggs.metric(
			'distinct_proveedores_contratos', 
			'cardinality', 
			precision_threshold=precision, 
			field='doc.compiledRelease.awards.suppliers.name.keyword'
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
			s = s.filter('exists', field='doc.compiledRelease.tender.mainProcurementCategory')

			s.aggs.metric(
				'procesos_total',
				'value_count',
				field='doc.compiledRelease.ocid.keyword'
			)

		if metodo == 'contrato':
			filtro_contrato = Q('exists', field='doc.compiledRelease.contracts.id')
			s = s.query('nested', path='doc.compiledRelease.contracts', query=filtro_contrato)

			s.aggs["contratos"].metric(
				'procesos_total', 
				'cardinality', 
				precision_threshold=precision, 
				field='doc.compiledRelease.contracts.id.keyword'
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
			s = s.filter('match_phrase', doc__compiledRelease__tender__mainProcurementCategory=categoria)

		if year.replace(' ', ''):
			if metodo == 'pago' or metodo == 'contrato':
				s = s.filter('range', doc__compiledRelease__date={'gte': datetime.date(int(year), 1, 1), 'lt': datetime.date(int(year)+1, 1, 1)})
			else:
				s = s.filter('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': datetime.date(int(year), 1, 1), 'lt': datetime.date(int(year)+1, 1, 1)})

		if term:
			if metodo == 'proceso':
				s = s.filter('match', doc__compiledRelease__tender__title=term)

			if metodo in  ['contrato', 'pago']:
				qDescripcion = Q("wildcard", doc__compiledRelease__contracts__description='*'+term+'*')
				s = s.query('nested', path='doc.compiledRelease.contracts', query=qDescripcion)

		if organismo.replace(' ', ''):
			s = s.filter('match_phrase', doc__compiledRelease__planning__budget__budgetBreakdown__classifications__organismo=organismo)

		if redFlag.replace(' ', ''):
			if(len(redFlag.split(',')) == 1):
				s = s.query('match_phrase', **{'banderas.title.keyword': redFlag})
			else:
				for value in redFlag.split(','):
					s = s.query("match_phrase", **{'banderas.title.keyword':value})

		search_results = SearchResults(s)

		#ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		mappingSort = {
			"year":"doc.compiledRelease.date",
			"institucion":"doc.compiledRelease.buyer.name.keyword",
			"categoria": "doc.compiledRelease.tender.mainProcurementCategory.keyword",
			"modalidad": "doc.compiledRelease.tender.procurementMethodDetails.keyword",
			"proveedor": "doc.compiledRelease.contracts.implementation.transactions.payee.name.keyword",
			"monto": "doc.compiledRelease.contracts.extra.sumTransactions",
			"organismo":"doc.compiledRelease.planning.budget.budgetBreakdown.classifications.financiador.keyword",
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

		redFlags = results.aggregations.redFlags.buckets

		monedas = results.aggregations.contratos.monedas.buckets

		if results.hits.total > 0:

			if metodo == 'proceso':
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
		filtros["redFlags"] = results.aggregations.redFlags.to_dict()
		filtros["monedas"] = results.aggregations.contratos.monedas.to_dict()
		filtros["años"] = results.aggregations.años.to_dict()
		filtros["categorias"] = results.aggregations.categorias.to_dict()
		filtros["instituciones"] = results.aggregations.instituciones.to_dict()
		filtros["metodos_de_seleccion"] = results.aggregations.metodos_de_seleccion.to_dict()
		filtros["organismosFinanciadores"] = results.aggregations.organismosFinanciadores.to_dict()

		total_compradores = results.aggregations.compradores_total.value

		if metodo == 'contrato':
			monto_promedio = results.aggregations.contratos.promedio_montos_contrato.value
			total_procesos = results.aggregations.contratos.procesos_total.value
			total_proveedores = results.aggregations.distinct_proveedores_contratos.value
		elif metodo == 'proceso':
			monto_promedio = results.aggregations.contratos.promedio_montos_contrato.value
			total_procesos = results.aggregations.procesos_total.value
			total_proveedores = results.aggregations.distinct_proveedores_contratos.value
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
		parametros["redFlag"] = redFlag
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
			response = context[0]["_source"]
			return Response(response)
		else:
			raise Http404

class Compradores(APIView):

	def get(self, request, format=None):
		page = int(request.GET.get('pagina', '1'))
		nombre = request.GET.get('nombre', '')  # nombre
		identificacion = request.GET.get('identificacion', '')  # identificacion
		dependencias = request.GET.get('dependencias', '0')
		term = request.GET.get('term', '')  # palabra clave
		tmc = request.GET.get('tmc', '')  # total monto contratado
		pmc = request.GET.get('pmc', '')  # promedio monto contratado
		mamc = request.GET.get('mamc', '')  # mayor monto contratado
		memc = request.GET.get('memc', '')  # menor monto contratado
		fup = request.GET.get('fup', '')  # fecha ultimo proceso
		cp = request.GET.get('cp', '')  # cantidad de procesos

		ordenarPor = request.GET.get('ordenarPor', '')
		paginarPor = request.GET.get('paginarPor', settings.PAGINATE_BY)

		tipoIdentificador = request.GET.get('tid', 'nombre')  # por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page - 1) * settings.PAGINATE_BY
		end = start + settings.PAGINATE_BY

		cliente = Elasticsearch(settings.ELASTICSEARCH_DSL_HOST, timeout=settings.TIMEOUT_ES)

		s = Search(using=cliente, index=OCDS_INDEX)

		filtros = []
		if nombre.replace(' ', ''):
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
			s.aggs['compradores']['contratos'].metric('promedio', 'avg',
													  field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['contratos'].metric('maximo', 'max',
													  field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['contratos'].metric('minimo', 'min',
													  field='doc.compiledRelease.contracts.value.amount')

			s.aggs['compradores'].metric('fecha_ultimo_proceso', 'max',
										 field='doc.compiledRelease.tender.tenderPeriod.startDate')

			# Filtros
			if tmc.replace(' ', ''):
				q_tmc = 'params.tmc' + tmc
				s.aggs['compradores'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"tmc": "contratos.suma"}, script=q_tmc)

			if pmc.replace(' ', ''):
				q_pmc = 'params.pmc' + pmc
				s.aggs['compradores'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"pmc": "contratos.promedio"},
							script=q_pmc)

			if mamc.replace(' ', ''):
				q_mamc = 'params.mamc' + mamc
				s.aggs['compradores'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"mamc": "contratos.maximo"},
							script=q_mamc)

			if memc.replace(' ', ''):
				q_memc = 'params.memc' + memc
				s.aggs['compradores'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"memc": "contratos.minimo"},
							script=q_memc)

			if cp.replace(' ', ''):
				q_cp = 'params.memc' + cp
				s.aggs['compradores'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"memc": "procesos"}, script=q_cp)

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
				dfCompradores['fecha_ultimo_proceso'] = pd.to_datetime(dfCompradores['fecha_ultimo_proceso'],
																	   errors='coerce')

			# Ejemplo: fup==2018-03-02
			if fup.replace(' ', ''):
				if len(fup) > 1:
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
			s.aggs['compradores']['nombre']['contratos'].metric('suma', 'sum',
																field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('promedio', 'avg',
																field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('maximo', 'max',
																field='doc.compiledRelease.contracts.value.amount')
			s.aggs['compradores']['nombre']['contratos'].metric('minimo', 'min',
																field='doc.compiledRelease.contracts.value.amount')

			s.aggs['compradores']['nombre'].metric('fecha_ultimo_proceso', 'max',
												   field='doc.compiledRelease.tender.tenderPeriod.startDate')

			# Filtros
			if tmc.replace(' ', ''):
				q_tmc = 'params.tmc' + tmc
				s.aggs['compradores']['nombre'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"tmc": "contratos.suma"}, script=q_tmc)

			if pmc.replace(' ', ''):
				q_pmc = 'params.pmc' + pmc
				s.aggs['compradores']['nombre'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"pmc": "contratos.promedio"},
							script=q_pmc)

			if mamc.replace(' ', ''):
				q_mamc = 'params.mamc' + mamc
				s.aggs['compradores']['nombre'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"mamc": "contratos.maximo"},
							script=q_mamc)

			if memc.replace(' ', ''):
				q_memc = 'params.memc' + memc
				s.aggs['compradores']['nombre'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"memc": "contratos.minimo"},
							script=q_memc)

			if cp.replace(' ', ''):
				q_cp = 'params.memc' + cp
				s.aggs['compradores']['nombre'] \
					.metric('filtro_totales', 'bucket_selector', buckets_path={"memc": "procesos"}, script=q_cp)

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
				dfCompradores['fecha_ultimo_proceso'] = pd.to_datetime(dfCompradores['fecha_ultimo_proceso'],
																	   errors='coerce')

			# Ejemplo: fup==2018-03-02
			if fup.replace(' ', ''):
				if len(fup) > 1:
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

		tipoIdentificador = request.GET.get('tid', 'nombre')  # por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)

		partieId = urllib.parse.unquote_plus(partieId)

		if tipoIdentificador == 'nombre':
			qPartieId = Q('match_phrase', doc__compiledRelease__parties__name__keyword=partieId)
			s = s.query('nested', inner_hits={"size": 1}, path='doc.compiledRelease.parties', query=qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order": "desc"}})
			s = s.source(False)
		else:
			qPartieId = Q('match_phrase', doc__compiledRelease__parties__id__keyword=partieId)
			s = s.query('nested', inner_hits={"size": 1}, path='doc.compiledRelease.parties', query=qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order": "desc"}})
			s = s.source(False)

		results = s[0:1].execute()

		if len(results) == 0:
			s = Search(using=cliente, index=OCDS_INDEX)
			qPartieId = Q('match_phrase', extra__buyerFullName__keyword=partieId)
			s = s.query(qPartieId)
			s = s.sort({"doc.compiledRelease.date": {"order": "desc"}})

			results2 = s[0:1].execute()

			if len(results2) > 0:
				buyerId = results2.hits.hits[0]["_source"]["doc"]["compiledRelease"]["buyer"]["id"]

				qPartieId = Q('match_phrase', doc__compiledRelease__parties__id__keyword=buyerId)
				s = s.query('nested', inner_hits={"size": 1}, path='doc.compiledRelease.parties', query=qPartieId)
				s = s.sort({"doc.compiledRelease.date": {"order": "desc"}})
				s = s.source(False)

				results3 = s[0:1].execute()

				dependencias = 1
			else:
				dependencias = 0
		else:
			dependencias = 0

		try:
			if dependencias != 1:
				partie = results["hits"]["hits"][0]["inner_hits"]["doc.compiledRelease.parties"]["hits"]["hits"][0][
					"_source"].to_dict()
			else:
				partie = results3["hits"]["hits"][0]["inner_hits"]["doc.compiledRelease.parties"]["hits"]["hits"][0][
					"_source"].to_dict()

			return Response(partie)

		except Exception as e:
			raise Http404

		return Response(results.hits.hits[0]["_source"]["doc"]["compiledRelease"]["buyer"])


# Indicadores de ONCAE


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
		tipoIdentificador = request.GET.get('tid', 'nombre')  # por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page - 1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=OCDS_INDEX)

		# Mostrando
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

		if comprador.replace(' ', ''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if ocid.replace(' ', ''):
			filtro = Q("match", doc__ocid__keyword=ocid)
			filtros.append(filtro)

		if titulo.replace(' ', ''):
			filtro = Q("match", doc__compiledRelease__tender__title=titulo)
			filtros.append(filtro)

		if categoriaCompra.replace(' ', ''):
			filtro = Q("match", doc__compiledRelease__tender__procurementMethodDetails=categoriaCompra)
			filtros.append(filtro)

		if estado.replace(' ', ''):
			filtro = Q("match", extra__lastSection__keyword=estado)
			filtros.append(filtro)

		if montoContratado.replace(' ', ''):
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

		if fechaInicio.replace(' ', ''):
			validarFecha = getDateParam(fechaInicio)

			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__tender__tenderPeriod__startDate=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'lt': valor,
																							   "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'lte': valor,
																							   "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gt': valor,
																							   "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__startDate={'gte': valor,
																							   "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaRecepcion.replace(' ', ''):
			validarFecha = getDateParam(fechaRecepcion)

			if validarFecha is not None:
				operador = validarFecha["operador"]
				valor = validarFecha["valor"]

				if operador == "==":
					filtro = Q('match', doc__compiledRelease__tender__tenderPeriod__endDate=valor)
				elif operador == "<":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'lt': valor,
																							 "format": "yyyy-MM-dd"})
				elif operador == "<=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'lte': valor,
																							 "format": "yyyy-MM-dd"})
				elif operador == ">":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'gt': valor,
																							 "format": "yyyy-MM-dd"})
				elif operador == ">=":
					filtro = Q('range', doc__compiledRelease__tender__tenderPeriod__endDate={'gte': valor,
																							 "format": "yyyy-MM-dd"})
				else:
					filtro = None

				if filtro is not None:
					filtros.append(filtro)

		if fechaPublicacion.replace(' ', ''):
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
		if ordenarPor.replace(' ', ''):
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
		estado = request.GET.get('estado', '')
		monto = request.GET.get('monto', '')
		fechaFirma = request.GET.get('fechaFirma', '')
		fechaInicio = request.GET.get('fechaInicio', '')
		ordenarPor = request.GET.get('ordenarPor', '')
		dependencias = request.GET.get('dependencias', '0')
		tipoIdentificador = request.GET.get('tid', 'id')  # por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page - 1) * paginarPor
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

		if proveedor.replace(' ', ''):
			qProveedor = Q('match', suppliers__name=proveedor)
			filtro = Q('nested', path='suppliers', query=qProveedor)
			filtros.append(filtro)

		if titulo.replace(' ', ''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if descripcion.replace(' ', ''):
			filtro = Q("match", description=descripcion)
			filtros.append(filtro)

		if tituloLicitacion.replace(' ', ''):
			filtro = Q("match", extra__tenderTitle=tituloLicitacion)
			filtros.append(filtro)

		if categoriaCompra.replace(' ', ''):
			filtro = Q("match_phrase", extra__tenderMainProcurementCategory=categoriaCompra)
			filtros.append(filtro)

		if estado.replace(' ', ''):
			filtro = Q("match", status=estado)
			filtros.append(filtro)

		if fechaInicio.replace(' ', ''):
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

		if fechaFirma.replace(' ', ''):
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

		if monto.replace(' ', ''):
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

		s = s.query('bool', filter=filtros)

		# Ordenar resultados.
		mappingSort = {
			"comprador": "extra.buyerFullName.keyword",
			"titulo": "title.keyword",
			"tituloLicitacion": "extra.tenderTitle.keyword",
			"categoriaCompra": "extra.tenderMainProcurementCategory.keyword",
			"estado": "status.keyword",
			"monto": "value.amount",
			"fechaFirma": "period.startDate",
			"fechaInicio": "dateSigned",
		}

		# ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ', ''):
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
		parametros["estado"] = estado
		parametros["monto"] = monto
		parametros["fechaInicio"] = fechaInicio
		parametros["fechaFirma"] = fechaInicio
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

		tipoIdentificador = request.GET.get('tid', 'nombre')  # por id, nombre

		if tipoIdentificador not in ['id', 'nombre']:
			tipoIdentificador = 'nombre'

		start = (page - 1) * paginarPor
		end = start + paginarPor

		cliente = ElasticSearchDefaultConnection()
		s = Search(using=cliente, index=CONTRACT_INDEX)

		# Filtros
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

		if comprador.replace(' ', ''):
			filtro = Q("match", extra__buyerFullName=comprador)
			filtros.append(filtro)

		if proveedor.replace(' ', ''):
			qProveedor = Q('match', implementation__transactions__payee__name=proveedor)
			s = s.query('nested', path='implementation.transactions', query=qProveedor)

		if titulo.replace(' ', ''):
			filtro = Q("match", title=titulo)
			filtros.append(filtro)

		if fecha.replace(' ', ''):
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

		if monto.replace(' ', ''):
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

		if pagos.replace(' ', ''):
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

		# ordenarPor = 'asc(comprador),desc(monto)
		ordenarES = {}
		if ordenarPor.replace(' ', ''):
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

# Descargas

class Descargas(APIView):

	def get(self, request, format=None):

		listaArchivos = []
		urlDescargas = '/api/v1/descargas/'

		with connections['portalocdshn_admin'].cursor() as cursor:
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
