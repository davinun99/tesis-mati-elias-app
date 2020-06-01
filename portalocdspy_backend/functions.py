#Funciones compartidas. 
import re 
import datetime
import dateutil

def validateSortParam(param):
	pattern = '^desc\(([^)]+)\)$|^asc\(([^)]+)\)$'

	if re.match(pattern, param):
		response = True
	else:
		response = False

	return response

# library: Elasticsearch, Pandas
def getSortParam(parametro, library):
	validarAsc = '^asc\(([^)]+)\)$'
	validarDesc = '^desc\(([^)]+)\)$'

	if re.match(validarAsc, parametro):
		if library == 'Elasticsearch':
			ascendente = 'asc'
		else:
			ascendente = True

		valor = re.search(validarAsc, parametro).group(1)
	else:
		if library == 'Elasticsearch':
			ascendente = 'desc'
		else:
			ascendente = False

		valor = re.search(validarDesc, parametro).group(1)

	respuesta = {
		"valor": valor,
		"ascendente": ascendente
	}

	return respuesta

#parametro = 'asc(username),desc(email),asc(plata)'
def getSortBy(parametro):
	parametros = parametro.split(',')

	columnas = []
	ascendentes = []

	for p in parametros:
		if validateSortParam(p):
			r = getSortParam(p, 'Pandas')
			columnas.append(r["valor"])
			ascendentes.append(r["ascendente"])

	respuesta = {
		"columnas": columnas,
		"ascendentes": ascendentes
	}

	return respuesta

#parametro = 'asc(username),desc(email),asc(plata)'
def getSortES(parametro):
	parametros = parametro.split(',')

	respuesta = []

	for p in parametros:
		if validateSortParam(p):
			r = getSortParam(p, 'Elasticsearch')
			respuesta.append({
				"valor": r["valor"],
				"orden": r["ascendente"]
			})

	if len(respuesta) > 0:
		return respuesta
	else:
		return None

def getOperator(param):
	validador = '^(>=|<=|<|>|==|!=)(\d*\.?\d*)$'

	if re.match(validador, param):
		validado = re.search(validador, param)
		operador = validado.group(1)
		valor = validado.group(2)
	else:
		operador = None
		valor = None

	if operador is not None and valor is not None:
		respuesta = {
			"operador": operador,
			"valor": valor
		}
	else:
		respuesta = None

	return respuesta

# yyyy-MM-dd

def getDateParam(dateString):
	validador = '^(>=|<=|<|>|==)(\d{4})\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$'

	if re.match(validador, dateString):
		validado = re.search(validador, dateString)
		operador = validado.group(1)
		yyyy = validado.group(2)
		mm = validado.group(3)
		dd = validado.group(4)
		valor = yyyy + '-' + mm + '-' + dd
	else:
		operador = None
		valor = None

	if operador is not None and valor is not None:
		respuesta = {
			"operador": operador,
			"valor": valor
		}
	else:
		respuesta = None

	return respuesta

"""
	Paramtero nuero de mes = 01, 02, 03, 04, .... , 11, 12
	Retorna el nombre del mes.
"""
def NombreDelMes(numero_mes):
	
	m = {
		"01":"Enero",
		"02":"Febrero",
		"03":"Marzo",
		"04":"Abril",
		"05":"Mayo",
		"06":"Junio",
		"07":"Julio",
		"08":"Agosto",
		"09":"Septiembre",
		"10":"Octubre",
		"11":"Noviembre",
		"12":"Diciembre"
	}

	try:
		respuesta = m[numero_mes]
		return respuesta
	except:
		return "No definido"

"""
	Parametros
		paquetes = lista de models PackageData.
		request = objeto de solicitud.
	retorna:
		diccionario con los metadatos de un paquete. 
"""
def generarMetaDatosPaquete(paquetes, request):

	uri = ''
	license = ''
	version = '1.1'
	publisher = {}
	extensions = []
	publishedDate = ''
	publicationPolicy = ''
	releases = []
	dominio = 'http://contratacionesabiertas.gob.hn/descargas/'

	metaDatosPaquete = {}

	fechaActual = datetime.datetime.now(dateutil.tz.tzoffset('UTC', -6*60*60))
	publishedDate = fechaActual.isoformat()

	for p in paquetes:
		paquete = p.data

		license = paquete['license']
		version = paquete['version']
		publisher = paquete['publisher']
		publicationPolicy = paquete['publicationPolicy']

		for e in paquete['extensions']:
			if not e in extensions:
				extensions.append(e)

	metaDatosPaquete["uri"] = request.build_absolute_uri()
	metaDatosPaquete["version"] = version
	metaDatosPaquete["publishedDate"] = publishedDate
	metaDatosPaquete["publisher"] = publisher
	metaDatosPaquete["extensions"] = extensions
	metaDatosPaquete["license"] = license
	metaDatosPaquete["publicationPolicy"] = publicationPolicy

	return metaDatosPaquete

"""
	retorna un valor para filtrar en bucket selector. 
	val: signo valor ej. >500
"""
def validateNumberParam(val):

	validador = '^(>=|<=|<|>|==)([-+]?[0-9]*\.?[0-9]+)$'

	if re.match(validador, val):
		validado = re.search(validador, val)
		operador = validado.group(1)
		numero = validado.group(2)
		decimal = float(numero)

		value = operador + str(decimal)
	else:
		value = None

	return value

"""
	Generando datos para el paquete de registros. 
"""
def paqueteRegistros(paquetes, request):

	uri = ''
	version = '1.1'
	extensions = []
	publisher = {}
	license = ''
	publicationPolicy = ''
	publishedDate = ''
	records = []

	metaDatosPaquete = {}

	fechaActual = datetime.datetime.now(dateutil.tz.tzoffset('UTC', -6*60*60))
	publishedDate = fechaActual.isoformat()

	for p in paquetes:
		paquete = p.data

		license = paquete['license']
		version = paquete['version']
		publisher = paquete['publisher']
		publicationPolicy = paquete['publicationPolicy']

		for e in paquete['extensions']:
			if not e in extensions:
				extensions.append(e)

	metaDatosPaquete["uri"] = request.build_absolute_uri()
	metaDatosPaquete["version"] = version
	metaDatosPaquete["publishedDate"] = publishedDate
	metaDatosPaquete["publisher"] = publisher
	metaDatosPaquete["extensions"] = extensions
	metaDatosPaquete["license"] = license
	metaDatosPaquete["publicationPolicy"] = publicationPolicy

	return metaDatosPaquete

# Descargas ES a CSV

proceso_csv = dict([
	("OCID", "doc.compiledRelease.ocid"),
	("Código Entidad","extra.parentTop.id"),
	("Entidad","extra.parentTop.name"),
	("Código Unidad Ejecutora","doc.compiledRelease.buyer.id"),
	("Unidad Ejecutora","doc.compiledRelease.buyer.name"),
	("Expediente", "doc.compiledRelease.tender.title"),
	("Tipo Adquisición", "doc.compiledRelease.tender.localProcurementCategory"),
	("Modalidad", "doc.compiledRelease.tender.procurementMethodDetails"),
	("Fecha de Inicio", "doc.compiledRelease.tender.tenderPeriod.startDate"),
	("Fecha Recepción Ofertas", "doc.compiledRelease.tender.tenderPeriod.endDate"),
	("Fecha de publicación", "doc.compiledRelease.tender.datePublished"),
	("Estado del proceso", "doc.compiledRelease.tender.statusDetails"),
	("Periodo de invitación - Recepción de ofertas", "extra.daysTenderPeriod"),
	("Etapa OCDS", "extra.lastSection"),
	("Fuente de datos", "doc.compiledRelease.sources.0.name"),
])

contrato_csv = dict([
	("OCID","extra.ocid"),
	("Código institución","extra.parentTop.id"),
	("Institución de Compra","extra.parentTop.name"),
	("Código GA","extra.parent1.id"),
	("Gerencia Administrativa","extra.parent1.name"),
	("Código unidad de compra","extra.buyer.id"),
	("Unidad de Compra","extra.buyer.name"),
	("RTN","suppliers.0.id"),
	("Proveedor","suppliers.0.name"),
	("Expediente","extra.tenderTitle"),
	("Número de Contrato","title"),
	("Monto", "value.amount"),
	("Moneda", "value.currency"),
	("Monto HNL","extra.LocalCurrency.amount"),
	("Moneda HNL","extra.LocalCurrency.currency"),
	("Fecha de Inicio","period.startDate"),
	("Fecha de Firma", "dateSigned"),
	("Periodo de Evaluación - Adjudicación y Contratación", "extra.tiempoContrato"),
	("Estado","statusDetails"),
	("Tipo Adquisición", "localProcurementCategory"),
	("Modalidad", "extra.tenderProcurementMethodDetails"),
	("Fuente de datos","extra.sources.0.name"),
])

producto_csv = dict([
	("OCID","extra.ocid"),
	("Número Gestion","extra.contratoId"),
	("producto Id","id"),
	("Producto","description"),
	("Cantidad solicitada","quantity"),
	("Monto por unidad","unit.value.amount"),
	("Total","extra.total"),
	("Moneda","unit.value.currency"),
	("UNSPSC código","classification.id"),
	("UNSPSC nombre","classification.description"),
	("Código del covenio marco","attributes.0.id"),
	("Nombre del convenio marco","attributes.0.value"),
	("Estado OC","extra.statusDetails"),
	("Fuente de datos","extra.sources.0.name"),
])

proceso_csv_titulos = list(proceso_csv.keys())
proceso_csv_paths = list(proceso_csv.values())

contrato_csv_titulos = list(contrato_csv.keys())
contrato_csv_paths = list(contrato_csv.values())

producto_csv_titulos = list(producto_csv.keys())
producto_csv_paths = list(producto_csv.values())

class Echo(object):
	def write(self, value):
		return value

def get_data_from_path(path, data):
	current_pos = data

	for part in path.split("."):
		try:
			part = int(part)
		except ValueError:
			pass
		try:
			current_pos = current_pos[part]
		except (KeyError, IndexError, TypeError):
			return ""
	return current_pos

def generador_proceso_csv(search):
	yield proceso_csv_titulos

	# Todos los procesos
	for result in search.scan():
		line = []
		for path in proceso_csv_paths:
			line.append(get_data_from_path(path, result))
		yield line

	# results = search[0:500].execute()

	# for result in results:
	# 	line = []
	# 	for path in proceso_csv_paths:
	# 		line.append(get_data_from_path(path, result))
	# 	yield line

def generador_contrato_csv(search):
	yield contrato_csv_titulos

	# Todos los contratos
	for result in search.scan():
		line = []
		for path in contrato_csv_paths:
			line.append(get_data_from_path(path, result))
		yield line

	# results = search[0:10].execute()

	# for result in results:
	# 	# print(result)
	# 	line = []
	# 	for path in contrato_csv_paths:
	# 		line.append(get_data_from_path(path, result))
	# 	yield line

	# print("")

def generador_producto_csv(search):
	yield producto_csv_titulos

	# Todos los contratos
	for result in search.scan():
		if 'items' in result:
			for item in result["items"]:
				if 'extra' in item:
					item["extra"]["sources"] = result["extra"]["sources"]
					item["extra"]["ocid"] = result["extra"]["ocid"]
					item["extra"]["contratoId"] = result["id"]
					item["extra"]["statusDetails"] = result["statusDetails"]
				else:
					item["extra"] = result["extra"]

				line = []
				for path in producto_csv_paths:
					line.append(get_data_from_path(path, item))
				yield line

	# results = search[0:10].execute()

	# for result in results:
	# 	for item in result["items"]:
	# 		if 'extra' in item:
	# 			item["extra"]["sources"] = result["extra"]["sources"]
	# 			item["extra"]["ocid"] = result["extra"]["ocid"]
	# 			item["extra"]["contratoId"] = result["id"]
	# 		else:
	# 			item["extra"] = result["extra"]

	# 		line = []
	# 		for path in producto_csv_paths:
	# 			line.append(get_data_from_path(path, item))
	# 		yield line

	# print("")
