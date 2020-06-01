from django.shortcuts import render
from portalocdspy_backend import documents
from django.core.paginator import Paginator, Page, EmptyPage, PageNotAnInteger
import os
from django.http import HttpResponse, Http404
from django.conf import settings

class DSEPaginator(Paginator):
    """
    Override Django's built-in Paginator class to take in a count/total number of items;
    Elasticsearch provides the total as a part of the query results, so we can minimize hits.
    """
    def __init__(self, *args, **kwargs):
        super(DSEPaginator, self).__init__(*args, **kwargs)
        self._count = self.object_list.hits.total
        print("Total", self.object_list.hits.total)

    def page(self, number):
        # this is overridden to prevent any slicing of the object_list - Elasticsearch has
        # returned the sliced data already.
        number = self.validate_number(number)
        return Page(self.object_list, number, self)

# Create your views here.
def Inicio(request):
    context = {}

    return render(request,'inicio/inicio.html', context)

def Proceso(request,ocid=''):
    return render(request,'proceso/proceso.html',{'ocid':ocid.replace('"','')})

def Api(request):
    return render(request,'api/api.html')

def ManualApi(request):
    return render(request,'manualApi/manualApi.html')

def Acerca(request):
    return render(request,'acerca/acerca.html')

def Busqueda(request):
    parametros = {}
    parametros['metodo'] = request.GET.get('metodo','proceso')
    return render(request,'busqueda/busqueda.html',parametros)

def Comprador(request,id=''):
    parametros = {
        "proveedorCon" : request.GET.get('proveedorCon',''),
        "tituloCon" : request.GET.get('tituloCon',''),
        "tituloLicitacionCon" : request.GET.get('tituloLicitacionCon',''),
        "descripcionCon" : request.GET.get('descripcionCon',''),
        "categoriaCompraCon" : request.GET.get('categoriaCompraCon',''),
        "estadoCon" : request.GET.get('estadoCon',''),
        "fechaFirmaCon" : request.GET.get('fechaFirmaCon','').replace(">", "").replace("<", "").replace("==", ""),
        "fechaInicioCon" : request.GET.get('fechaInicioCon','').replace(">", "").replace("<", "").replace("==", ""),
        "montoCon" : request.GET.get('montoCon','').replace(">", "").replace("<", "").replace("==", ""),
        "dependencias" : int(request.GET.get('dependencias','0')),
        "paginarPorCon" : int(request.GET.get('paginarPorCon','5')),
        "ordenarPorCon" : request.GET.get('ordenarPorCon',''),
        "id":id.replace('"',''),
        "compradorPag" : request.GET.get('compradorPag',''),
        "proveedorPag" : request.GET.get('proveedorPag',''),
        "tituloPag" : request.GET.get('tituloPag',''),
        "estadoPag" : request.GET.get('estadoPag',''),
        "fechaPag" : request.GET.get('fechaPag','').replace(">", "").replace("<", "").replace("==", ""),
        "montoPag" : request.GET.get('montoPag','').replace(">", "").replace("<", "").replace("==", ""),
        "pagosPag" : request.GET.get('pagosPag','').replace(">", "").replace("<", "").replace("==", ""),
        "paginarPorPag" : int(request.GET.get('paginarPorPag','5')),
        "ordenarPorPag" : request.GET.get('ordenarPorPag',''),

        "compradorPro" : request.GET.get('compradorPro',''),
        "ocidPro" : request.GET.get('ocidPro',''),
        "tituloPro" : request.GET.get('tituloPro',''),
        "categoriaCompraPro" : request.GET.get('categoriaCompraPro',''),
        "estadoPro" : request.GET.get('estadoPro',''),
        "montoContratadoPro" : request.GET.get('montoContratadoPro','').replace(">", "").replace("<", "").replace("==", ""),
        "fechaInicioPro" : request.GET.get('fechaInicioPro','').replace(">", "").replace("<", "").replace("==", ""),
        "fechaRecepcionPro" : request.GET.get('fechaRecepcionPro','').replace(">", "").replace("<", "").replace("==", ""),
        "fechaPublicacionPro" : request.GET.get('fechaPublicacionPro','').replace(">", "").replace("<", "").replace("==", ""),
        
        "paginarPorPro" : int(request.GET.get('paginarPorPro','5')),
        "ordenarPorPro" : request.GET.get('ordenarPorPro','')
    }
    parametros['operadorfechaFirmaCon'] = verificarOperador(request.GET.get('fechaFirmaCon',''))
    parametros['operadorfechaInicioCon'] = verificarOperador(request.GET.get('fechaInicioCon',''))
    parametros['operadormontoCon'] = verificarOperador(request.GET.get('montoCon',''))

    parametros['ordencompradorCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'compradorCon')
    parametros['ordentituloCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'tituloCon')
    parametros['ordentituloLicitacionCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'tituloLicitacionCon')
    parametros['ordendescripcionCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'descripcionCon')
    parametros['ordencategoriaCompraCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'categoriaCompraCon')
    parametros['ordenestadoCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'estadoCon')
    parametros['ordenfechaFirmaCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'fechaFirmaCon')
    parametros['ordenfechaInicioCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'fechaInicioCon')
    parametros['ordenmontoCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'montoCon')

    """Pagos"""
    parametros['operadormontoPag'] = verificarOperador(request.GET.get('montoPag',''))
    parametros['operadorfechaPag'] = verificarOperador(request.GET.get('fechaPag',''))
    parametros['operadorpagosPag'] = verificarOperador(request.GET.get('pagosPag',''))

    parametros['ordencompradorPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'compradorPag')
    parametros['ordenproveedorPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'proveedorPag')
    parametros['ordentituloPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'tituloPag')
    parametros['ordenfechaPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'fechaPag')
    parametros['ordenestadoPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'estadoPag')
    parametros['ordenmontoPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'montoPag')
    parametros['ordenpagosPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'pagosPag')


    """Procesos"""
    parametros['operadormontoContratadoPro'] = verificarOperador(request.GET.get('montoContratadoPro',''))
    parametros['operadorfechaInicioPro'] = verificarOperador(request.GET.get('fechaInicioPro',''))
    parametros['operadorfechaRecepcionPro'] = verificarOperador(request.GET.get('fechaRecepcionPro',''))
    parametros['operadorfechaPublicacionPro'] = verificarOperador(request.GET.get('fechaPublicacionPro',''))

    parametros['ordencompradorPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'compradorPro')
    parametros['ordenocidPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'ocidPro')
    parametros['ordentituloPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'tituloPro')
    parametros['ordencategoriaCompraPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'categoriaCompraPro')
    parametros['ordenestadoPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'estadoPro')
    parametros['ordenmontoContratadoPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'montoContratadoPro')
    parametros['ordenfechaInicioPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'fechaInicioPro')
    parametros['ordenfechaRecepcionPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'fechaRecepcionPro')
    parametros['ordenfechaPublicacionPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'fechaPublicacionPro')
    return render(request,'comprador/comprador.html',parametros)

def Compradores(request):
    parametros = {
      "nombre" : request.GET.get('nombre',''),
      "identificacion" : request.GET.get('identificacion',''),
      "tmc" : request.GET.get('tmc','').replace(">", "").replace("<", "").replace("==", ""),
      "pmc" : request.GET.get('pmc','').replace(">", "").replace("<", "").replace("==", ""),
      "mamc" : request.GET.get('mamc','').replace(">", "").replace("<", "").replace("==", ""),
      "fup" : request.GET.get('fup','').replace(">", "").replace("<", "").replace("==", ""),
      "cp" : request.GET.get('cp','').replace(">", "").replace("<", "").replace("==", ""),
      "dependencias" : request.GET.get('dependencias','0'),
      "memc" : request.GET.get('memc','').replace(">", "").replace("<", "").replace("==", ""),
      "paginarPor" : int(request.GET.get('paginarPor','5')),
      "ordenarPor" : request.GET.get('ordenarPor','')
    }
    parametros['operadortmc'] = verificarOperador(request.GET.get('tmc',''))
    parametros['operadorpmc'] = verificarOperador(request.GET.get('pmc',''))
    parametros['operadormamc'] = verificarOperador(request.GET.get('mamc',''))
    parametros['operadormemc'] = verificarOperador(request.GET.get('memc',''))
    parametros['operadorfup'] = verificarOperador(request.GET.get('fup',''))
    parametros['operadorcp'] = verificarOperador(request.GET.get('cp',''))

    parametros['ordennombre'] = verificarOrden(request.GET.get('ordenarPor',''),'nombre')
    parametros['ordenidentificacion'] = verificarOrden(request.GET.get('ordenarPor',''),'identificacion')
    parametros['ordentmc'] = verificarOrden(request.GET.get('ordenarPor',''),'tmc')
    parametros['ordenpmc'] = verificarOrden(request.GET.get('ordenarPor',''),'pmc')
    parametros['ordenmamc'] = verificarOrden(request.GET.get('ordenarPor',''),'mamc')
    parametros['ordenfup'] = verificarOrden(request.GET.get('ordenarPor',''),'fup')
    parametros['ordencp'] = verificarOrden(request.GET.get('ordenarPor',''),'cp')
    parametros['ordenmemc'] = verificarOrden(request.GET.get('ordenarPor',''),'memc')
    return render(request,'compradores/compradores.html',parametros)

def Descargas(request):
    return render(request,'descargas/descargas.html')

def DescargarArchivo(request, file):
  path_to_file = "C:\\Users\\Isaias Zelaya\\Desktop/"

  file_path = os.path.join(settings.MEDIA_ROOT, path_to_file + file)
  if os.path.exists(file_path):
    with open(file_path, 'rb') as fh:
      response = HttpResponse(fh.read(), content_type="application/force-download")
      response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
      return response
  raise Http404

def Preguntas(request):
    return render(request,'preguntas/preguntas.html')

def Procesos_Comprador(request):
    return render(request,'procesos_comprador/procesos_comprador.html')

def Procesos_Proveedor(request):
    return render(request,'procesos_proveedor/procesos_proveedor.html')

def Proveedor(request,id=''):
    parametros = {
      "compradorCon" : request.GET.get('compradorCon',''),
      "tituloCon" : request.GET.get('tituloCon',''),
      "tituloLicitacionCon" : request.GET.get('tituloLicitacionCon',''),
      "descripcionCon" : request.GET.get('descripcionCon',''),
      "categoriaCompraCon" : request.GET.get('categoriaCompraCon',''),
      "estadoCon" : request.GET.get('estadoCon',''),
      "fechaFirmaCon" : request.GET.get('fechaFirmaCon','').replace(">", "").replace("<", "").replace("==", ""),
      "fechaInicioCon" : request.GET.get('fechaInicioCon','').replace(">", "").replace("<", "").replace("==", ""),
      "montoCon" : request.GET.get('montoCon','').replace(">", "").replace("<", "").replace("==", ""),
      "paginarPorCon" : int(request.GET.get('paginarPorCon','5')),
      "ordenarPorCon" : request.GET.get('ordenarPorCon',''),
      "id":id.replace('"',''),
      "compradorPag" : request.GET.get('compradorPag',''),
      "tituloPag" : request.GET.get('tituloPag',''),
      "estadoPag" : request.GET.get('estadoPag',''),
      "fechaPag" : request.GET.get('fechaPag','').replace(">", "").replace("<", "").replace("==", ""),
      "montoPag" : request.GET.get('montoPag','').replace(">", "").replace("<", "").replace("==", ""),
      "pagosPag" : request.GET.get('pagosPag','').replace(">", "").replace("<", "").replace("==", ""),
      "paginarPorPag" : int(request.GET.get('paginarPorPag','5')),
      "ordenarPorPag" : request.GET.get('ordenarPorPag',''),
      "clasificacionPro" : request.GET.get('clasificacionPro',''),
      "montoPro" : request.GET.get('montoPro','').replace(">", "").replace("<", "").replace("==", ""),
      "cantidadContratosPro" : request.GET.get('cantidadContratosPro','').replace(">", "").replace("<", "").replace("==", ""),
      "paginarPorPro" : int(request.GET.get('paginarPorPro','5')),
      "ordenarPorPro" : request.GET.get('ordenarPorPro',''),
    }
    parametros['operadorfechaFirmaCon'] = verificarOperador(request.GET.get('fechaFirmaCon',''))
    parametros['operadorfechaInicioCon'] = verificarOperador(request.GET.get('fechaInicioCon',''))
    parametros['operadormontoCon'] = verificarOperador(request.GET.get('montoCon',''))

    parametros['ordencompradorCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'compradorCon')
    parametros['ordentituloCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'tituloCon')
    parametros['ordentituloLicitacionCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'tituloLicitacionCon')
    parametros['ordendescripcionCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'descripcionCon')
    parametros['ordencategoriaCompraCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'categoriaCompraCon')
    parametros['ordencompradorCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'compradorCon')
    parametros['ordentituloCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'tituloCon')
    parametros['ordendescripcionCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'descripcionCon')
    parametros['ordencategoriaCompraCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'categoriaCompraCon')
    parametros['ordenestadoCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'estadoCon')
    parametros['ordenfechaFirmaCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'fechaFirmaCon')
    parametros['ordenfechaInicioCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'fechaInicioCon')
    parametros['ordenmontoCon'] = verificarOrden(request.GET.get('ordenarPorCon',''),'montoCon')


    parametros['operadormontoPag'] = verificarOperador(request.GET.get('montoPag',''))
    parametros['operadorfechaPag'] = verificarOperador(request.GET.get('fechaPag',''))
    parametros['operadorpagosPag'] = verificarOperador(request.GET.get('pagosPag',''))

    parametros['ordencompradorPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'compradorPag')
    parametros['ordentituloPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'tituloPag')
    parametros['ordenfechaPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'fechaPag')
    parametros['ordenmontoPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'montoPag')
    parametros['ordenpagosPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'pagosPag')
    parametros['ordenestadoPag'] = verificarOrden(request.GET.get('ordenarPorPag',''),'estadoPag')


    """Productos"""
    parametros['operadormontoPro'] = verificarOperador(request.GET.get('montoPro',''))
    parametros['operadorcantidadContratosPro'] = verificarOperador(request.GET.get('cantidadContratosPro',''))

    parametros['ordenclasificacionPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'clasificacionPro')
    parametros['ordenmontoPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'montoPro')
    parametros['ordencantidadContratosPro'] = verificarOrden(request.GET.get('ordenarPorPro',''),'cantidadContratosPro')




    return render(request,'proveedor/proveedor.html',parametros)

def Proveedores(request):
    parametros = {
      "nombre" : request.GET.get('nombre',''),
      "identificacion" : request.GET.get('identificacion',''),
      "tmc" : request.GET.get('tmc','').replace(">", "").replace("<", "").replace("==", ""),
      "cp" : request.GET.get('cp','').replace(">", "").replace("<", "").replace("==", ""),
      "pmc" : request.GET.get('pmc','').replace(">", "").replace("<", "").replace("==", ""),
      "mamc" : request.GET.get('mamc','').replace(">", "").replace("<", "").replace("==", ""),
      "fua" : request.GET.get('fua','').replace(">", "").replace("<", "").replace("==", ""),
      "memc" : request.GET.get('memc','').replace(">", "").replace("<", "").replace("==", ""),
      "paginarPor" : int(request.GET.get('paginarPor','5')),
      "ordenarPor" : request.GET.get('ordenarPor','')
    }
    parametros['operadortmc'] = verificarOperador(request.GET.get('tmc',''))
    parametros['operadorpmc'] = verificarOperador(request.GET.get('pmc',''))
    parametros['operadormamc'] = verificarOperador(request.GET.get('mamc',''))
    parametros['operadormemc'] = verificarOperador(request.GET.get('memc',''))
    parametros['operadorfua'] = verificarOperador(request.GET.get('fua',''))
    parametros['operadorcp'] = verificarOperador(request.GET.get('cp',''))

    parametros['ordennombre'] = verificarOrden(request.GET.get('ordenarPor',''),'nombre')
    parametros['ordenidentificacion'] = verificarOrden(request.GET.get('ordenarPor',''),'identificacion')
    parametros['ordentmc'] = verificarOrden(request.GET.get('ordenarPor',''),'tmc')
    parametros['ordenpmc'] = verificarOrden(request.GET.get('ordenarPor',''),'pmc')
    parametros['ordenmamc'] = verificarOrden(request.GET.get('ordenarPor',''),'mamc')
    parametros['ordencp'] = verificarOrden(request.GET.get('ordenarPor',''),'cp')
    parametros['ordenfua'] = verificarOrden(request.GET.get('ordenarPor',''),'fua')
    parametros['ordenmemc'] = verificarOrden(request.GET.get('ordenarPor',''),'memc')
    
    return render(request,'proveedores/proveedores.html',parametros)
def ProveedoresSefin(request):
  parametros = {
    "nombre" : request.GET.get('nombre',''),
    "identificacion" : request.GET.get('identificacion',''),
    "tmc" : request.GET.get('tmc','').replace(">", "").replace("<", "").replace("==", ""),
    "cp" : request.GET.get('cp','').replace(">", "").replace("<", "").replace("==", ""),
    "pmc" : request.GET.get('pmc','').replace(">", "").replace("<", "").replace("==", ""),
    "mamc" : request.GET.get('mamc','').replace(">", "").replace("<", "").replace("==", ""),
    "fua" : request.GET.get('fua','').replace(">", "").replace("<", "").replace("==", ""),
    "memc" : request.GET.get('memc','').replace(">", "").replace("<", "").replace("==", ""),
    "paginarPor" : int(request.GET.get('paginarPor','5')),
    "ordenarPor" : request.GET.get('ordenarPor','')
  }
  parametros['operadortmc'] = verificarOperador(request.GET.get('tmc',''))
  parametros['operadorpmc'] = verificarOperador(request.GET.get('pmc',''))
  parametros['operadormamc'] = verificarOperador(request.GET.get('mamc',''))
  parametros['operadormemc'] = verificarOperador(request.GET.get('memc',''))
  parametros['operadorfua'] = verificarOperador(request.GET.get('fua',''))
  parametros['operadorcp'] = verificarOperador(request.GET.get('cp',''))

  parametros['ordennombre'] = verificarOrden(request.GET.get('ordenarPor',''),'nombre')
  parametros['ordenidentificacion'] = verificarOrden(request.GET.get('ordenarPor',''),'identificacion')
  parametros['ordentmc'] = verificarOrden(request.GET.get('ordenarPor',''),'tmc')
  parametros['ordenpmc'] = verificarOrden(request.GET.get('ordenarPor',''),'pmc')
  parametros['ordenmamc'] = verificarOrden(request.GET.get('ordenarPor',''),'mamc')
  parametros['ordencp'] = verificarOrden(request.GET.get('ordenarPor',''),'cp')
  parametros['ordenfua'] = verificarOrden(request.GET.get('ordenarPor',''),'fua')
  parametros['ordenmemc'] = verificarOrden(request.GET.get('ordenarPor',''),'memc')
  
  return render(request,'proveedoresSefin/proveedoresSefin.html',parametros)
  
def Visualizaciones(request):
    return render(request,'visualizaciones/visualizaciones.html')

def TableroProcesosContratacion(request):
    return render(request,'tableroProcesosContratacion/tableroProcesosContratacion.html')

def TableroProcesosPago(request):
    return render(request,'tableroProcesosPago/tableroProcesosPago.html')

def IndicadoresProcesosContratacion(request):
    return render(request,'indicadoresProcesosContratacion/indicadoresProcesosContratacion.html')

def pesosContratos(request):
    return render(request,'pesosContratos/pesosContratos.html')
def pesosContratosEmbed(request):
    return render(request,'pesosContratos/pesosContratosEmbed.html')


def verificarOperador(filtro):
    operador='<'
    if(filtro):
      if('>' in filtro):
        operador='>'
      if('=' in filtro):
        operador='='
    return operador

def verificarOrden(filtro,nombre):
    filtrosProveedoresPropiedades = {
    "fecha_ultimo_proceso" : "fua",
    "id" : "identificacion",
    "mayor_monto_contratado" : "mamc",
    "menor_monto_contratado" : "memc",
    "name" : "nombre",
    "procesos" : "procesos",
    "promedio_monto_contratado" : "pmc",
    "total_monto_contratado" : "tmc"
    }
    orden='neutro'
    if((filtro.replace("desc(", "").replace("asc(", "").replace(")", "") in filtrosProveedoresPropiedades) and filtrosProveedoresPropiedades[filtro.replace("desc(", "").replace("asc(", "").replace(")", "")] == nombre):
      if(filtro):
        if('asc(' in filtro):
          orden='ascendente'
        if('desc(' in filtro):
          orden='descendente'
    return orden