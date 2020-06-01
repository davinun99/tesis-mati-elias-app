/**
 * @file comprador.js Este archivo se incluye en la sección de comprador del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**
 * Id del Comprador
 * @type {string}
 */
var compradorId = '';

/**
 * Objeto de datos del comprador
 * @type {Object}
 */
var datosComprador = {};

/**
 * Parametros dentro de la url con los parametros que corresponden a su método según la tabla
 * @type {Object}
 */
var filtrosAPropiedades = {
    /*Tabla Contratos */
    "proveedorCon": 'proveedor',
    "tituloCon": "titulo",
    "compradorCon":"compradorCon",
    "tituloLicitacionCon": "tituloLicitacion",
    "descripcionCon": "descripcion",
    "categoriaCompraCon": "categoriaCompra",
    "estadoCon": "estado",
    "fechaFirmaCon": "fechaFirma",
    "fechaInicioCon": "fechaInicio",
    "montoCon": "monto",
    "dependencias": "dependencias",
    "paginarPorCon": "paginarPor",
    "ordenarPorCon": "ordenarPor",
    /*Tabla Pagos */
    "compradorPag": "comprador",
    "proveedorPag": "proveedor",
    "tituloPag": "titulo",
    "estadoPag": "estado",
    "fechaPag": "fecha",
    "montoPag": "monto",
    "pagosPag": "pagos",
    "paginarPorPag": "paginarPor",
    "ordenarPorPag": "ordenarPor",
    /*Tabla Procesos */
    "compradorPro": "comprador",
    "ocidPro": "ocid",
    "tituloPro": "titulo",
    "categoriaCompraPro": "categoriaCompra",
    "estadoPro": "estado",
    "montoContratadoPro": "montoContratado",
    "fechaInicioPro": "fechaInicio",
    "fechaRecepcionPro": "fechaRecepcion",
    "fechaPublicacionPro": "fechaPublicacion",
    "ordenarPorPro": "ordenarPor"
};

/**
 * Traducciones para la Categoría de Compra
 * @type {Object}
 */
var categoriaCompra={
    'goods':{titulo:'Bienes y provisiones',descripcion:'El objeto primario de este proceso de contratación involucra bienes físicos o electrónicos o provisiones'},
    'works':{titulo:'Obras',descripcion:'El objeto primario de este proceso de contratación involucra construcción, reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
    'services':{titulo:'Servicios',descripcion:'El objeto primario de este proceso de contratación involucra servicios profesionales de alguna manera, generalmente contratados en la forma de resultados medibles o entregables.'},
    'goodsOrServices':{titulo:'Bienes y/o Servicios',descripcion:''},
    'consultingServices':{titulo:'Consultorías',descripcion:''}
  }

/**
 * Traducciones para las estapas de un proceso
 * @type {Object}
 */
var estadoProceso={
'planning':{
    titulo:'Planeación',
    descripcion:'Se propone o planea un proceso de contratación. La información en la sección de licitación describe el proceso propuesto. El campo tender.status debe de usarse para identificar si la planeación está en una etapa temprana o si hay planes detallados para una licitación.'
},
'tender':{
    titulo:'Licitación',
    descripcion:'Provee información sobre una nueva licitación (llamado a propuestas). La entrega de licitación debe contener detalles de los bienes o servicios que se buscan.'
},
'awards':{
    titulo:'Adjudicación',
    descripcion:'Da información sobre la adjudicación de un contrato. Estarán presentes una o más secciones de adjudicación, y la sección de licitación puede estar poblada con detalles del proceso que llevó a la adjudicación.'
},
'contracts':{
    titulo:'Contrato',
    descripcion:'Da información sobre los detalles de un contrato que ha entrado, o entrará, en vigencia. La sección de licitación puede ser poblada con detalles del proceso que lleva al contrato, y la sección de adjudicación puede tener detalles sobre la adjudicación sobre la cual este contrato será firmado.'
},
'implementation':{
    titulo:'Implementación',
    descripcion:'Provee información nueva sobre la implementación de un proceso de contratación.'
}		
}

/**
 * Inicializa los eventos de las tablas, obtiene los datos del comprador.
 */
$(function() {
    compradorId = decodeURIComponent($('#compradorId').val());
    $('.opcionFiltroBusquedaPagina').on('click', function(e) {
        $(e.currentTarget).addClass('active')
    });
    tippy('#informacionTipoDatos', {
        arrow: true,
        arrowType: 'round',
        content: '¿Que son estos tipos de datos?'
    });
    tippy('#buscarInformacion', {
        arrow: true,
        arrowType: 'round',
        content: 'Haz click para buscar'
    });
    $.datepicker.regional['es'] = {
        closeText: 'Cerrar',
        prevText: '< Ant',
        nextText: 'Sig >',
        currentText: 'Hoy',
        monthNames: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
        monthNamesShort: ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
        dayNames: ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'],
        dayNamesShort: ['Dom', 'Lun', 'Mar', 'Mié', 'Juv', 'Vie', 'Sáb'],
        dayNamesMin: ['Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sá'],
        weekHeader: 'Sm',
        dateFormat: 'dd/mm/yy',
        firstDay: 1,
        isRTL: false,
        showMonthAfterYear: false,
        yearSuffix: ''
    };
    $.datepicker.setDefaults($.datepicker.regional["es"]);
    $('.fecha').datepicker({
        "dateFormat": 'yy-mm-dd'
    });
    $('.fecha').mask('0000-00-00');
    $('.OpcionFiltroBusquedaNumerico input').on('change', function(evento) {
        cambiarFiltroNumerico(evento.currentTarget);
    });
    ObtenerComprador();
    var elementosNumericos = [];
    for (let i = 0; i < $('.elementoNumerico').length; i++) {
        var configuracionNumerica = {
            decimalCharacter: '.',
            decimalPlaces: $($('.elementoNumerico')[i]).attr('decimal') == "true" ? 2 : 0,
            digitalGroupSpacing: 3,
            digitGroupSeparator: ','
        };
        elementosNumericos[i] = new AutoNumeric($('.elementoNumerico')[i], configuracionNumerica);
    }

    $('#cajonContratos input.campoAzulBusqueda').on('change', function(evento) {
        var elemento = $('#cajonContratos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaCon'] = 1;
        InputFiltroContratos(filtros, true);
    });
    $('#cajonPagos input.campoAzulBusqueda').on('change', function(evento) {
        var elemento = $('#cajonPagos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaPag'] = 1;
        InputFiltroPagos(filtros, true);
    });
    $('#cajonProcesos input.campoAzulBusqueda').on('change', function(evento) {
        var elemento = $('#cajonProcesos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaPro'] = 1;
        InputFiltroProcesos(filtros, true);
    });
    $('#buscarInformacionContratos').on('click', function(evento) {
        var elemento = $('#cajonContratos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaCon'] = 1;
        InputFiltroContratos(filtros, true);
    });
    $('#buscarInformacionPagos').on('click', function(evento) {
        var elemento = $('#cajonPagos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaPag'] = 1;
        InputFiltroPagos(filtros, true);
    });
    $('#buscarInformacionProcesos').on('click', function(evento) {
        var elemento = $('#cajonProcesos input.campoAzulBusqueda');
        var filtros = {};
        filtros = ObtenerFiltrosGenerales();
        filtros[elemento.attr('filtro')] = (elemento.val());
        if (!ValidarCadena(filtros[elemento.attr('filtro')])) {
            delete filtros[elemento.attr('filtro')];
        }
        filtros['paginaPro'] = 1;
        InputFiltroProcesos(filtros, true);
    });

    AsignarEventosFiltro('#cajonContratos', 'Con', ObtenerFiltrosGenerales, InputFiltroContratos);
    AsignarEventosFiltro('#cajonPagos', 'Pag', ObtenerFiltrosGenerales, InputFiltroPagos);
    AsignarEventosFiltro('#cajonProcesos', 'Pro', ObtenerFiltrosGenerales, InputFiltroProcesos);
    AsignarOrdenTablaFiltros(OrdenFiltroContratos, '#cajonContratos .ordenEncabezado');
    AsignarOrdenTablaFiltros(OrdenFiltroPagos, '#cajonPagos .ordenEncabezado');
    AsignarOrdenTablaFiltros(OrdenFiltroProcesos, '#cajonProcesos .ordenEncabezado');
    $('#paginacionBusquedaContrato').on('change', function(e) {
        CantidadResultadosContrato($('#paginacionBusquedaContrato').val() ? $('#paginacionBusquedaContrato').val() : 5);
    });
    $('#paginacionBusquedaPago').on('change', function(e) {
        CantidadResultadosPago($('#paginacionBusquedaPago').val() ? $('#paginacionBusquedaPago').val() : 5);
    });
    $('#paginacionBusquedaProceso').on('change', function(e) {
        CantidadResultadosProceso($('#paginacionBusquedaProceso').val() ? $('#paginacionBusquedaProceso').val() : 5);
    });
    InicializarDescargas();
});

/**
 * Agrega el cambio de dirección en la url cuando cambia la tabla de contratos
 * @param {number} numero 
 */
function CantidadResultadosContrato(numero) {
    PushDireccionContratos(AccederUrlPagina({ paginaCon: 1, paginarPorCon: numero }));
}

/**
 * Agrega el cambio de dirección en la url cuando cambia la tabla de pagos
 * @param {number} numero 
 */
function CantidadResultadosPago(numero) {
    PushDireccionPagos(AccederUrlPagina({ paginaPag: 1, paginarPorPag: numero }));
}

/**
 * Agrega el cambio de dirección en la url cuando cambia la tabla de procesos
 * @param {number} numero 
 */
function CantidadResultadosProceso(numero) {
    PushDireccionProcesos(AccederUrlPagina({ paginaPro: 1, paginarPorPro: numero }));
}

/**
 * Agrega los eventos a los campos una vez que cambien su valor
 * @param {string} selector -Identifiador de la tabla
 * @param {string} prefijo -Prefijo para Identificar la Tabla
 * @param {Object} funcionFiltros -Funcion para obtener los filtros
 * @param {funcionInput} funcionInput -Funcion a realizar posterior al cambio
 */
function AsignarEventosFiltro(selector, prefijo, funcionFiltros, funcionInput) {
    $(selector + ' .campoFiltrado input[type="text"], '+selector+' .campoFiltrado select.campoBlancoTextoSeleccion').on({
        'change': function(e) {
            var elemento = $(e.currentTarget);
            var elementoPadre = elemento.closest('.campoFiltrado');
            var filtros = {};
            filtros = funcionFiltros();
            switch (elementoPadre.attr('tipo')) {
                case 'fecha':
                    filtros[elementoPadre.attr('filtro')] = ValidarCadena(elemento.val()) ? (elemento.attr('opcion') + elemento.val()) : '';
                    if (!ValidarCadena(filtros[elementoPadre.attr('filtro')])) {
                        delete filtros[elementoPadre.attr('filtro')];
                    }
                    filtros['pagina' + prefijo] = 1;
                    funcionInput(filtros, true);
                    break;
                case 'numero':
                    filtros[elementoPadre.attr('filtro')] = ValidarCadena(elemento.val()) ? (elemento.attr('opcion') + elemento.val()) : '';
                    if (!ValidarCadena(filtros[elementoPadre.attr('filtro')])) {
                        delete filtros[elementoPadre.attr('filtro')];
                    }
                    filtros['pagina' + prefijo] = 1;
                    funcionInput(filtros, true);
                    break;
                default:
                    filtros[elementoPadre.attr('filtro')] = (elemento.val());
                    if (!ValidarCadena(filtros[elementoPadre.attr('filtro')])) {
                        delete filtros[elementoPadre.attr('filtro')];
                    }
                    filtros['pagina' + prefijo] = 1;
                    funcionInput(filtros, true);
                    break;
            }

        }
    })
}

/**
 * Funcion para obtener los datos del comprador y cargar las tablas del mismo
 */
function ObtenerComprador() {
    DebugFecha();
    MostrarEspera('body .tamanoMinimo',true);
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) ,{
        tid:'id'
    }, function(datos) {
        DebugFecha();
        datosComprador = datos;
        OcultarEspera('body .tamanoMinimo');
        if (datos.id) {
            AnadirDatosComprador();
            $('.contenedorTablas').show()
            CargarContratosComprador();
            CargarPagosComprador();
            CargarProcesosComprador();
            VerificarIntroduccion('INTROJS_COMPRADOR', 1);
        } else {
            $('#noEncontrado').show();
        }

    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error get');

        OcultarEspera('body .tamanoMinimo');
        $('#noEncontrado').show();

    });
}

/**
 * Añade los datos del comprador
*/
function AnadirDatosComprador() {
    $('.contenedorInformacion').append(
        $('<div>', { class: 'row mt-5' }).append(
            $('<div>', { class: 'col-md-12' }).append(
                $('<h1>', { class: 'textoColorPrimario mt-3 tituloDetalleProceso', toolTexto: 'buyer.name' }).text(
                    datosComprador.name
                )
            )
        ),
        $('<div>', { class: 'row' }).append(
            $('<h4>', { class: 'text-primary-edcax titularCajonSombreado  col-md-12' }).text(
                'Información del Comprador'
            )
        ),
        $('<div>', { class: 'row' }).append(
            $('<div>', { class: 'col-md-12' }).append(
                $('<div>', { class: 'cajonSombreado contenedorDetalleProcesoDatos', 'data-step': "1", 'data-intro': "Puedes visualizar la dirección de un comprador en esta sección, en caso de que este disponible." }).append(
                    $('<div>', { class: 'contenedorProceso informacionProceso' }).append(

                        (datosComprador.id ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].id" }).text('Identificador:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            VerificarCadenaRTN(datosComprador.id) ?
                                            $('<span>', { class: 'botonGeneral fondoColorPrimario' }).text('RTN') : $('<span>').text(datosComprador.id),
                                            VerificarCadenaRTN(datosComprador.id) ?
                                            $('<span>').text(datosComprador.id.replace('HN-RTN-', ' ')) : null

                                        )
                                    ))) : null),
                        (datosComprador.address && datosComprador.address.streetAddress ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].address.streetAddress" }).text('Dirección:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            datosComprador.address.streetAddress
                                        )
                                    ))) : null)
                    )
                )
            )
        ),
        (datosComprador.contactPoint && (datosComprador.contactPoint.name || datosComprador.contactPoint.telephone || datosComprador.contactPoint.faxNumber || datosComprador.contactPoint.email)) ? $('<div>', { class: 'row' }).append(
            $('<h4>', { class: 'text-primary-edcax titularCajonSombreado  col-md-12 mt-1' }).text(
                'Datos de Contacto'
            )
        ) : null,
        (datosComprador.contactPoint && (datosComprador.contactPoint.name || datosComprador.contactPoint.telephone || datosComprador.contactPoint.faxNumber || datosComprador.contactPoint.email)) ? $('<div>', { class: 'row' }).append(
            $('<div>', { class: 'col-md-12 mt-2' }).append(
                $('<div>', { class: 'cajonSombreado contenedorDetalleProcesoDatos', 'data-step': "2", 'data-intro': "Puedes visualizar los datos de contacto de un comprador en esta sección, en caso de que este disponible." }).append(
                    $('<div>', { class: 'contenedorProceso informacionProceso' }).append(

                        (datosComprador.contactPoint && datosComprador.contactPoint.name ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].contactPoint.name" }).text('Nombre:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            datosComprador.contactPoint.name
                                        )
                                    ))) : null),
                        (datosComprador.contactPoint && datosComprador.contactPoint.telephone ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].contactPoint.telephone" }).text('Telefono:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            datosComprador.contactPoint.telephone
                                        )
                                    ))) : null),
                        (datosComprador.contactPoint && datosComprador.contactPoint.faxNumber ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].contactPoint.faxNumber" }).text('FAX:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            datosComprador.contactPoint.faxNumber
                                        )
                                    ))) : null),
                        (datosComprador.contactPoint && datosComprador.contactPoint.email ?
                            $('<div>', { class: 'contenedorTablaCaracteristicas', style: 'width:100%' }).append(
                                $('<table>').append(
                                    $('<tbody>').append(
                                        $('<tr>').append(
                                            $('<td>', { class: 'tituloTablaCaracteristicas', toolTexto: "parties[n].contactPoint.email" }).text('Correo Electrónico:'),
                                            $('<td>', { class: 'contenidoTablaCaracteristicas' })).append(
                                            datosComprador.contactPoint.email
                                        )
                                    ))) : null)
                    )
                )
            )
        ) : null

    );
}

/**
 * Obtiene los datos de los contratos del comprador
 */
function CargarContratosComprador() {
    $('#resultadosContratosComprador').html(
        $('<tr>').append(
            $('<td>', { style: 'height:300px;position:relative', colspan: '8', id: 'cargando' })
        ));
    MostrarEspera('#cargando');
    var parametros = ObtenerFiltrosContratos();
    EliminarEventoModalDescarga('descargaJsonCompradorContratos');
    EliminarEventoModalDescarga('descargaCsvCompradorContratos');
    EliminarEventoModalDescarga('descargaXlsxCompradorContratos');
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/contratos', parametros).done(function(datos) {
        console.dir('Contratos');
        console.dir(datos);

        AgregarResultadosContratosComprador(datos, '#resultadosContratosComprador');
        MostrarPaginacion(datos, '.ContratosComprador',
            function(e) {
                PaginaContratosComprador($(e.currentTarget).attr('pagina'))
            });


        AgregarToolTips();
        ObtenerDescargaCompradorContratos(datos);




    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');
        AgregarResultadosContratosComprador({ resultados: [] }, '#resultadosContratosComprador');
        AgregarToolTips();

    });
}

/**
 * Obtiene los datos de los pagos del comprador
 */
function CargarPagosComprador() {
    $('#resultadosPagosComprador').html(
        $('<tr>').append(
            $('<td>', { style: 'height:300px;position:relative', colspan: '5', id: 'cargandoPagos' })
        ));
    MostrarEspera('#cargandoPagos');
    var parametros = ObtenerFiltrosPagos();
    EliminarEventoModalDescarga('descargaJsonCompradorPagos');
    EliminarEventoModalDescarga('descargaCsvCompradorPagos');
    EliminarEventoModalDescarga('descargaXlsxCompradorPagos');
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/pagos', parametros).done(function(datos) {
        console.dir('Pagos');
        console.dir(datos);

        AgregarResultadosPagosComprador(datos, '#resultadosPagosComprador')
        MostrarPaginacion(datos, '.PagosComprador',
            function(e) {
                PaginaPagosComprador($(e.currentTarget).attr('pagina'));
            });


        AgregarToolTips();
        ObtenerDescargaCompradorPagos(datos);

    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');
        AgregarResultadosPagosComprador({ resultados: [] }, '#resultadosPagosComprador');
        AgregarToolTips();

    });
}

/**
 * Obtiene los datos de los procesos del comprador
 */
function CargarProcesosComprador() {
    $('#resultadosProcesosComprador').html(
        $('<tr>').append(
            $('<td>', { style: 'height:300px;position:relative', colspan: '8', id: 'cargandoProcesos' })
        ));
    MostrarEspera('#cargandoProcesos');
    var parametros = ObtenerFiltrosProcesos();
    EliminarEventoModalDescarga('descargaJsonCompradorProcesos');
    EliminarEventoModalDescarga('descargaCsvCompradorProcesos');
    EliminarEventoModalDescarga('descargaXlsxCompradorProcesos');
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/procesos', parametros).done(function(datos) {
        console.dir('Procesos');
        console.dir(datos);

        AgregarResultadosProcesosComprador(datos, '#resultadosProcesosComprador')
        MostrarPaginacion(datos, '.ProcesosComprador',
            function(e) {
                PaginaProcesosComprador($(e.currentTarget).attr('pagina'))
            });


        AgregarToolTips();
        ObtenerDescargaCompradorProcesos(datos);

    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');
        AgregarResultadosProcesosComprador({ resultados: [] }, '#resultadosProcesosComprador');
        AgregarToolTips();

    });
}

/**
 * Agrega los resultados a la tabla de contratos
 * @param {Object} datos -Datos de los resultados
 * @param {string} selector -Identificador donde se encuentra 
 */
function AgregarResultadosContratosComprador(datos, selector) {
    var resultados = datos.resultados;
    $(selector).html('');
    for (var i = 0; i < resultados.length; i++) {
        $(selector).append(
            $('<tr>').append(
                $('<td>', { 'data-label': 'Proveedor' }).append(

                    resultados[i] && resultados[i]._source && resultados[i]._source.suppliers?ObtenerProveedoresArreglo(resultados[i]._source.suppliers):$('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                $('<td>', { 'data-label': 'Título de Contrato', class: 'textoAlineadoIzquierda' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.title ? $('<a>', { class: 'enlaceTablaGeneral', href: '/proceso/' + encodeURIComponent(resultados[i]._source.extra.ocid) + '/?contrato=' + resultados[i]._source.id, toolTexto: resultados[i]._source.title }).text(ReducirTexto(resultados[i]._source.title, 80)) : $('<a>', { class: 'enlaceTablaGeneral', href: '/proceso/' + encodeURIComponent(resultados[i]._source.extra.ocid) + '/?contrato=' + resultados[i]._source.id }).text('Sin título')
                ),
                $('<td>', { 'data-label': 'Descripción', class: 'textoAlineadoIzquierda' }).append(
                    $('<span>', { text: ReducirTexto(resultados[i] && resultados[i]._source && resultados[i]._source.description ? resultados[i]._source.description : '', 80), toolTexto: resultados[i] && resultados[i]._source && resultados[i]._source.description ? resultados[i]._source.description : '' })

                ),
                $('<td>', { 'data-label': 'Nombre del Proceso', class: 'textoAlineadoCentrado' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.extra && resultados[i]._source.extra.tenderTitle ? resultados[i]._source.extra.tenderTitle : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                
                $('<td>', { 'data-label': 'Categoría de Compras', class: 'textoAlineadoCentrado' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.localProcurementCategory ? (categoriaCompra[resultados[i]._source.localProcurementCategory]?categoriaCompra[resultados[i]._source.localProcurementCategory].titulo: resultados[i]._source.localProcurementCategory) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                $('<td>', { 'data-label': 'Monto del Contrato', class: 'textoAlineadoDerecha' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.value && Validar(resultados[i]._source.value.amount) ? [ValorMoneda(resultados[i]._source.value.amount), $('<span>', { class: 'textoColorPrimario', text: ' ' + resultados[i]._source.value.currency })] : ''
                ),
                $('<td>', { 'data-label': 'Fecha de Firma del Contrato', class: 'textoAlineadoCentrado' }).append(
                    $('<span>', { class: resultados[i] && resultados[i]._source && resultados[i]._source.dateSigned && resultados[i]._source.dateSigned != 'NaT' ? '' : 'textoColorGris' }).text(
                        resultados[i] && resultados[i]._source && resultados[i]._source.dateSigned && resultados[i]._source.dateSigned != 'NaT' ? ObtenerFecha(resultados[i]._source.dateSigned, 'fecha') : 'No Disponible'
                    )
                ),
                $('<td>', { 'data-label': 'Estado', class: 'textoAlineadoCentrado' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.status ? (estadosContrato[resultados[i]._source.status]? estadosContrato[resultados[i]._source.status].titulo:resultados[i]._source.status) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                )
            )
        );
    }
    if (!resultados.length) {
        $(selector).append(
            $('<tr>', { style: '' }).append(
                $('<td>', { 'data-label': '', 'colspan': 8 }).append(
                    $('<h4>', { class: 'titularColor textoColorPrimario mt-3 mb-3' }).text('No se Encontraron Contratos')
                )));
    }
}


/**
 * Obtiene la Subtabla de pagos
 * @param {Object} datos -Obtiene datos de la subtabla
 * @return {Object[]} -Retorna un arreglo de objetos html correspondientes a cada pago
 */
function ObtenerFilasSubTablaPagos(datos){
    var elementos = [];
    if (datos && datos.implementation && datos.implementation.transactions && datos.implementation.transactions.length) {
        for (let i = 0; i < datos.implementation.transactions.length; i++) {
            elementos.push(
                $('<tr>').append(
                    $('<td>', { 'data-label': 'Descripción de la Transacción' }).append(
                        
                        ObtenerObligacionesTransaccion(datos.implementation.transactions[i],datos.implementation.financialObligations) && ObtenerObligacionesTransaccion(datos.implementation.transactions[i],datos.implementation.financialObligations).length && ObtenerObligacionesTransaccion(datos.implementation.transactions[i],datos.implementation.financialObligations)[0].description? $('<span>', { text: ReducirTexto(ObtenerObligacionesTransaccion(datos.implementation.transactions[i],datos.implementation.financialObligations)[0].description, 80), toolTexto: ObtenerObligacionesTransaccion(datos.implementation.transactions[i],datos.implementation.financialObligations)[0].description}) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                    ),
                    $('<td>', { 'data-label': 'Objeto de Gasto' }).append(
                        datos && datos.extra && datos.extra.objetosGasto && datos.extra.objetosGasto.length ? $('<span>', { text: ReducirTexto(datos.extra.objetosGasto.join(', '), 80), toolTexto: datos.extra.objetosGasto.join(', ')}) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                        
                    ),
                    $('<td>', { 'data-label': 'Monto del Pago' }).append(
                        datos.implementation.transactions[i].value && Validar(datos.implementation.transactions[i].value.amount) ? [ValorMoneda(datos.implementation.transactions[i].value.amount), $('<span>', { class: 'textoColorPrimario', text: ' '+(datos.implementation.transactions[i].date.currency?datos.implementation.transactions[i].date.currency:'HNL') })] : ''
                    ),
                    $('<td>', { 'data-label': 'Fecha del Pago' }).append(
                        $('<span>', { class: datos.implementation.transactions[i].date&&datos.implementation.transactions[i].date != 'NaT' ? '' : 'textoColorGris' }).text(
                            datos.implementation.transactions[i].date && datos.implementation.transactions[i].date != 'NaT' ? ObtenerFecha(datos.implementation.transactions[i].date, 'fecha') : 'No Disponible'
                        )
                    )
                )
            );
            
            
        }
    }
    if(!elementos.length){
        elementos.push(
            $('<tr>', { style: '' }).append(
                $('<td>', { 'data-label': '', 'colspan': 4 }).append(
                    $('<h4>', { class: 'titularColor textoColorPrimario mt-3 mb-3' }).text('No hay pagos disponibles')
                )));
    }
    return elementos;
}

/**
 * Agrega una fila en la tabla de pagos
 * @param {Object} resultados -Fila de resultados
 * @param {string} selector -Contendor donde se agregara la fila
 * @param {number} i -Indice
 */
function AgregarFilaPago(resultados,selector,i){
    $(selector).append(
        $('<tr>',{
            class:'filaSubTabla transicion',
            toolPosicon:'right-end',
            toolTexto:'<span class="far fa-eye"><span> <span style="font-family:Roboto">Puedes ver los pagos <br> haciendo CLICK en esta fila.<span>',
            on: {
                click: function(e){
                    if(!$(e.currentTarget).hasClass('abierta')){
                        $(e.currentTarget).after($('<tr class="subTabla">').append(
                            $('<td colspan="'+$(e.currentTarget).find('td').length+'">').append(
                                $('<div>',{class:'cajonSombreado'}).append(
                                    $('<h6>',{class:'textoColorPrimario textoTitulo'}).text('Pagos'),
                                    $('<table>',{class:'tablaGeneral'}).append(
                                        $('<thead>').append(
                                            $('<tr>').append(
                                                $('<th>',{toolTexto:"contracts[n].implementation .financialObligations[n].description",text:'Descripción de la transacción'}),
                                                $('<th>',{toolTexto:"planning.budget.budgetBreakdown[n] .classifications.objeto",text:'Objeto de gasto'}),
                                                $('<th>',{toolTexto:"contracts[n].implementation. transactions[n].value.amount",text:'Monto del pago'}),
                                                $('<th>',{toolTexto:"contracts[n].implementation .transactions[n].date",text:'Fecha del pago'})
                                            )
                                        ),
                                        $('<tbody>').append(
                                            ObtenerFilasSubTablaPagos(resultados[i]._source)
                                        )
                                    )

                                )
                            )
                            
                        ));
                        $(e.currentTarget).addClass('abierta');
                    }else{
                        if($(e.currentTarget).next('.subTabla').length){
                            $(e.currentTarget).next('.subTabla').remove();
                            $(e.currentTarget).removeClass('abierta');
                        }
                        
                    }
                    AgregarToolTips();
                }
            }
        }).append(
            $('<td>', { 'data-label': 'Comprador' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.extra && resultados[i]._source.extra.buyer&&resultados[i]._source.extra.buyer.id ? $('<a>', { class: 'enlaceTablaGeneral', href: '/comprador/' + encodeURIComponent(resultados[i]._source.extra.buyer.id) }).text(resultados[i]._source.extra.buyerFullName) : ''
            ),
            $('<td>', { 'data-label': 'Proveedor' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.implementation && resultados[i]._source.implementation.transactions&& resultados[i]._source.implementation.transactions.length? ObtenerProveedoresTransacciones(resultados[i]._source.implementation.transactions):$('<span>', { class: 'textoColorGris' }).text('No Disponible')
               
            ),
            $('<td>', { 'data-label': 'Título de Contrato', class: 'textoAlineadoIzquierda' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.title ? $('<a>', { class: 'enlaceTablaGeneral', href: '/proceso/' + encodeURIComponent(resultados[i]._source.extra.ocid) + '/?contrato=' + resultados[i]._source.id, toolTexto: resultados[i]._source.title }).text(ReducirTexto(resultados[i]._source.title, 80)) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
            ),
            $('<td>', { 'data-label': 'Monto del Contrato', class: 'textoAlineadoDerecha' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.value && Validar(resultados[i]._source.value.amount) ? [ValorMoneda(resultados[i]._source.value.amount), $('<span>', { class: 'textoColorPrimario', text: ' ' + resultados[i]._source.value.currency })] : ''


            ),
            $('<td>', { 'data-label': 'Suma de Todos los Pagos', class: 'textoAlineadoDerecha' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.extra && Validar(resultados[i]._source.extra.sumTransactions) ? [ValorMoneda(resultados[i]._source.extra.sumTransactions), $('<span>', { class: 'textoColorPrimario', text: ' HNL' })] : ''


            ),

            $('<td>', { 'data-label': 'Fecha de Último Pago', class: 'textoAlineadoCentrado' }).append(
                $('<span>', { class: resultados[i] && resultados[i]._source && resultados[i]._source.extra && resultados[i]._source.extra.transactionLastDate && resultados[i]._source.extra.transactionLastDate != 'NaT' ? '' : 'textoColorGris' }).text(
                    resultados[i] && resultados[i]._source && resultados[i]._source.extra && resultados[i]._source.extra.transactionLastDate && resultados[i]._source.extra.transactionLastDate != 'NaT' ? ObtenerFecha(resultados[i]._source.extra.transactionLastDate, 'fecha') : 'No Disponible'
                )

            )

        )
    );
}

/**
 * Obtiene las transacciones de los proveedores
 * @param {Object[]} transacciones -Arreglo de transacciones
 * @return {Object[]}
 */
function ObtenerProveedoresTransacciones(transacciones){
    var elementos=[];
    for (var i=0; i< transacciones.length; i++) {
        if(transacciones[i].payee&&transacciones[i].payee.id){
            elementos.push(
                $('<a>',{text:transacciones[i].payee.name,class:'enlaceTablaGeneral',href:'/proveedor/'+transacciones[i].payee.id})
            );
        }
    }
    return elementos;
}

/**
 * Agrega las filas de resultados en los pagos de un comprador
 * @param {Object} datos -datos en la respuesta del metodo de pagos
 * @param {string} selector -contenedor HTML de resultados de pagos
 */
function AgregarResultadosPagosComprador(datos, selector) {
    var resultados = datos.resultados;
    $(selector).html('');
    for (var i = 0; i < resultados.length; i++) {
        AgregarFilaPago(resultados,selector,i);
    }
    if (!resultados.length) {
        $(selector).append(
            $('<tr>', { style: '' }).append(
                $('<td>', { 'data-label': '', 'colspan': 5 }).append(
                    $('<h4>', { class: 'titularColor textoColorPrimario mt-3 mb-3' }).text('No se Encontraron Pagos')
                )));
    }
}

/**
 * Obtiene un arreglo de elementos HTML de los datos de los proveedores
 * @param {Object} datos -Arreglo de proveedores
 */
function ObtenerProveedoresArreglo(datos) {
    var elementos = [];
    if (datos && datos.length) {
        for (let i = 0; i < datos.length; i++) {
            if (datos[i].id) {
                elementos.push(
                    $('<a>', { class: 'enlaceTablaGeneral', href: '/proveedor/' + encodeURIComponent(datos[i].id) }).text(datos[i].name)
                )
                elementos.push(' ');
            }
        }
    }
    return elementos;
}

/**
 * Agrega los resultados en la tabla de procesos del comprador
 * @param {Object} datos -datos de la respuesta
 * @param {string} selector -contenedor donde se desean agregar
 */
function AgregarResultadosProcesosComprador(datos, selector) {
    var resultados = datos.resultados;
    $(selector).html('');
    for (var i = 0; i < resultados.length; i++) {
        $(selector).append(
            $('<tr>').append(
                $('<td>', { 'data-label': 'Comprador' }).append(
                resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.buyer && resultados[i]._source.doc.compiledRelease.buyer.id ?
                $('<a>', { class: 'enlaceTablaGeneral', href: '/comprador/' + encodeURIComponent(resultados[i]._source.doc.compiledRelease.buyer.id)}).text(
                    resultados[i]._source.extra&&resultados[i]._source.extra.buyerFullName?resultados[i]._source.extra.buyerFullName:'Comprador'
                    ) :  $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                $('<td>', { 'data-label': 'Id del Proceso' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.ocid ? $('<a>', { class: 'enlaceTablaGeneral', href: '/proceso/' + encodeURIComponent(resultados[i]._source.doc.ocid) }).text(resultados[i]._source.doc.ocid) : ''
                ),
                $('<td>', { 'data-label': 'Título del Proceso' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.tender && resultados[i]._source.doc.compiledRelease.tender.title ? $('<a>', { class: 'enlaceTablaGeneral', href: '/proceso/' + encodeURIComponent(resultados[i]._source.doc.ocid) }).text(resultados[i]._source.doc.compiledRelease.tender.title) : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                $('<td>', { 'data-label': 'Modalidad de Compra' }).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.tender && resultados[i]._source.doc.compiledRelease.tender.procurementMethodDetails ? resultados[i]._source.doc.compiledRelease.tender.procurementMethodDetails : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),

                $('<td>', { 'data-label': 'Fecha de Publicación de la Licitación', class: 'textoAlineadoCentrado' }).append(
                    $('<span>', { class: resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.date && resultados[i]._source.doc.compiledRelease.date != 'NaT' ? '' : 'textoColorGris' }).text(
                        resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.date && resultados[i]._source.doc.compiledRelease.date != 'NaT' ? ObtenerFecha(resultados[i]._source.doc.compiledRelease.date, 'fecha') : 'No Disponible'
                    )
                ),
                $('<td>', { 'data-label': 'Fecha de Recepción de Ofertas', class: 'textoAlineadoCentrado' }).append(
                    $('<span>', { class: resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.tender && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod.endDate && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod.endDate != 'NaT' ? '' : 'textoColorGris' }).text(
                        resultados[i] && resultados[i]._source && resultados[i]._source.doc && resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.tender && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod.endDate && resultados[i]._source.doc.compiledRelease.tender.tenderPeriod.endDate != 'NaT' ? ObtenerFecha(resultados[i]._source.doc.compiledRelease.tender.tenderPeriod.endDate, 'fecha') : 'No Disponible'
                    )

                ),
                $('<td>', { 'data-label': 'Estado' , class: 'textoAlineadoCentrado'}).append(
                    resultados[i] && resultados[i]._source && resultados[i]._source.extra && resultados[i]._source.extra.lastSection ? (estadoProceso[resultados[i]._source.extra.lastSection]?estadoProceso[resultados[i]._source.extra.lastSection].titulo: resultados[i]._source.extra.lastSection)
                     : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                ),
                $('<td>', { 'data-label': 'Monto Contratado', class: 'textoAlineadoDerecha' }).append(
                    resultados[i]._source.doc.compiledRelease && resultados[i]._source.doc.compiledRelease.tender && resultados[i]._source.doc.compiledRelease.tender.extra && resultados[i]._source.doc.compiledRelease.tender.extra && Validar(resultados[i]._source.doc.compiledRelease.tender.extra.sumContracts) ? [ValorMoneda(resultados[i]._source.doc.compiledRelease.tender.extra.sumContracts), $('<span>', { class: 'textoColorPrimario', text: ' HNL' })] : $('<span>', { class: 'textoColorGris' }).text('No Disponible')
                )
            )
        );
    }
    if (!resultados.length) {
        $(selector).append(
            $('<tr>', { style: '' }).append(
                $('<td>', { 'data-label': '', 'colspan': 8 }).append(
                    $('<h4>', { class: 'titularColor textoColorPrimario mt-3 mb-3' }).text('No se Encontraron Procesos')
                )));
    }
}

/**
 * Agrega la paginación de una página
 * @param {Object} datos -datos de la respuesta del metodo
 * @param {string} selector -Contenedor donde se añadira la paginación
 * @param {Object} funcion -Funcion que se ejecutara al hacer clic en una página
 */
function MostrarPaginacion(datos, selector, funcion) {
    var paginarPor = datos.parametros.paginarPor ? datos.parametros.paginarPor : (datos.parametros.pagianrPor ? datos.parametros.pagianrPor : 5);
    var pagina = datos.parametros.pagina ? datos.parametros.pagina : 1;
    var paginacion = ObtenerPaginacion(datos.paginador.page, Math.ceil(ObtenerNumero(datos.paginador['total.items']) / ObtenerNumero(paginarPor))  )
    $('.navegacionTablaGeneral' + selector).html('');
    if (datos.paginador.has_previous) {
        $('.navegacionTablaGeneral' + selector).append(
            $('<a href="javascript:void(0)"  pagina="' + datos.paginador.previous_page_number + '"  class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-left"></i></span></a>').on({
                click: funcion
            })
        );
    }
    for (var i = 0; i < paginacion.length; i++) {
        if (paginacion[i] == '...') {
            $('.navegacionTablaGeneral' + selector).append(
                $('<a href="javascript:void(0)" class="numerosNavegacionTablaGeneral numeroNormalNavegacionTablaGeneral">').append($('<span>').text(paginacion[i]))
            );
        } else {
            $('.navegacionTablaGeneral' + selector).append(
                $('<a href="javascript:void(0)" pagina="' + paginacion[i] + '"  class="numerosNavegacionTablaGeneral ' + ((paginacion[i] == datos.paginador.page) ? 'current' : '') + '">').on({
                    click: funcion

                }).append($('<span>').text(paginacion[i]))
            );
        }
    }
    if (datos.paginador.has_next) {
        $('.navegacionTablaGeneral' + selector).append(
            $('<a href="javascript:void(0)" pagina="' + datos.paginador.next_page_number + '" class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-right"></i></span></a>').on({
                click: funcion

            })
        );
    }

    $('.totalResultado' + selector).html(datos.paginador['total.items']);
    $('.inicioResultado' + selector).html((ObtenerNumero(paginarPor) * (ObtenerNumero(pagina))) - ObtenerNumero(paginarPor));
    $('.finResultado' + selector).html(((ObtenerNumero(paginarPor) * (ObtenerNumero(pagina)) > ObtenerNumero(datos.paginador['total.items'])) ? ObtenerNumero(datos.paginador['total.items']) : (ObtenerNumero(paginarPor) * (ObtenerNumero(pagina)))));


}

/**
 * Agrega la página con el filtrado que se hizo en los contratos
 * @param {number} numero -numero de pagina que se desea agregar
 */
function PaginaContratosComprador(numero) {
    PushDireccionContratos(AccederUrlPagina({ paginaCon: numero }));
}

/**
 * Agrega la página con el filtrado que se hizo en los pagos
 * @param {number} numero -numero de pagina que se desea agregar
 */
function PaginaPagosComprador(numero) {
    PushDireccionPagos(AccederUrlPagina({ paginaPag: numero }));
}

/**
 * Agrega la página con el filtrado que se hizo en los procesos
 * @param {number} numero -numero de pagina que se desea agregar
 */
function PaginaProcesosComprador(numero) {
    PushDireccionProcesos(AccederUrlPagina({ paginaPro: numero }));
}

/**
 * Agrega la página al historial con el filtrado que se hizo en los contratos
 * @param {string} direccion -nueva direccion de la pagina
 */
function PushDireccionContratos(direccion) {
    window.history.pushState({}, document.title, direccion);
    CargarContratosComprador();
}

/**
 * Agrega la página al historial con el filtrado que se hizo en los pagos
 * @param {string} direccion -nueva direccion de la pagina
 */
function PushDireccionPagos(direccion) {
    window.history.pushState({}, document.title, direccion);
    CargarPagosComprador();
}

/**
 * Agrega la página al historial con el filtrado que se hizo en los procesos
 * @param {string} direccion -nueva direccion de la pagina
 */
function PushDireccionProcesos(direccion) {
    window.history.pushState({}, document.title, direccion);
    CargarProcesosComprador();
}

/**
 * Agrega la página con el filtrado que se hizo en los contratos
 * @param {Object} filtros -parametros ya agregados
 * @param {boolean} desUrl -descarta agregar los valores actuales en la url
 */
function InputFiltroContratos(filtros, desUrl) {
    PushDireccionContratos(AccederUrlPagina(filtros, desUrl));
}

/**
 * Agrega la página con el filtrado que se hizo en los pagos
 * @param {Object} filtros -parametros ya agregados
 * @param {boolean} desUrl -descarta agregar los valores actuales en la url
 */
function InputFiltroPagos(filtros, desUrl) {
    PushDireccionPagos(AccederUrlPagina(filtros, desUrl));
}

/**
 * Agrega la página con el filtrado que se hizo en los procesos
 * @param {Object} filtros -parametros ya agregados
 * @param {boolean} desUrl -descarta agregar los valores actuales en la url
 */
function InputFiltroProcesos(filtros, desUrl) {
    PushDireccionProcesos(AccederUrlPagina(filtros, desUrl));
}


/**
 * Regresa una dirección con los filtros aplicados
 * @param {Object} opciones -parametros ya agregados
 * @param {boolean} desUrl -descarta agregar los valores actuales en la url
 * @return {string}
 */
function AccederUrlPagina(opciones, desUrl) {
    var direccion = ('/comprador/' + encodeURIComponent(compradorId) + '/?' +

        'paginaCon=' + (opciones.paginaCon ? opciones.paginaCon : (ObtenerNumero(ObtenerValor('paginaCon')) ? ObtenerNumero(ObtenerValor('paginaCon')) : 1)) +
        '&paginarPorCon=' + (opciones.paginarPorCon ? opciones.paginarPorCon : (ObtenerNumero(ObtenerValor('paginarPorCon')) ? ObtenerNumero(ObtenerValor('paginarPorCon')) : 5)) +
        (ValidarCadena(opciones.compradorCon) ? '&compradorCon=' + encodeURIComponent(opciones.compradorCon) : (ValidarCadena(ObtenerValor('compradorCon')) && !desUrl ? '&compradorCon=' + ObtenerValor('compradorCon') : '')) +
        (ValidarCadena(opciones.proveedorCon) ? '&proveedorCon=' + encodeURIComponent(opciones.proveedorCon) : (ValidarCadena(ObtenerValor('proveedorCon')) && !desUrl ? '&proveedorCon=' + ObtenerValor('proveedorCon') : '')) +
        (ValidarCadena(opciones.tituloCon) ? '&tituloCon=' + encodeURIComponent(opciones.tituloCon) : (ValidarCadena(ObtenerValor('tituloCon')) && !desUrl ? '&tituloCon=' + ObtenerValor('tituloCon') : '')) +
        (ValidarCadena(opciones.descripcionCon) ? '&descripcionCon=' + encodeURIComponent(opciones.descripcionCon) : (ValidarCadena(ObtenerValor('descripcionCon')) && !desUrl ? '&descripcionCon=' + ObtenerValor('descripcionCon') : '')) +
        (ValidarCadena(opciones.tituloLicitacionCon) ? '&tituloLicitacionCon=' + encodeURIComponent(opciones.tituloLicitacionCon) : (ValidarCadena(ObtenerValor('tituloLicitacionCon')) && !desUrl ? '&tituloLicitacionCon=' + ObtenerValor('tituloLicitacionCon') : '')) +
        (ValidarCadena(opciones.categoriaCompraCon) ? '&categoriaCompraCon=' + encodeURIComponent(opciones.categoriaCompraCon) : (ValidarCadena(ObtenerValor('categoriaCompraCon')) && !desUrl ? '&categoriaCompraCon=' + ObtenerValor('categoriaCompraCon') : '')) +
        (ValidarCadena(opciones.fechaFirmaCon) ? '&fechaFirmaCon=' + encodeURIComponent(opciones.fechaFirmaCon) : (ValidarCadena(ObtenerValor('fechaFirmaCon')) && !desUrl ? '&fechaFirmaCon=' + ObtenerValor('fechaFirmaCon') : '')) +
        (ValidarCadena(opciones.fechaInicioCon) ? '&fechaInicioCon=' + encodeURIComponent(opciones.fechaInicioCon) : (ValidarCadena(ObtenerValor('fechaInicioCon')) && !desUrl ? '&fechaInicioCon=' + ObtenerValor('fechaInicioCon') : '')) +
        (ValidarCadena(opciones.montoCon) ? '&montoCon=' + encodeURIComponent(reemplazarValor(opciones.montoCon, ',', '')) : (ValidarCadena(ObtenerValor('montoCon')) && !desUrl ? '&montoCon=' + ObtenerValor('montoCon') : '')) +
        (ValidarCadena(opciones.estadoCon) ? '&estadoCon=' + encodeURIComponent(opciones.estadoCon) : (ValidarCadena(ObtenerValor('estadoCon')) && !desUrl ? '&estadoCon=' + ObtenerValor('estadoCon') : '')) +
        (ValidarCadena(opciones.ordenarPorCon) ? '&ordenarPorCon=' + encodeURIComponent(opciones.ordenarPorCon) : (ValidarCadena(ObtenerValor('ordenarPorCon')) && !desUrl ? '&ordenarPorCon=' + ObtenerValor('ordenarPorCon') : '')) +


        '&paginaPag=' + (opciones.paginaPag ? opciones.paginaPag : (ObtenerNumero(ObtenerValor('paginaPag')) ? ObtenerNumero(ObtenerValor('paginaPag')) : 1)) +
        '&paginarPorPag=' + (opciones.paginarPorPag ? opciones.paginarPorPag : (ObtenerNumero(ObtenerValor('paginarPorPag')) ? ObtenerNumero(ObtenerValor('paginarPorPag')) : 5)) +
        (ValidarCadena(opciones.compradorPag) ? '&compradorPag=' + encodeURIComponent(opciones.compradorPag) : (ValidarCadena(ObtenerValor('compradorPag')) && !desUrl ? '&compradorPag=' + ObtenerValor('compradorPag') : '')) +
        (ValidarCadena(opciones.proveedorPag) ? '&proveedorPag=' + encodeURIComponent(opciones.proveedorPag) : (ValidarCadena(ObtenerValor('proveedorPag')) && !desUrl ? '&proveedorPag=' + ObtenerValor('proveedorPag') : '')) +
        (ValidarCadena(opciones.tituloPag) ? '&tituloPag=' + encodeURIComponent(opciones.tituloPag) : (ValidarCadena(ObtenerValor('tituloPag')) && !desUrl ? '&tituloPag=' + ObtenerValor('tituloPag') : '')) +
        (ValidarCadena(opciones.montoPag) ? '&montoPag=' + encodeURIComponent(reemplazarValor(opciones.montoPag, ',', '')) : (ValidarCadena(ObtenerValor('montoPag')) && !desUrl ? '&montoPag=' + ObtenerValor('montoPag') : '')) +
        (ValidarCadena(opciones.pagosPag) ? '&pagosPag=' + encodeURIComponent(reemplazarValor(opciones.pagosPag, ',', '')) : (ValidarCadena(ObtenerValor('tituloPag')) && !desUrl ? '&tituloPag=' + ObtenerValor('tituloPag') : '')) +
        (ValidarCadena(opciones.fechaPag) ? '&fechaPag=' + encodeURIComponent(reemplazarValor(opciones.fechaPag, ',', '')) : (ValidarCadena(ObtenerValor('fechaPag')) && !desUrl ? '&fechaPag=' + ObtenerValor('fechaPag') : '')) +
        (ValidarCadena(opciones.ordenarPorPag) ? '&ordenarPorPag=' + encodeURIComponent(opciones.ordenarPorPag) : (ValidarCadena(ObtenerValor('ordenarPorPag')) && !desUrl ? '&ordenarPorPag=' + ObtenerValor('ordenarPorPag') : '')) +

        '&paginaPro=' + (opciones.paginaPro ? opciones.paginaPro : (ObtenerNumero(ObtenerValor('paginaPro')) ? ObtenerNumero(ObtenerValor('paginaPro')) : 1)) +
        '&paginarPorPro=' + (opciones.paginarPorPro ? opciones.paginarPorPro : (ObtenerNumero(ObtenerValor('paginarPorPro')) ? ObtenerNumero(ObtenerValor('paginarPorPro')) : 5)) +
        (ValidarCadena(opciones.compradorPro) ? '&compradorPro=' + encodeURIComponent(opciones.compradorPro) : (ValidarCadena(ObtenerValor('compradorPro')) && !desUrl ? '&compradorPro=' + ObtenerValor('compradorPro') : '')) +
        (ValidarCadena(opciones.ocidPro) ? '&ocidPro=' + encodeURIComponent(opciones.ocidPro) : (ValidarCadena(ObtenerValor('ocidPro')) && !desUrl ? '&ocidPro=' + ObtenerValor('ocidPro') : '')) +
        (ValidarCadena(opciones.tituloPro) ? '&tituloPro=' + encodeURIComponent(opciones.tituloPro) : (ValidarCadena(ObtenerValor('tituloPro')) && !desUrl ? '&tituloPro=' + ObtenerValor('tituloPro') : '')) +
        (ValidarCadena(opciones.categoriaCompraPro) ? '&categoriaCompraPro=' + encodeURIComponent(opciones.categoriaCompraPro) : (ValidarCadena(ObtenerValor('categoriaCompraPro')) && !desUrl ? '&categoriaCompraPro=' + ObtenerValor('categoriaCompraPro') : '')) +
        (ValidarCadena(opciones.fechaPublicacionPro)? '&fechaPublicacionPro='+encodeURIComponent(opciones.fechaPublicacionPro): (ValidarCadena(ObtenerValor('fechaPublicacionPro'))&&!desUrl?'&fechaPublicacionPro='+ObtenerValor('fechaPublicacionPro'):''))+
        (ValidarCadena(opciones.fechaRecepcionPro)? '&fechaRecepcionPro='+encodeURIComponent(opciones.fechaRecepcionPro): (ValidarCadena(ObtenerValor('fechaRecepcionPro'))&&!desUrl?'&fechaRecepcionPro='+ObtenerValor('fechaRecepcionPro'):''))+
        (ValidarCadena(opciones.montoContratadoPro) ? '&montoContratadoPro=' + encodeURIComponent(reemplazarValor(opciones.montoContratadoPro, ',', '')) : (ValidarCadena(ObtenerValor('montoContratadoPro')) && !desUrl ? '&montoContratadoPro=' + ObtenerValor('montoContratadoPro') : ''))+


        


        



        (ValidarCadena(opciones.clasificacionPro) ? '&clasificacionPro=' + encodeURIComponent(opciones.clasificacionPro) : (ValidarCadena(ObtenerValor('clasificacionPro')) && !desUrl ? '&clasificacionPro=' + ObtenerValor('clasificacionPro') : '')) +
        (ValidarCadena(opciones.cantidadContratosPro) ? '&cantidadContratosPro=' + encodeURIComponent(reemplazarValor(opciones.cantidadContratosPro, ',', '')) : (ValidarCadena(ObtenerValor('cantidadContratosPro')) && !desUrl ? '&cantidadContratosPro=' + ObtenerValor('cantidadContratosPro') : '')) +
        (ValidarCadena(opciones.estadoPro) ? '&estadoPro=' + encodeURIComponent(opciones.estadoPro) : (ValidarCadena(ObtenerValor('estadoPro')) && !desUrl ? '&estadoPro=' + ObtenerValor('estadoPro') : ''))+
        
        (ValidarCadena(opciones.ordenarPorPro) ? '&ordenarPorPro='+encodeURIComponent(opciones.ordenarPorPro):(ValidarCadena(ObtenerValor('ordenarPorPro'))&&!desUrl?'&ordenarPorPro='+ObtenerValor('ordenarPorPro'):''))

    );



    return direccion;
}

/**
 * Obtiene el valor de un parametro de orden
 * @param {string} texto -filtro de orden aplicado
 */
function ObtenerOrdenConversion(texto) {
    texto = ObtenerTexto(texto);
    if (/desc\(/.test(texto)) {
        texto = texto.replace('desc(', '').replace(')', '')
        return 'desc(' + filtrosAPropiedades[texto] + ')';
    } else if (/asc\(/.test(texto)) {
        texto = texto.replace('asc(', '').replace(')', '')
        return 'asc(' + filtrosAPropiedades[texto] + ')';
    }
    return texto;
}

/**
 * Obtiene un json con todos los filtros aplicados 
 * @param {string} prefijo -si se desean obtener las propiedasdes con un prefijo
 */
function ObtenerFiltrosGenerales(prefijo){
    if(!prefijo){
      return Object.assign(ObtenerFiltrosPagos('Pag'),ObtenerFiltrosContratos('Con'),ObtenerFiltrosProcesos('Pro'));
    }else{
      switch(prefijo){
        case 'Con':
          return ObtenerFiltrosContratos(prefijo);
        case 'Pag':
          return ObtenerFiltrosPagos(prefijo);
        case 'Pro':
          return ObtenerFiltrosProcesos(prefijo);
        default:
          return {};
      }
    }
  }

/**
 * Obtiene un json con los filtros aplicados de contratos
 * @param {string} prefijo -si se desean obtener las propiedasdes con un prefijo
 */
function ObtenerFiltrosContratos(prefijo) {
    var parametros = {}
    parametros[prefijo ? 'pagina' + prefijo : 'pagina'] = ObtenerNumero(ObtenerValor('paginaCon')) ? ObtenerNumero(ObtenerValor('paginaCon')) : 1;
    parametros[prefijo ? 'paginarPor' + prefijo : 'paginarPor'] = ObtenerNumero(ObtenerValor('paginarPorCon')) ? ObtenerNumero(ObtenerValor('paginarPorCon')) : 5;
    if (Validar(ObtenerValor('proveedorCon'))) {
        parametros[prefijo ? 'proveedor' + prefijo : 'proveedor'] = decodeURIComponent(ObtenerValor('proveedorCon'));
    }
    if (Validar(ObtenerValor('tituloCon'))) {
        parametros[prefijo ? 'titulo' + prefijo : 'titulo'] = decodeURIComponent(ObtenerValor('tituloCon'));
    }
    if (Validar(ObtenerValor('descripcionCon'))) {
        parametros[prefijo ? 'descripcion' + prefijo : 'descripcion'] = decodeURIComponent(ObtenerValor('descripcionCon'));
    }
    if (Validar(ObtenerValor('categoriaCompraCon'))) {
        parametros[prefijo ? 'categoriaCompra' + prefijo : 'categoriaCompra'] = decodeURIComponent(ObtenerValor('categoriaCompraCon'));
    }
    if (Validar(ObtenerValor('fechaFirmaCon'))) {
        parametros[prefijo ? 'fechaFirma' + prefijo : 'fechaFirma'] = decodeURIComponent(ObtenerValor('fechaFirmaCon'));
    }
    if (Validar(ObtenerValor('fechaInicioCon'))) {
        parametros[prefijo ? 'fechaInicio' + prefijo : 'fechaInicio'] = decodeURIComponent(ObtenerValor('fechaInicioCon'));
    }
    if (Validar(ObtenerValor('montoCon'))) {
        parametros[prefijo ? 'monto' + prefijo : 'monto'] = decodeURIComponent(ObtenerValor('montoCon'));
    }
    if (Validar(ObtenerValor('estadoCon'))) {
        parametros[prefijo ? 'estado' + prefijo : 'estado'] = decodeURIComponent(ObtenerValor('estadoCon'));
    }
    if (Validar(ObtenerValor('tituloLicitacionCon'))) {
        parametros[prefijo ? 'tituloLicitacion' + prefijo : 'tituloLicitacion'] = decodeURIComponent(ObtenerValor('tituloLicitacionCon'));
    }
    if (Validar(ObtenerValor('dependencias'))) {
        parametros['dependencias'] = decodeURIComponent(ObtenerValor('dependencias'));
    }
    if (Validar(ObtenerValor('ordenarPorCon'))) {
        parametros[prefijo ? 'ordenarPor' + prefijo : 'ordenarPor'] = prefijo ? decodeURIComponent(ObtenerValor('ordenarPorCon')) : ObtenerOrdenConversion(decodeURIComponent(ObtenerValor('ordenarPorCon')));
    }
    return parametros;
}

/**
 * Obtiene un json con los filtros aplicados de pagos
 * @param {string} prefijo -si se desean obtener las propiedasdes con un prefijo
 */
function ObtenerFiltrosPagos(prefijo) {
    var parametros = {}
    parametros[prefijo ? 'pagina' + prefijo : 'pagina'] = ObtenerNumero(ObtenerValor('paginaPag')) ? ObtenerNumero(ObtenerValor('paginaPag')) : 1;
    parametros[prefijo ? 'paginarPor' + prefijo : 'paginarPor'] = ObtenerNumero(ObtenerValor('paginarPorPag')) ? ObtenerNumero(ObtenerValor('paginarPorPag')) : 5;
    if (Validar(ObtenerValor('compradorPag'))) {
        parametros[prefijo ? 'comprador' + prefijo : 'comprador'] = decodeURIComponent(ObtenerValor('compradorPag'));
    }
    if (Validar(ObtenerValor('proveedorPag'))) {
        parametros[prefijo ? 'proveedor' + prefijo : 'proveedor'] = decodeURIComponent(ObtenerValor('proveedorPag'));
    }
    if (Validar(ObtenerValor('tituloPag'))) {
        parametros[prefijo ? 'titulo' + prefijo : 'titulo'] = decodeURIComponent(ObtenerValor('tituloPag'));
    }
    if (Validar(ObtenerValor('montoPag'))) {
        parametros[prefijo ? 'monto' + prefijo : 'monto'] = decodeURIComponent(ObtenerValor('montoPag'));
    }
    if (Validar(ObtenerValor('pagosPag'))) {
        parametros[prefijo ? 'pagos' + prefijo : 'pagos'] = decodeURIComponent(ObtenerValor('pagosPag'));
    }
    if (Validar(ObtenerValor('fechaPag'))) {
        parametros[prefijo ? 'fecha' + prefijo : 'fecha'] = decodeURIComponent(ObtenerValor('fechaPag'));
    }
    if (Validar(ObtenerValor('dependencias'))) {
        parametros['dependencias'] = decodeURIComponent(ObtenerValor('dependencias'));
    }
    if (Validar(ObtenerValor('ordenarPorPag'))) {
        parametros[prefijo ? 'ordenarPor' + prefijo : 'ordenarPor'] = prefijo ? decodeURIComponent(ObtenerValor('ordenarPorPag')) : ObtenerOrdenConversion(decodeURIComponent(ObtenerValor('ordenarPorPag')));
    }
    return parametros;
}

/**
 * Obtiene un json con los filtros aplicados de procesos
 * @param {string} prefijo -si se desean obtener las propiedasdes con un prefijo
 */
function ObtenerFiltrosProcesos(prefijo) {
    var parametros = {}
    parametros[prefijo ? 'pagina' + prefijo : 'pagina'] = ObtenerNumero(ObtenerValor('paginaPro')) ? ObtenerNumero(ObtenerValor('paginaPro')) : 1;
    parametros[prefijo ? 'paginarPor' + prefijo : 'paginarPor'] = ObtenerNumero(ObtenerValor('paginarPorPro')) ? ObtenerNumero(ObtenerValor('paginarPorPro')) : 5;
    if (Validar(ObtenerValor('compradorPro'))) {
        parametros[prefijo ? 'comprador' + prefijo : 'comprador'] = decodeURIComponent(ObtenerValor('compradorPro'));
    }
    if (Validar(ObtenerValor('ocidPro'))) {
        parametros[prefijo ? 'ocid' + prefijo : 'ocid'] = decodeURIComponent(ObtenerValor('ocidPro'));
    }
    if (Validar(ObtenerValor('tituloPro'))) {
        parametros[prefijo ? 'titulo' + prefijo : 'titulo'] = decodeURIComponent(ObtenerValor('tituloPro'));
    }
    if (Validar(ObtenerValor('categoriaCompraPro'))) {
        parametros[prefijo ? 'categoriaCompra' + prefijo : 'categoriaCompra'] = decodeURIComponent(ObtenerValor('categoriaCompraPro'));
    }
    if (Validar(ObtenerValor('estadoPro'))) {
        parametros[prefijo ? 'estado' + prefijo : 'estado'] = decodeURIComponent(ObtenerValor('estadoPro'));
    }
    if (Validar(ObtenerValor('montoContratadoPro'))) {
        parametros[prefijo ? 'montoContratado' + prefijo : 'montoContratado'] = decodeURIComponent(ObtenerValor('montoContratadoPro'));
    }
    if (Validar(ObtenerValor('fechaInicioPro'))) {
        parametros[prefijo ? 'fechaInicio' + prefijo : 'fechaInicio'] = decodeURIComponent(ObtenerValor('fechaInicioPro'));
    }
    if (Validar(ObtenerValor('fechaRecepcionPro'))) {
        parametros[prefijo ? 'fechaRecepcion' + prefijo : 'fechaRecepcion'] = decodeURIComponent(ObtenerValor('fechaRecepcionPro'));
    }
    if (Validar(ObtenerValor('fechaPublicacionPro'))) {
        parametros[prefijo ? 'fechaPublicacion' + prefijo : 'fechaPublicacion'] = decodeURIComponent(ObtenerValor('fechaPublicacionPro'));
    }
    if (Validar(ObtenerValor('dependencias'))) {
        parametros['dependencias'] = decodeURIComponent(ObtenerValor('dependencias'));
    }
    if (Validar(ObtenerValor('ordenarPorPro'))) {
        parametros[prefijo ? 'ordenarPor' + prefijo : 'ordenarPor'] = prefijo ? decodeURIComponent(ObtenerValor('ordenarPorPro')) : ObtenerOrdenConversion(decodeURIComponent(ObtenerValor('ordenarPorPro')));
    }
    return parametros;
}


/**
 * Agrega un filtrado de orden de contratos
 * @param {string} filtro -filtro que se desea aplicar
 * @param {string} orden -tipo de orden
 */
function OrdenFiltroContratos(filtro, orden) {
    switch (orden) {
        case 'ascendente':
            PushDireccionContratos(AccederUrlPagina({ paginaCon: 1, ordenarPorCon: 'asc(' + filtro + ')' }));
            break;
        case 'descendente':
            PushDireccionContratos(AccederUrlPagina({ paginaCon: 1, ordenarPorCon: 'desc(' + filtro + ')' }));
            break;
        case 'neutro':
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorCon']) {
                delete filtros['ordenarPorCon'];
            }
            filtros['paginaCon'] = 1;
            PushDireccionContratos(AccederUrlPagina(filtros, true));
            break;
        default:
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorCon']) {
                delete filtros['ordenarPorCon'];
            }
            filtros['paginaCon'] = 1;
            PushDireccionContratos(AccederUrlPagina(filtros, true));
            break;

    }
}

/**
 * Agrega un filtrado de orden de pagos
 * @param {string} filtro -filtro que se desea aplicar
 * @param {string} orden -tipo de orden
 */
function OrdenFiltroPagos(filtro, orden) {
    switch (orden) {
        case 'ascendente':
            PushDireccionPagos(AccederUrlPagina({ paginaPag: 1, ordenarPorPag: 'asc(' + filtro + ')' }));
            break;
        case 'descendente':
            PushDireccionPagos(AccederUrlPagina({ paginaPag: 1, ordenarPorPag: 'desc(' + filtro + ')' }));
            break;
        case 'neutro':
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorPag']) {
                delete filtros['ordenarPorPag'];
            }
            filtros['paginaPag'] = 1;
            PushDireccionPagos(AccederUrlPagina(filtros, true));
            break;
        default:
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorPag']) {
                delete filtros['ordenarPorPag'];
            }
            filtros['paginaPag'] = 1;
            PushDireccionPagos(AccederUrlPagina(filtros, true));
            break;

    }
}

/**
 * Agrega un filtrado de orden de procesos
 * @param {string} filtro -filtro que se desea aplicar
 * @param {string} orden -tipo de orden
 */
function OrdenFiltroProcesos(filtro, orden) {
    switch (orden) {
        case 'ascendente':
            PushDireccionProcesos(AccederUrlPagina({ paginaPro: 1, ordenarPorPro: 'asc(' + filtro + ')' }));
            break;
        case 'descendente':
            PushDireccionProcesos(AccederUrlPagina({ paginaPro: 1, ordenarPorPro: 'desc(' + filtro + ')' }));
            break;
        case 'neutro':
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorPro']) {
                delete filtros['ordenarPorPro'];
            }
            filtros['paginaPro'] = 1;
            PushDireccionProcesos(AccederUrlPagina(filtros, true));
            break;
        default:
            var filtros = ObtenerFiltrosGenerales();
            if (filtros['ordenarPorPro']) {
                delete filtros['ordenarPorPro'];
            }
            filtros['paginaPro'] = 1;
            PushDireccionProcesos(AccederUrlPagina(filtros, true));
            break;

    }
}

/**
 * Inicializa las Descargas de las tablas
 */
function InicializarDescargas(){

    AbrirModalDescarga('descargaJsonCompradorProcesos','Descarga JSON',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaCsvCompradorProcesos','Descarga CSV',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaXlsxCompradorProcesos','Descarga XLSX',true);/*Crear Modal Descarga */
    $('#descargaJSONProcesos').on('click',function(e){
      AbrirModalDescarga('descargaJsonCompradorProcesos','Descarga JSON');
    });
    $('#descargaCSVProcesos').on('click',function(e){
      AbrirModalDescarga('descargaCsvCompradorProcesos','Descarga CSV');
    });
    $('#descargaXLSXProcesos').on('click',function(e){
      AbrirModalDescarga('descargaXlsxCompradorProcesos','Descarga XLSX');
    });
    AbrirModalDescarga('descargaJsonCompradorContratos','Descarga JSON',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaCsvCompradorContratos','Descarga CSV',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaXlsxCompradorContratos','Descarga XLSX',true);/*Crear Modal Descarga */
    $('#descargaJSONContratos').on('click',function(e){
      AbrirModalDescarga('descargaJsonCompradorContratos','Descarga JSON');
    });
    $('#descargaCSVContratos').on('click',function(e){
      AbrirModalDescarga('descargaCsvCompradorContratos','Descarga CSV');
    });
    $('#descargaXLSXContratos').on('click',function(e){
      AbrirModalDescarga('descargaXlsxCompradorContratos','Descarga XLSX');
    });
    AbrirModalDescarga('descargaJsonCompradorPagos','Descarga JSON',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaCsvCompradorPagos','Descarga CSV',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaXlsxCompradorPagos','Descarga XLSX',true);/*Crear Modal Descarga */
    $('#descargaJSONPagos').on('click',function(e){
      AbrirModalDescarga('descargaJsonCompradorPagos','Descarga JSON');
    });
    $('#descargaCSVPagos').on('click',function(e){
      AbrirModalDescarga('descargaCsvCompradorPagos','Descarga CSV');
    });
    $('#descargaXLSXPagos').on('click',function(e){
      AbrirModalDescarga('descargaXlsxCompradorPagos','Descarga XLSX');
    });
  }


  /**
   * Agrega la funcion de descarga de los procesos
   * @param {Object} datos -datos de los procesos
   */
  function ObtenerDescargaCompradorProcesos(datos){
    /*var parametros = ObtenerFiltrosProcesos();
    parametros['pagina']=1;
    parametros['paginarPor']=resultados.paginador['total.items']?resultados.paginador['total.items']:5;
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/procesos',parametros).done(function( datos ) {*/
      console.dir('Descargas Comprador Procesos')
      console.dir(datos);
      AgregarEventoModalDescarga('descargaJsonCompradorProcesos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source.doc.compiledRelease;
        });
        DescargarJSON(descarga,'Comprador Procesos');
      });
      AgregarEventoModalDescarga('descargaCsvCompradorProcesos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source.doc.compiledRelease;
        });
        DescargarCSV(ObtenerMatrizObjeto(descarga) ,'Comprador Procesos');
      });
      AgregarEventoModalDescarga('descargaXlsxCompradorProcesos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source.doc.compiledRelease;
        });
        DescargarXLSX(ObtenerMatrizObjeto(descarga) ,'Comprador Procesos');
      });
    
      /*
    }).fail(function() {
        console.dir('error de api descargas');
        
      });*/
    
  }

 /**
   * Agrega la funcion de descarga de los contratos
   * @param {Object} datos -datos de los conratos
   */
  function ObtenerDescargaCompradorContratos(datos){
      /*
    var parametros = ObtenerFiltrosContratos();
    parametros['pagina']=1;
    parametros['paginarPor']=resultados.paginador['total.items']?resultados.paginador['total.items']:5;
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/contratos',parametros).done(function( datos ) {*/
      console.dir('Descargas Comprador Contratos')
      console.dir(datos);
      AgregarEventoModalDescarga('descargaJsonCompradorContratos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarJSON(descarga,'Comprador Contratos');
      });
      AgregarEventoModalDescarga('descargaCsvCompradorContratos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarCSV(ObtenerMatrizObjeto(descarga) ,'Comprador Contratos');
      });
      AgregarEventoModalDescarga('descargaXlsxCompradorContratos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarXLSX(ObtenerMatrizObjeto(descarga) ,'Comprador Contratos');
      });
    
      /*
    }).fail(function() {
        console.dir('error de api descargas');
        
      });*/
  }

 /**
   * Agrega la funcion de descarga de los pagos
   * @param {Object} datos -datos de los pagos
   */
  function ObtenerDescargaCompradorPagos(datos){
    /*var parametros = ObtenerFiltrosPagos();
    parametros['pagina']=1;
    parametros['paginarPor']=resultados.paginador['total.items']?resultados.paginador['total.items']:5;
    parametros['tid']='id';
    $.get(api + "/compradores/" + encodeURIComponent(compradorId) + '/pagos',parametros).done(function( datos ) {*/
      console.dir('Descargas Comprador Pagos')
      console.dir(datos);
      AgregarEventoModalDescarga('descargaJsonCompradorPagos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarJSON(descarga,'Comprador Pagos');
      });
      AgregarEventoModalDescarga('descargaCsvCompradorPagos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarCSV(ObtenerMatrizObjeto(descarga) ,'Comprador Pagos');
      });
      AgregarEventoModalDescarga('descargaXlsxCompradorPagos',function(){
        var descarga=datos.resultados.map(function(e){
          return e._source;
        });
        DescargarXLSX(ObtenerMatrizObjeto(descarga) ,'Comprador Pagos');
      });
    
      /*
    }).fail(function() {
        console.dir('error de api descargas');
        
      });*/
  }