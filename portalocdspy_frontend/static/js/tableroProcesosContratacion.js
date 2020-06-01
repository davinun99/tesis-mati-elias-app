/**
 * @file tableroProcesosContratacion.js Este archivo se incluye en la sección de Tablero de Procesos de Contratación del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**
 * Variable de filtros aplicables con su  titulo y parametro
 * @type {Object} 
 */
var filtrosAplicables={
    monedas: {titulo:'Moneda',parametro:'moneda'},
    instituciones: {titulo:'Institución Compradora',parametro:'idinstitucion'},
    años: {titulo:'Año',parametro:'año'},
    proveedores: {titulo:'Proveedor',parametro:'proveedor'},
    categorias: {titulo:/*'Categoría de Compra'*/'Tipo de Contrato',parametro:'categoria'},
    modalidades : {titulo:'Modalidad de Compra',parametro:'modalidad'},
    sistemas :{titulo:'Sistema de Origen', parametro: 'sistema'}
    
  };

/**
 * Variable de filtros aplicables con su  titulo y parametro con su parametro como llave
 * @type {Object} 
 */
  var filtrosAplicablesR={
    moneda: {titulo:'Moneda',parametro:'monedas'},
    idinstitucion: {titulo:'Institución Compradora',parametro:'instituciones'},
    año: {titulo:'Año',parametro:'años'},
    proveedor: {titulo:'Proveedor',parametro:'proveedores'},
    modalidad: {titulo:'Modalidad de Compra',parametro:'modalidades'},
    categoria : {titulo:/*'Categoría de Compra'*/'Tipo de Contrato',parametro:'categorias'},
    sistema: {titulo:'Sistema de Origen', parametro:'sistemas'}
    
  };

/**
 * Arreglo para definir el orden en el que se presentan los filtros
 * @type {string[]} 
 */
  var ordenFiltros=['años','monedas','instituciones','categorias','modalidades','sistemas'];

  /**
 * Objeto para obtener traducciones e informacion de algunos códigos el OCDS
 * @type {Object} 
 */
  var traducciones={
    'goods':{titulo: 'Suministro de Bienes y/o Servicios'/*'Bienes y provisiones'*/,descripcion:'El proceso de contrataciones involucra bienes o suministros físicos o electrónicos.'},
    'works':{titulo:'Obras',descripcion:'El proceso de contratación involucra construcción reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
    'services':{titulo: 'Servicios'/*'Servicios'*/,descripcion:'El proceso de contratación involucra servicios profesionales de algún tipo, generalmente contratado con base de resultados medibles y entregables. Cuando el código de consultingServices está disponible o es usado por datos en algún conjunto da datos en particular, el código de servicio sólo debe usarse para servicios no de consultoría.'},
    'consultingServices':{titulo:'Consultorías',descripcion:'Este proceso de contratación involucra servicios profesionales provistos como una consultoría.'},
    'tender':{titulo:/*'Licitación' */'Desde Elaboración hasta Evaluación',descripcion:'Provee información sobre una nueva licitación (llamado a propuestas). La entrega de licitación debe contener detalles de los bienes o servicios que se buscan.'},
    'awards':{titulo:'Adjudicado'/*'Licitación'*/ ,descripcion:'Da información sobre la adjudicación de un contrato. Estarán presentes una o más secciones de adjudicación, y la sección de licitación puede estar poblada con detalles del proceso que llevó a la adjudicación.'},
    'contracts':{titulo:'Contrato',descripcion:'Da información sobre los detalles de un contrato que ha entrado, o entrará, en vigencia. La sección de licitación puede ser poblada con detalles del proceso que lleva al contrato, y la sección de adjudicación puede tener detalles sobre la adjudicación sobre la '},
    'planning':{
        titulo:'Planeación',
        descripcion:'Se propone o planea un proceso de contratación. La información en la sección de licitación describe el proceso propuesto. El campo tender.status debe de usarse para identificar si la planeación está en una etapa temprana o si hay planes detallados para una licitación.'
    },
    'goodsOrServices':{titulo:'Bienes y/o Servicios',descripcion:''}

  }
  window.onpopstate = function(e){
    location.reload();
  }
/**
 * Obtiene los filtros e inicializa los graficos
 */
  $(function(){
    $('.botonAzulFiltroBusqueda,.cerrarContenedorFiltrosBusqueda').on('click',function(e){
        $('.contenedorFiltrosBusqueda').toggle('slide');
     
      });
      $( window ).resize(function() {
       if($(window).width()>767){
        $('.contenedorFiltrosBusqueda').show();
       }
      });
    PanelInicialFiltros('#elastic-list');
    ObtenerFiltros();
    
    CargarGraficos();
    $('#quitarFiltros, #quitarFiltros2').on('click',function(e){
        PushDireccionGraficos(AccederUrlPagina({},true));
      });

});

/**
 * Ejecuta la carga de todos los gráficos
 */
function CargarGraficos(){
    $('.contenedorGrafico > .grafico').each(function(i,elemento){
        if(echarts.getInstanceByDom(elemento)){
            echarts.getInstanceByDom(elemento).clear();
        }
    });
    
    InicializarCantidadProcesos();
    InicializarMontoProcesos();
    CargarCajonesCantidadProcesos();
    CargarCajonesCantidadContratos();
    CargarCajonesMontoContratos();
    CantidadProcesosTipoContrato();
    CantidadProcesosModalidadContratacion();
    MontoProcesosTipoContrato();
    MontoContratosModalidadContratacion();
    TiempoPromedioEtapas();
    CantidadProcesosEtapas();
    Top10Proveedores();
    Top10Compradores();
    InicializarConteo();
    
}
/**
 * Obtiene los datos de los filtros a mostrar
 */
function ObtenerFiltros(){
    var parametros=ObtenerJsonFiltrosAplicados({});
    $.get(api+"/dashboardoncae/filtros/",parametros).done(function( datos ) {

    MostrarListaElastica(datos,'#elastic-list');
    MostrarEtiquetaListaElasticaAplicada();
    MostrarListaElasticaAplicados();
      
        
      }).fail(function() {
          
          
        });

}



/**
 * Obtiene un Json con los filtros aplicados según la url, recibe un json inicial por si se quiere setear el valor de algun grafico que no este todavía en la url
 */
function ObtenerJsonFiltrosAplicados(parametros){
    if(Validar(ObtenerValor('moneda'))){
        parametros['moneda']=decodeURIComponent(ObtenerValor('moneda'));
    }
    if(Validar(ObtenerValor('idinstitucion'))){
    parametros['idinstitucion']=decodeURIComponent(ObtenerValor('idinstitucion'));
    }
    if(Validar(ObtenerValor('año'))){
      parametros['año']=decodeURIComponent(ObtenerValor('año'));
    }
    if(Validar(ObtenerValor('proveedor'))){
        parametros['proveedor']=decodeURIComponent(ObtenerValor('proveedor'));
    }
    if(Validar(ObtenerValor('categoria'))){
      parametros['categoria']=decodeURIComponent(ObtenerValor('categoria'));
    }
    if(Validar(ObtenerValor('modalidad'))){
        parametros['modalidad']=decodeURIComponent(ObtenerValor('modalidad'));
    }
    if(Validar(ObtenerValor('sistema'))){
        parametros['sistema']=decodeURIComponent(ObtenerValor('sistema'));
    }
    if(Validar(ObtenerValor('masinstituciones'))){
        parametros['masinstituciones']=decodeURIComponent(ObtenerValor('masinstituciones'));
    }
    if(Validar(ObtenerValor('masproveedores'))){
        parametros['masproveedores']=decodeURIComponent(ObtenerValor('masproveedores'));
    }
    

    return parametros;
  }

/**
 * Obtiene los datos de los filtros a mostrar
 * @param {Object} opciones - objeto de filtros aplicados
 * @param {boolean} desUrl - descartar parametro de la url en caso de que este y no se haya recibido un json de opciones
 * @return {string}
 */
  function AccederUrlPagina(opciones,desUrl){
    var direccion=('/tableroProcesosContratacion/?'+
    (ValidarCadena(opciones.moneda)? '&moneda='+encodeURIComponent(opciones.moneda): (ValidarCadena(ObtenerValor('moneda'))&&!desUrl?'&moneda='+ObtenerValor('moneda'):''))+
    (ValidarCadena(opciones.idinstitucion)? '&idinstitucion='+encodeURIComponent(opciones.idinstitucion): (ValidarCadena(ObtenerValor('idinstitucion'))&&!desUrl?'&idinstitucion='+ObtenerValor('idinstitucion'):''))+
   
    (ValidarCadena(opciones.año)? '&año='+encodeURIComponent(opciones.año): (ValidarCadena(ObtenerValor('año'))&&!desUrl?'&año='+ObtenerValor('año'):''))+
    (ValidarCadena(opciones.proveedor)? '&proveedor='+encodeURIComponent(opciones.proveedor): (ValidarCadena(ObtenerValor('proveedor'))&&!desUrl?'&proveedor='+ObtenerValor('proveedor'):''))+
    (ValidarCadena(opciones.categoria)? '&categoria='+encodeURIComponent(opciones.categoria): (ValidarCadena(ObtenerValor('categoria'))&&!desUrl?'&categoria='+ObtenerValor('categoria'):''))+
    (ValidarCadena(opciones.modalidad) ? '&modalidad='+encodeURIComponent(opciones.modalidad):(ValidarCadena(ObtenerValor('modalidad'))&&!desUrl?'&modalidad='+ObtenerValor('modalidad'):''))+
    (ValidarCadena(opciones.sistema) ? '&sistema='+encodeURIComponent(opciones.sistema):(ValidarCadena(ObtenerValor('sistema'))&&!desUrl?'&sistema='+ObtenerValor('sistema'):''))+
    (ValidarCadena(opciones.masproveedores) ? '&masproveedores='+encodeURIComponent(opciones.masproveedores):(ValidarCadena(ObtenerValor('masproveedores'))&&!desUrl?'&masproveedores='+ObtenerValor('masproveedores'):''))+
    (ValidarCadena(opciones.masinstituciones) ? '&masinstituciones='+encodeURIComponent(opciones.masinstituciones):(ValidarCadena(ObtenerValor('masinstituciones'))&&!desUrl?'&masinstituciones='+ObtenerValor('masinstituciones'):''))
  
    );
    return direccion;
  }
/**
 * Agrega la dirección al historial de búsqueda del navegador
 */
  function PushDireccionGraficos(direccion){
    window.history.pushState({}, document.title,direccion);
    ObtenerFiltros();
    CargarGraficos();
  }
  
/**
  * Muestra las etiquetas con los filtros aplicados
  * @param {Object} parametros 
  */
  function MostrarEtiquetasFiltrosAplicados(parametros){
    delete parametros.masinstituciones;
    delete parametros.masproveedores;
    if(!$.isEmptyObject(parametros)){
      $('#contenedorSinFiltros').hide();
      $('#contenedorFiltros').show();
    }else{
      $('#contenedorFiltros').hide();
      $('#contenedorSinFiltros').show();
    }
    $('#listaFiltrosAplicados,#extencionFiltrosAplicados').html('');
    $.each(parametros,function(llave,filtro){
      $('#listaFiltrosAplicados,#extencionFiltrosAplicados').append(
        $('<div>',{class:'grupoEtiquetaFiltro col-md-12x mb-1x',style:'display:inline-block'}).append(
          $('<div>',{class:'grupoEtiquetaTitulo mr-1',text:filtrosAplicablesR[llave].titulo +':'}),
          $('<div>',{class:'filtrosAplicados'}).append(
            $('<div>',{class:'etiquetaFiltro','llave':llave,'valor':filtro}).append(
                (traducciones[filtro]?traducciones[filtro].titulo:filtro),
              '&nbsp;',
              $('<i>',{class:'fas fa-times'}).on('click',function(e){
                var filtros=ObtenerJsonFiltrosAplicados({});
                delete filtros[filtrosAplicablesR[$(e.currentTarget).parent().attr('llave')]?$(e.currentTarget).parent().attr('llave'):''];
                
                PushDireccionGraficos(AccederUrlPagina(filtros,true));
                $('.etiquetaFiltro[llave="'+$(e.currentTarget).parent().attr('llave')+'"]').parent().prev().remove();
                $('.etiquetaFiltro[llave="'+$(e.currentTarget).parent().attr('llave')+'"]').parent().remove();
              })
            )
          )
        )
      )
    });
    $('.filtrosContenedoFiltrosBusqueda').attr('style','height:calc(100vh - '+($('#extencionFiltrosAplicados').height()?123:110)+'px - '+($('#extencionFiltrosAplicados').height() + ($('#extencionFiltrosAplicados').height()?4:0))+'px)')

  }

/**
   * Envia un Json cono los parametros de los filtros aplicados a la función que muestra las etiquetas de filtros aplicados
   */
  function MostrarEtiquetaListaElasticaAplicada(){
  
    var parametros={
    };
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarEtiquetasFiltrosAplicados(parametros);
  }

  /**
   * Muestra la Seleccion aplicada en la lista de filtros.
   */
  function MostrarListaElasticaAplicados(){
    var filtros={
    };
    filtros=ObtenerJsonFiltrosAplicados(filtros);
    $.each(filtros,function(llave,valor){
      $('ul#ul'+ filtrosAplicablesR[llave].parametro ).find(
        'li[valor="'+(valor).toString()+'"]'
      ).addClass('active');
    });
  }

  /**
   * Muestra el contenido de los filtros aplicados, las opciones de selección
   */
  function MostrarListaElastica(datos,selector){
    $(selector).html('');
    $.each(datos.respuesta,function(llave,valor){
      $(selector).append(
        $('<div class="list-container col-md-12 2">').append(
          $('<div class="panel panel-default ">').append(
            $('<div class="panel-heading">').text(
              filtrosAplicables[llave]?filtrosAplicables[llave].titulo:llave
            ),
            $('<input>',{type:'text', class:'elastic-filter',placeholder:filtrosAplicables[llave]?filtrosAplicables[llave].titulo:llave ,filtro:llave,on:{
              keyup:function(e){
                var texto=$(e.currentTarget).val();
                if (texto.length > 0) {
                  texto = texto.toLocaleLowerCase();
                  var regla = " ul#" + 'ul'+llave + ' li[formato*="' + texto + '"]{display:block;} ';
                  regla += " ul#" + 'ul'+llave + ' li:not([formato*="' + texto + '"]){display:none;}';
                  $('#style'+llave).html(regla);
                } else {
                  $('#style'+llave).html('');
                }
              }
            }}),
            $('<style>',{id:'style'+llave}),
            $('<ul >',{class:'list-group',id:'ul'+llave}).append(
              AgregarPropiedadesListaElastica(valor,llave)
            ),
            ['instituciones','proveedores'].includes(llave)&&valor&&valor.length>=50?
              $('<a>',{
                class:'enlaceTablaGeneral ptextoColorPrimario pcursorMano',
                href:'javascript:void(0)',
                style:'width:150px;padding:5px 15px',
                text: valor.length==50? 'Mostrar Todos...':'Mostrar Menos...',
                toolTexto:valor.length==50?'Mostrar más resultados':'Mostrar menos resultados',
                toolCursor:'true',
                llave:llave,
                on:{
                  click:MostrarMasResultados
                }
              })
            :null
              
            
          )
        )
      )
      
      
    });
    AgregarToolTips();
    
    
  }
/**
 * Ejecuta la acción al ahcer click en eun botón
 * @param {Object} e - evento de un click
 */
  function MostrarMasResultados(e){
    switch($(e.currentTarget).attr('llave')){
        case 'instituciones':
                var filtros=ObtenerJsonFiltrosAplicados({});
                if(filtros.masinstituciones){
                    delete filtros.masinstituciones;
                }else{
                    filtros['masinstituciones']=1;
                }
                PushDireccionGraficos(AccederUrlPagina(filtros,true));

            break;
        case 'proveedores':
                var filtros=ObtenerJsonFiltrosAplicados({});
                if(filtros.masproveedores){
                    delete filtros.masproveedores;
                }else{
                    filtros['masproveedores']=1;
                }
                PushDireccionGraficos(AccederUrlPagina(filtros,true));
            break;
        default:
            break;
    }
}

/**
 * Devuelve información de un filtro como ser que valor usar para mostrar la cantidad en el badge y que valor usar a la hora de que sea seleccionado
 * @param {string} llave - llave de un filtro 
 */
function ValoresLlaves(llave){
    switch(llave){
        case 'años':
            return {valor:'key_as_string',cantidad:'procesos',codigo:'key_as_string'};
        case 'categorias':
            return {valor:'categoria',cantidad:'procesos',codigo:'categoria'};
        case 'instituciones':
            return {valor:'nombre',cantidad:'procesos',codigo:'codigo'};
        case 'modalidades':
            return {valor:'modalidad',cantidad:'procesos',codigo:'modalidad'};
        case 'monedas':
            return {valor:'moneda',cantidad:'procesos',codigo:'moneda'};
        case 'sistemas':
        return {valor:'id',cantidad:'ocids',codigo:'id'};
        default:
            return {valor:'key_as_string',cantidad:'procesos',codigo:'key_as_string'};
    }
}

/**
 * Agrega los elementos que pueden ser seleccionados en un categoría
 * @param {Object[]} valor - arreglo de valores seleccionables en un filtro
 * @param {string} llave - llave de un filtro 
 */
function AgregarPropiedadesListaElastica(valor,llave){
    var elementos=[]
    $.each(valor,function(i,propiedades){
       elementos.push(
        $('<li >',{
        class:'list-group-item',
        valor:propiedades[ValoresLlaves(llave).codigo]?propiedades[ValoresLlaves(llave).codigo]:propiedades.key, 
        formato: (ObtenerTexto(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ValoresLlaves(llave).valor])).toString().toLowerCase(),'llave':llave,
        
        on:{
          click:function(e){
            var filtro=$(e.currentTarget);
            if(filtro.hasClass('active')){
              filtro.removeClass('active')
            }else{
              filtro.parent().find('.list-group-item.active').removeClass('active');
              filtro.addClass('active');
            }
            /*var filtros={
            };
            $('li.list-group-item.active').each(function(cla,val){
              filtros[filtrosAplicables[$(val).attr('llave')]?filtrosAplicables[$(val).attr('llave')].parametro:'' ]=$(val).attr('valor');
            });*/
            $('li.list-group-item').not('.active').remove();
            $( '.list-group' ).not(':has(li)').append(
                $('<li >',{
                    class:'list-group-item animated fadeIn noEncima'
                }
                ).append(
                    $('<div>',{class:'badge',style:'background:transparent'}).append($('<img>',{src:'/static/img/otros/loaderFiltros.svg',style:'height:20px'})),
                    $('<div>',{
                    class:'elastic-data cargandoElementoLista',
                    text:'Cargando'}
                    )
                  )
            );
            var filtros=ObtenerJsonFiltrosAplicados({});
            if(filtro.hasClass('active')){
                filtros[filtrosAplicables[$(e.currentTarget).attr('llave')]?filtrosAplicables[$(e.currentTarget).attr('llave')].parametro:'']=$(e.currentTarget).attr('valor');
              }else{
                delete filtros[filtrosAplicables[$(e.currentTarget).attr('llave')]?filtrosAplicables[$(e.currentTarget).attr('llave')].parametro:''];
              }
            PushDireccionGraficos(AccederUrlPagina(filtros,true));
          }
        }}).append(
          $('<div>',{
              class:'badge',
              toolTexto: (propiedades.procesos||propiedades.contratos)?('Procesos: '+ValorNumerico(propiedades.procesos)+'<br>Contratos: '+ValorNumerico(propiedades.contratos)):'OCID',
              text:(ValoresLlaves(llave).cantidad)=='procesos'?ValorNumerico(propiedades[ValoresLlaves(llave).cantidad]===0?propiedades['contratos']:propiedades[ValoresLlaves(llave).cantidad]):ValorNumerico(propiedades[ValoresLlaves(llave).cantidad]) 
            }),
          $('<div >',{
          class:'elastic-data',
          toolTexto:(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ValoresLlaves(llave).valor]),
          toolCursor:'true',
          
          text:(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ValoresLlaves(llave).valor])}
          )
        )
      )
    });
    return elementos;
  }

/**
 * Agrega los elementos que pueden ser seleccionados en un categoría
 * @param {Object[]} valor - arreglo de valores seleccionables en un filtro
 * @param {string} llave - llave de un filtro 
 */
function PanelInicialFiltros(selector){
    $(selector).html('')
  $.each(ordenFiltros,function(indice,elemento){

      $(selector).append(
        $('<div class="list-container col-md-12 2 animated fadeIn">').append(
          $('<div class="panel panel-default ">').append(
            $('<div class="panel-heading">').text(
              filtrosAplicables[elemento]?filtrosAplicables[elemento].titulo:elemento
            ),
            $('<input>',{type:'text', class:'elastic-filter',placeholder:filtrosAplicables[elemento]?filtrosAplicables[elemento].titulo:elemento ,filtro:elemento/*,on:{
              keyup:function(e){
                var texto=$(e.currentTarget).val();
                if (texto.length > 0) {
                  texto = texto.toLocaleLowerCase();
                  var regla = " ul#" + 'ul'+elemento.llave + ' li[formato*="' + texto + '"]{display:block;} ';
                  regla += " ul#" + 'ul'+elemento.llave + ' li:not([formato*="' + texto + '"]){display:none;}';
                  $('#style'+elemento.llave).html(regla);
                } else {
                  $('#style'+elemento.llave).html('');
                }
              }
            }*/}),
            $('<ul >',{class:'list-group',id:'ul'+elemento}).append(
              $('<li >',{
                  class:'list-group-item animated fadeIn noEncima'
              }
              ).append(
                  $('<div>',{class:'badge',style:'background:transparent'}).append($('<img>',{src:'/static/img/otros/loaderFiltros.svg',style:'height:20px'})),
                  $('<div>',{
                  class:'elastic-data cargandoElementoLista',
                  text:'Cargando'}
                  )
                )
            )
              
            
          )
        )
      );
      
      
    });
}

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad y Porcentaje de Procesos de Contratación Iniciados con Relación al Total por Mes
 */
function InicializarCantidadProcesos(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#cantidadProcesos',true);
    $.get(api+"/dashboardoncae/cantidaddeprocesos/",parametros).done(function( datos ) {
    OcultarReloj('#cantidadProcesos');
    if((datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadprocesos)  && datos.resultados.cantidadprocesos.length==0)||datos.resultados.cantidadprocesos.map(function(e){return ObtenerNumero(e);}).reduce(function(a, b){return a + b;}, 0)==0){
        MostrarSinDatos('#cantidadProcesos',true);
        return;
    }

    
    var grafico=echarts.init(document.getElementById('cantidadProcesos'));
    var opciones = {
        baseOption:{
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross',
                crossStyle: {
                    color: '#999'
                }
            },
            formatter:  function (e){
                var cadena=e[0].name+'<br>';

                e.forEach(function(valor,indice){
                    cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+valor.value +' '+(valor.seriesIndex==0?'Procesos':'%')+'<br>'
                });
                return cadena;
            }
        },
    legend: {
        plain: 'scroll',
        orient: 'horizontal',
        position:'bottom',
        bottom:0,
        right:'center'
    },
    toolbox: {
        orient:'horizontal',
        itemsize:20,
        itemGap:15,
        right:20,
        top:25,
        feature: {
            dataView: {show: true, readOnly: false,title:'Vista',lang: ['Vista de Datos', 'Cerrar', 'Actualizar'] },
            magicType: {show: true, type: ['line', 'bar'],title:{line:'Linea',bar:'Barra',stack:'Pila',tiled:'Teja'}},
            restore: {show: true,title:'Restaurar'},
            saveAsImage: {show: true,title:'Descargar'}
        },
        emphasis:{
            iconStyle:{
                textPosition:'top'
            }
        }
    },
        xAxis: [
            {
                type: 'category',
                data: datos.resultados.meses,
                axisPointer: {
                    type: 'shadow'
                },
                axisLabel:{
                    interval:0,
                    rotate:45,
                    showMinLabel:false
                },
                name:'Meses'
            }
        ],
        grid:{
            containLabel:true
        },
        yAxis: [
            {
                type: 'value',
                name: 'Cantidad',
                min: 0,
                axisLabel: {
                    formatter: '{value}'
                },
                position:'left',
                axisPointer: {
                    label: {
                        formatter: '{value} Procesos'
                    }
                }
            },
            {
                type: 'value',
                min: 0,
                max: 100,
                axisLabel: {
                    show:false
                },
                position:'right',
                axisTick : {show: false},
                axisLine:{show:false},
                splitLine:{show:false},
                axisPointer: {
                    label: {
                        formatter: '{value} %'
                    }
                }
            }
        ],
        series: [
            {
                name:'Cantidad de Procesos de Contratación',
                type:'bar',
                data:datos.resultados.cantidadprocesos,
                itemStyle:{
                    color: ObtenerColores('Pastel1')[0]
                },
                
                label:{
                    show:true,
                    formatter:function (e){
                        return ValorNumerico(e.value)  +''
                    }
                }
            },
            {
                name:'Porcentaje de la Cantidad de Procesos de Contratación en Relación a los Demás Meses',
                type:'line',
                data:datos.resultados.promedioprocesos.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
                symbol: 'circle',
                symbolSize: 10,
                lineStyle: {
                    normal: {
                        color: ObtenerColores('Pastel1')[9],
                        width: 4
                    }
                },
                itemStyle:{
                    color: ObtenerColores('Pastel1')[9]
                },
                yAxisIndex:1,
                
                label:{
                    show:true,
                    color:'grey',
                    formatter:function (e){
                        return ValorMoneda(e.value)  +' %'
                    }
                }
            }
        ],
        grid:{
            containLabel:true
        }},
        media: [ 
            {
                query: {
                    maxWidth: 700
                },   
                option: {       
                    series: [
                        {
                            name:'Cantidad de Procesos de Contratación',
                            type:'bar',
                            data:datos.resultados.cantidadprocesos,
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[0]
                            }
                        },
                        {
                            name:'Porcentaje de la Cantidad de Procesos de\nContratación en Relación a los Demás Meses',
                            type:'line',
                            data:datos.resultados.promedioprocesos.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
                            symbol: 'circle',
                            symbolSize: 10,
                            lineStyle: {
                                normal: {
                                    color: ObtenerColores('Pastel1')[9],
                                    width: 4
                                }
                            },
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[9]
                            },
                            yAxisIndex:1
                        }
                    ],
                    grid:{
                        right:0,
                        left:0
                    }
                }
            }
        ]
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
    }).fail(function() {
            
            
    });
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Monto de Contratos por Mes
 */
function InicializarMontoProcesos(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#montoProcesos',true);
    $.get(api+"/dashboardoncae/montosdecontratos/",parametros).done(function( datos ) {
       
        OcultarReloj('#montoProcesos');
        if((datos&&datos.resultados&&Array.isArray(datos.resultados.monto_contratos_mes)  && datos.resultados.monto_contratos_mes.length==0)||datos.resultados.monto_contratos_mes.map(function(e){return ObtenerNumero(e);}).reduce(function(a, b){return a + b;}, 0)==0){
            MostrarSinDatos('#montoProcesos',true);
            return;
        }
    var grafico=echarts.init(document.getElementById('montoProcesos'));
    var opciones = {
        baseOption:{
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross',
                    crossStyle: {
                        color: '#999'
                    }
                },
                formatter:  function (e){
                    var cadena=e[0].name+'<br>';

                e.forEach(function(valor,indice){
                    cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?valor.value:ValorMoneda(valor.value)) +' '+(valor.seriesIndex==0?'%':'HNL')+'<br>'
                });
                return cadena;
            }
            },
            legend: {
                plain: 'scroll',
                orient: 'horizontal',
                position:'bottom',
                bottom:0,
                right:'center'
            },
            grid:{
                containLabel:true
            },
            toolbox: {
                orient:'horizontal',
                itemsize:20,
                itemGap:15,
                right:20,
                top:25,
                feature: {
                    dataView: {show: true, readOnly: false,title:'Vista',lang: ['Vista de Datos', 'Cerrar', 'Actualizar'] },
                    magicType: {show: true, type: ['line', 'bar'],title:{line:'Linea',bar:'Barra',stack:'Pila',tiled:'Teja'}},
                    restore: {show: true,title:'Restaurar'},
                    saveAsImage: {show: true,title:'Descargar'}
                },
                emphasis:{
                    iconStyle:{
                        textPosition:'top'
                    }
                }
            },
            xAxis: [
                {
                    type: 'category',
                    data: datos.resultados.meses,
                    axisPointer: {
                        type: 'shadow'
                    },
                    axisLabel:{
                        interval:0,
                        rotate:45,
                        showMinLabel:false
                    },
                    name :'Meses'
                }
            ],
            yAxis: [
                
                {
                    type: 'value',
                    min: 0,
                    max: 100,
                    axisLabel: {
                        show:false
                    },
                    position:'right',
                    axisTick : {show: false},
                    axisLine:{show:false},
                    splitLine:{show:false},
                    axisPointer: {
                        label: {
                            formatter: '{value} %'
                        }
                    }
                },
                {
                    type: 'value',
                    name: 'Monto',
                    min: 0,
                    axisLabel: {
                        formatter: '{value} HNL'
                    },
                    name:'Lempiras',
                    axisPointer: {
                        label: {
                            formatter: '{value} HNL'
                        }
                    }
                }
            ],
            series: [
                
                {
                    name:'Porcentaje del Monto Contratado en Relación a los Demás Meses',
                    type:'line',
                    data:/*datos.resultados.monto_contratos_mes.map(function(e){return ObtenerNumero(((ObtenerNumero(e)/ObtenerNumero(datos.resultados.total_monto_contratos))*100).toFixed(2))}),*/datos.resultados.porcentaje_montos_mes.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
                    symbol: 'circle',
                    symbolSize: 10,
                    lineStyle: {
                        normal: {
                            color: ObtenerColores('Pastel1')[9],
                            width: 4/*,
                            type: 'dashed'*/
                        }
                    },
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[9]
                    }
                
                },{
                    name:'Monto de Procesos de Contratación',
                    type:'bar',
                    data:datos.resultados.monto_contratos_mes,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[1]
                    },
                    
                label:{
                    show:true,
                    color:'#c4c4c4',
                    formatter:function (e){
                        return ValorMoneda(e.value)  +' HNL'
                    },
                    fontFamily:'Poppins',
                    fontWeight:600,
                    fontSize:12,
                    rotate:90,
                    verticalAlign:'top',
                    position: ['50%', '0%'],
                    zIndex:3,
                    align:'right'
                },
                yAxisIndex:1
                }
            ],
            grid:{
                containLabel:true
            }
        },
        media:[{
            query:{
                maxWidth:600
            },
            option:{
                series: [
                    {
                        name:'Monto de Procesos de Contratación',
                        type:'bar',
                        data:datos.resultados.monto_contratos_mes,
                        itemStyle:{
                            color: ObtenerColores('Pastel1')[1]
                        },
                        yAxisIndex:1
                    },
                    {
                        name:'Porcentaje del Monto Contratado en \nRelación a los Demás Meses',
                        type:'line',
                        data:/*datos.resultados.monto_contratos_mes.map(function(e){return ObtenerNumero(((ObtenerNumero(e)/ObtenerNumero(datos.resultados.total_monto_contratos))*100).toFixed(2))}),*/datos.resultados.porcentaje_montos_mes.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
                        symbol: 'circle',
                        symbolSize: 10,
                        lineStyle: {
                            normal: {
                                color: ObtenerColores('Pastel1')[9],
                                width: 4/*,
                                type: 'dashed'*/
                            }
                        },
                        itemStyle:{
                            color: ObtenerColores('Pastel1')[9]
                        }
                    
                    }
                ]
            }
        }]
        
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
    }).fail(function() {
          
          
    });
    
}




/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Procesos de Contratación por Etapas
 */
function CantidadProcesosEtapas(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#CantidadProcesosEtapas',true)
    $.get(api+"/dashboardoncae/procesosporetapa/",parametros).done(function( datos ) {
        
    OcultarReloj('#CantidadProcesosEtapas');
    if(datos&&datos.resultados&&Array.isArray(datos.resultados.procesos)  && datos.resultados.procesos.length==0){
        MostrarSinDatos('#CantidadProcesosEtapas',true);
        return;
    }
        var grafico=echarts.init(document.getElementById('CantidadProcesosEtapas'));
        var opciones = {
            baseOption:{
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross',
                    crossStyle: {
                        color: '#999'
                    }
                }
            },
            toolbox: {
                orient:'horizontal',
                itemsize:20,
                itemGap:15,
                right:20,
                top:25,
                feature: {
                    dataView: {show: true, readOnly: false,title:'Vista',lang: ['Vista de Datos', 'Cerrar', 'Actualizar'] },
                    magicType: {show: true, type: ['line', 'bar'],title:{line:'Linea',bar:'Barra',stack:'Pila',tiled:'Teja'}},
                    restore: {show: true,title:'Restaurar'},
                    saveAsImage: {show: true,title:'Descargar'}
                },
                emphasis:{
                    iconStyle:{
                        textPosition:'top'
                    }
                }
            },
            xAxis: [
                {
                    type: 'category',
                    data: datos.resultados.etapas.map(function(e){return (traducciones[e]?traducciones[e].titulo:e);}),
                    axisPointer: {
                        type: 'shadow',
                        showMinLabel:true
                    },
                    name :'Etapas',
                    axisLabel:{
                        showMinLabel:true
                    }
                }
            ],
            yAxis: [
                {
                    type: 'value',
                    name: 'Cantidad',
                    min: 0,
                    axisLabel: {
                        formatter: '{value}'
                    }
                }
            ],
            series: [
                {
                    name:'Procesos',
                    type:'bar',
                    data:datos.resultados.procesos,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[3]
                    },
                    
                    label:{
                        show:true,
                        formatter:function (e){
                            return ValorNumerico(e.value)  
                        },
                        fontFamily:'Poppins',
                        fontWeight:700,
                        fontSize:25
                    }
                }
            ],
            grid:{
                containLabel:true
            }},
            media:[
                {
                    query:{maxWidth:600},

                    option:{
                    
                    tooltip: {
                        position:['0%','50%']
                    }
                    }
                }
            ]
        };
        grafico.setOption(opciones, true);
    
        
        window.addEventListener("resize", function(){
            grafico.resize();
        });
    }).fail(function() {
    
    });
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Tiempo Promedio por Etapas de Procesos de Contratación
 */
function TiempoPromedioEtapas(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#tiempoPromedioEtapas',true);
    $.get(api+"/dashboardoncae/tiemposporetapa/",parametros).done(function( datos ) {
    OcultarReloj('#tiempoPromedioEtapas');
    
    var resultados=[];
    if(datos&&datos.resultados){
        $.each(datos.resultados,function(tipocontrato,modalidad){
            $.each(modalidad,function(llave,valor){
                resultados.push({
                    'tipoContrato':tipocontrato,
                    'modalidad':llave,
                    promedioDiasIniciarContrato:  ObtenerNumero(valor.promedioDiasIniciarContrato),
                    promedioDiasLicitacion: ObtenerNumero(valor.promedioDiasLicitacion)
                });
            });
        });
    }
    var seriesGrafico=[];
    var ejeY=[];
    if(Array.isArray(resultados)  && resultados.length==0){
        MostrarSinDatos('#tiempoPromedioEtapas',true);
        return;
    }
    resultados.forEach(function(valor){
        ejeY.push(
            (traducciones[valor.tipoContrato]?traducciones[valor.tipoContrato].titulo:valor.tipoContrato) +' | '+valor.modalidad
        );
        seriesGrafico.push(
            {
                name:  (traducciones[valor.tipoContrato]?traducciones[valor.tipoContrato].titulo:valor.tipoContrato) +' | '+valor.modalidad,
                type: 'bar',
                stack: 'Tiempo',
                label: {
                    normal: {
                        show: true,
                        position: 'insideRight'
                    }
                },
                data: [Math.round( ObtenerNumero(valor.promedioDiasLicitacion)),Math.round( ObtenerNumero(valor.promedioDiasIniciarContrato))],
                itemStyle:{
                    color: ObtenerColores('Pastel1')[3]
                }

                
            }
        );
    });

    var grafico=echarts.init(document.getElementById('tiempoPromedioEtapas'));
    var opciones ={
        baseOption:{
            tooltip : {
                trigger: 'axis',
                axisPointer : {           
                    type : 'shadow'    
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            legend: {
            },
            xAxis:  {
                type: 'value',
                axisLabel: {
                    formatter: '{value} Días'
                }
            },
            yAxis: {
                type: 'category',
                data: ejeY,
                axisLabel: {
                    interval:0,
                    showMinLabel:false,
                    padding:[0,0,0,0],
                    formatter: function(e){
                        return '{valor|' + e.split(' | ')[0] + ' }\n{value|' + e.split(' | ')[1] + '}'
                        
                    },
                    rich: {
                        value: {
                        },
                        valor:{
                            fontWeight:'bold'
                        }
                    }
                }
            },
            series:[
                {
                    name:  'Periodo de invitación – recepción de ofertas',
                    type: 'bar',
                    stack: 'Tiempo',
                    label: {
                        normal: {
                            show: true,
                            position: 'insideLeft'
                            ,
                        formatter:function(e){
                            if(e.value==0){
                                return '';
                            }else{
                                return "{c} Días".replace('{c}',e.value);
                            }
                        }
                        }
                        
                    },
                    data: resultados.map(function(e){
                        return Math.round( ObtenerNumero(e.promedioDiasLicitacion));
                    }) ,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[4]
                    }
    
                    
                },
                {
                    name:  'Periodo de Evaluación – Adjudicación - Contratación',
                    type: 'bar',
                    stack: 'Tiempo',
                    label: {
                        normal: {
                            show: true,
                            position: 'insideLeft',
                            formatter:function(e){
                                if(e.value==0){
                                    return '';
                                }else{
                                    return "{c} Días".replace('{c}',e.value);
                                }
                            }
                        }
                    },
                    data: resultados.map(function(e){
                        return Math.round( ObtenerNumero(e.promedioDiasIniciarContrato));
                    }),
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[1]
                    }
    
                    
                }
                
                
            ],
            label:{
                show:true,
                fontFamily:'Poppins',
                fontWeight:600,
                fontSize:13,
                formatter: function (e){
                    return "{c} Días".replace('{c}',e.value);
                }
            }
        }
        
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
    }).fail(function() {
    
    });
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Procesos de Contratación por Tipo de Contrato
 */
function CantidadProcesosTipoContrato(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#CantidadProcesosTipoContrato',true);
$.get(api+"/dashboardoncae/procesosporcategoria/",parametros).done(function( datos ) {

    
OcultarReloj('#CantidadProcesosTipoContrato');
var datosPastel=[];
datos.resultados.categorias.forEach(function(valor,indice){
    datosPastel.push(
        {
            name:traducciones[valor]?traducciones[valor].titulo:valor,
            value:datos.resultados.procesos[indice]?datos.resultados.procesos[indice]:0
        }
    )
});
if(Array.isArray(datosPastel)  && datosPastel.length==0){
    MostrarSinDatos('#CantidadProcesosTipoContrato',true);
    return;
}
var grafico=echarts.init(document.getElementById('CantidadProcesosTipoContrato'));
    var opciones = {
        
        baseOption: {
            tooltip : {
                trigger: 'item',
                
                formatter:  function (valor){
    
                    var cadena=valor.name+'<br>';
                   
                    cadena=cadena+' '+valor.marker+' '+ValorNumerico(valor.value)+' '+'Procesos'+' ('+valor.percent+'%)'+'<br>';
                    return cadena;
                }
            },
            legend: {
                type: 'scroll',
                orient: 'horizontal',
                
                position:'bottom',
                textStyle:{
                    color:'gray'
                }
            },
            series : [
                {
                    name: 'Cantidad de Procesos por Tipo de Contrato',
                    type: 'pie',
                    radius : '45%',
                    data: datosPastel,
                    itemStyle: {
                        color: function(e){
                            var colores=ObtenerColores('Pastel1');
                            return colores[e.dataIndex];
                        },
                        emphasis: {
                            shadowBlur: 10,
                            shadowOffsetX: 0,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    },
                    label:{
                        color:'gray',
                        formatter:function (e){
                            return ''+e.name+' \n'+ValorNumerico(e.value) +' ('+ ValorMoneda(e.percent) +'%)'
                        }
                    }
                }
            ],
            grid:{
                containLabel:true
            }
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    tooltip:{
                        position: ['0%', '50%']
                    }
                }
            }
        ]
        
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
}).fail(function() {
    
});
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Monto de Contratos por Tipo de Contrato
 */
function MontoProcesosTipoContrato(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#MontoProcesosTipoContrato',true);
    $.get(api+"/dashboardoncae/contratosporcategoria/",parametros).done(function( datos ) {
    
        
    OcultarReloj('#MontoProcesosTipoContrato');
    var datosPastel=[];
    datos.resultados.categorias.forEach(function(valor,indice){
        datosPastel.push(
            {
                name:traducciones[valor.name]?traducciones[valor.name].titulo:valor.name,
                value:valor.value
            }
        )
    });
    if(Array.isArray(datosPastel)  && datosPastel==0){
        MostrarSinDatos('#MontoProcesosTipoContrato',true);
        return;
    }
    var grafico=echarts.init(document.getElementById('MontoProcesosTipoContrato'));
    var opciones = {
        

        baseOption:{
            tooltip : {
                trigger: 'item',
                
                formatter:  function (valor){
    
                    var cadena=valor.name+'<br>';
                   
                    cadena=cadena+' '+valor.marker+' '+ValorMoneda(valor.value)+' '+'HNL'+' ('+valor.percent+'%)'+'<br>';
                    return cadena;
                }
            },
            legend: {
                type: 'scroll',
                orient: 'horizontal',
                textStyle:{
                    color:'gray'
                }
            },
            series : [
                {
                    name: 'Monto de Contratos por Categoría de Compra',
                    type: 'pie',
                    radius : '45%',
                    center: ['50%', '50%'],
                    data: datosPastel,
                    itemStyle: {
                        color: function(e){
                            var colores=ObtenerColores('Pastel1')
                            return colores[e.dataIndex];
                        },
                        emphasis: {
                            shadowBlur: 10,
                            shadowOffsetX: 0,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    },
                    label:{
                        color:'gray',
                        formatter:function (e){
                            return ''+e.name+' \n'+ValorMoneda(e.value) +' HNL ('+ ValorMoneda(e.percent) +'%)'
                        }
                    }
                }
            ],
            grid:{
                containLabel:true
            }
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    tooltip:{
                        position: ['0%', '50%']
                    }
                }
            }
        ]
        
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
    }).fail(function(e) {
        
    });
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Procesos de Contratación por Modalidad de Contratación
 */
function CantidadProcesosModalidadContratacion(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#CantidadProcesosModalidadContratacion',true);
$.get(api+"/dashboardoncae/procesospormodalidad/",parametros).done(function( datos ) {

    
OcultarReloj('#CantidadProcesosModalidadContratacion');
var datosPastel=[];
datos.resultados.modalidades.forEach(function(valor,indice){
    datosPastel.push(
        {
            name:traducciones[valor]?traducciones[valor].titulo:valor,
            value:datos.resultados.procesos[indice]?datos.resultados.procesos[indice]:0
        }
    )
});
if(Array.isArray(datosPastel)  && datosPastel.length==0){
    MostrarSinDatos('#CantidadProcesosModalidadContratacion',true);
    return;
}
var grafico=echarts.init(document.getElementById('CantidadProcesosModalidadContratacion'));
var opciones = {
    baseOption:{
    tooltip : {
        trigger: 'item',
                
        formatter:  function (valor){

            var cadena=valor.name+'<br>';
           
            cadena=cadena+' '+valor.marker+' '+ValorNumerico(valor.value)+' '+'Procesos'+' ('+valor.percent+'%)'+'<br>';
            return cadena;
        }
    },
    legend: {
        type: 'scroll',
        orient: 'vertical',
        right: 10,
        top: 20,
        bottom: 20,
        textStyle:{
            color:'gray'
        }
    },
    series : [
        {
            name: 'Cantidad de Procesos por Modalidad de Contratación',
            type: 'pie',
            radius : '40%',
            center: ['50%', '50%'],
            data: datosPastel,
            itemStyle: {
                color: function(e){
                    var colores=ObtenerColores('Pastel1');
                    return colores[e.dataIndex];
                },
                emphasis: {
                    shadowBlur: 10,
                    shadowOffsetX: 0,
                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
            },
            grid:{
                containLabel:true
            },
            formatter: '{c}',
            label:{
                color:'gray',
                formatter:function (e){
                    return ''+e.name+' \n'+ValorNumerico(e.value) +' ('+ ValorMoneda(e.percent) +'%)'
                }
            }
        }
    ],
    grid:{
        containLabel:true
    }
    },
    media: [ 
        {
            query: {
                
                maxWidth: 600
            },   
            option: {     
                legend: {
                    type:'plain',
                    orient:'horizontal',
                    bottom:0,
                    right:'center',
                    formatter: function (e){
                        return e +'. '+ ValorNumerico(datosPastel.filter(function(data){ if(data.name===e){return true;}}).length?datosPastel.filter(function(data){ if(data.name===e){return true;}})[0].value:0);
                    }
                }
                ,tooltip:{
                    position: ['0%', '50%']
                }
            }
        },
        {
        query: {
            maxWidth: 500
        }, 
        option:{
            
            series : [
                {
                    name: 'Cantidad de Procesos por\nModalidad de Contratación',
                    type: 'pie',
                    radius : '40%',
                    center: ['50%', '30%'],
                    data: datosPastel,
                    itemStyle: {
                        color: function(e){
                            var colores=ObtenerColores('Pastel1');
                            return colores[e.dataIndex];
                        },
                        emphasis: {
                            shadowBlur: 10,
                            shadowOffsetX: 0,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                        
                    },
                    grid:{
                        containLabel:true
                    },
                    formatter: '{c}',
                    
                    label:{
                        show :false
                    },
                    labelLine:{
                        show:false
                    }
                }
            ],
            legend: {
                type:'plain',
                orient:'vertical',
                bottom:0,
                right:'center'
            },
            tooltip: {
                position:['0%','50%']
            }
        }
        }
    ]
    
};
grafico.setOption(opciones, true);


window.addEventListener("resize", function(){
    grafico.resize();
});

}).fail(function() {
    
});

}

/**
 * Obtiene los datos e inicializa el gráfico de Monto de Contratos por Modalidad de Contratación
 */
function MontoContratosModalidadContratacion(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#MontoContratosModalidadContratacion',true);
$.get(api+"/dashboardoncae/contratospormodalidad/",parametros).done(function( datos ) {

    
OcultarReloj('#MontoContratosModalidadContratacion');
var datosPastel=[];
    datos.resultados.modalidades.forEach(function(valor,indice){
        datosPastel.push(
            {
                name:traducciones[valor.name]?traducciones[valor.name].titulo:valor.name,
                value:valor.value
            }
        )
    });
    if(Array.isArray(datosPastel)  && datosPastel.length==0){
        MostrarSinDatos('#MontoContratosModalidadContratacion',true);
        return;
    }
    var grafico=echarts.init(document.getElementById('MontoContratosModalidadContratacion'));
    var opciones = {
        baseOption:{
        tooltip : {
            trigger: 'item',
                
            formatter:  function (valor){

                var cadena=valor.name+'<br>';
               
                cadena=cadena+' '+valor.marker+' '+ValorMoneda(valor.value)+' '+'HNL'+' ('+valor.percent+'%)'+'<br>';
                return cadena;
            }
        },
        legend: {
            type: 'scroll',
            orient: 'vertical',
            right: 10,
            top: 20,
            bottom: 20,
            textStyle:{
                color:'gray'
            }
        },
        calculable:true,
        series : [
            {
                name: 'Monto de Contratos por \n Modalidad de Contratación',
                type: 'pie',
                radius : '40%',
                center: ['50%', '50%'],
                data:datosPastel,
                itemStyle: {
                    color: function(e){
                        var colores=ObtenerColores('Pastel1');
                        return colores[e.dataIndex];
                    },
                    emphasis: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                },grid:{
                    containLabel:true
                },
                label:{
                    color:'gray',
                    formatter:function (e){
                        return ''+e.name+' \n'+ValorMoneda(e.value) +' HNL ('+ ValorMoneda(e.percent) +'%)'
                    }
                }
            }
        ],
        grid:{
            containLabel:false,
        }},
        media: [ 
            {
                query: {
                    maxWidth: 600
                },  
                option: {      
                    legend: {
                        type:'plain',
                        orient:'horizontal',
                        bottom:0,
                        right:'center',
                        formatter: function (e){
                            return e +'. '+ ValorMoneda(datosPastel.filter(function(data){ if(data.name===e){return true;}}).length?datosPastel.filter(function(data){ if(data.name===e){return true;}})[0].value:0)+' HNL';
                        }
                    }
                    ,tooltip:{
                        position: ['0%', '50%']
                    }
                }
            },{
                query:{
                    maxWidth:500
                },
                option:{
                    series : [
                        {
                            name: 'Monto de Contratos por \nModalidad de Contratación',
                            type: 'pie',
                            radius : '40%',
                            center: ['50%', '30%'],
                            data:datosPastel,
                            itemStyle: {
                                color: function(e){
                                    var colores=ObtenerColores('Pastel1');
                                    return colores[e.dataIndex];
                                },
                                emphasis: {
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                                }
                            },grid:{
                                containLabel:true
                            },
                    
                            label:{
                                show :false
                            },
                            labelLine:{
                                show:false
                            }
                        }
                    ],
                    legend: {
                        type:'plain',
                        orient:'vertical',
                        bottom:0,
                        right:'center'
                    },
                    tooltip: {
                        position:['0%','50%']
                    }
                }
            }
        ]
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
}).fail(function() {
    
});
    
}

/**
 * Obtiene los datos e inicializa el gráfico de Top 10 Compradores por Montos de Contrato
 */
function Top10Compradores(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#top10Compradores',true);
$.get(api+"/dashboardoncae/topcompradores/",parametros).done(function( datos ) {

    
OcultarReloj('#top10Compradores');
if(datos&&datos.resultados&&Array.isArray(datos.resultados.montoContratado)  && datos.resultados.montoContratado.length==0){
    MostrarSinDatos('#top10Compradores',true);
    return;
}
var grafico=echarts.init(document.getElementById('top10Compradores'));
    var opciones = {
        baseOption:{
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross',
                    crossStyle: {
                        color: '#999'
                    }
                },
                formatter:  function (e){
    
    
                    
                    var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
    
                    e.forEach(function(valor,indice){
                        cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value) :valor.value) +' '+(valor.seriesIndex==0?'HNL':'')+'<br>'
                    });
                    return cadena;
                }
            },
            toolbox: {
                orient:'horizontal',
                itemsize:20,
                itemGap:15,
                right:20,
                top:25,
                feature: {
                    dataView: {show: true, readOnly: false,title:'Vista',lang: ['Vista de Datos', 'Cerrar', 'Actualizar'] },
                    magicType: {show: true, type: ['line', 'bar'],title:{line:'Linea',bar:'Barra',stack:'Pila',tiled:'Teja'}},
                    restore: {show: true,title:'Restaurar'},
                    saveAsImage: {show: true,title:'Descargar'}
                },
                emphasis:{
                    iconStyle:{
                        textPosition:'top'
                    }
                }
            },
            xAxis: [
                {
                    type: 'value',
                    axisLabel: {
                        formatter: '{value} HNL',
                        rotate:45,
                    showMinLabel:false
                    },
                    axisPointer: {
                        label: {
                            formatter: '{value} HNL'
                        }
                    },
                    textStyle:{
                        color:'gray'
                    },
                    name:'Monto\nContratado'
                }
            ],
            yAxis: [
                {
                    
                    type: 'category',
                    data: datos.resultados.nombreCompradores.reverse(),
                    axisPointer: {
                        type: 'shadow',
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                        
                    },
                    align: 'right',
                    axisLabel:{
                        interval:0,
                        //rotate:45,
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        },
                        showMinLabel:false,
                        padding:[0,0,0,0]
                    },
                    name:'Compradores'
                }
            ],
            series: [
                {
                    name:'Monto Contratado',
                    type:'bar',
                    data:datos.resultados.montoContratado.reverse(),
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[0]
                    },
                    label: {
                        normal: {
                            show:true,
                                fontFamily:'Poppins',
                                fontWeight:700,
                                fontSize:15,
                            position: 'right',
                            formatter: function (e){
                                return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                            },
                            color:'gray'
                        }
                    },
                    barWidth:30,
                    barCategoryGap:'20%',
                    barGap:'50%'
                }
            ],
            grid:{
                containLabel:true,
                right:'10%'
            },
            label:{}
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    xAxis: [
                        
                        {
                            name:'Compradores',
                            type: 'category',
                            data: datos.resultados.nombreCompradores.reverse(),
                            axisPointer: {
                                type: 'shadow',
                                label:{
                                    formatter:function(params){
                                        return  ObtenerParrafo(params.value,40);
                                    }
                                }
                                
                            },
                            align: 'right',
                            axisLabel:{
                                interval:0,
                                rotate:90,
                                showMinLabel:false,
                                padding:[0,0,0,0],
                                formatter:function(e){
                                    return ObtenerParrafo(e,50);
                                }
                            }
                        }
                    ],
                    yAxis: [
                        {
                            type: 'value',
                            axisLabel: {
                                formatter: '{value} HNL',
                                rotate:65,
                            showMinLabel:false
                            },
                            axisPointer: {
                                label: {
                                    formatter: '{value} HNL'
                                }
                            }
                        }
                    ],
                    series: [
                        
                        
                        {
                            name:'Monto Contratado',
                            type:'bar',
                            data:datos.resultados.montoContratado.reverse(),
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[0]
                            },
                            label: {
                                normal: {
                                    show:false,
                                    fontFamily:'Poppins',
                                    fontWeight:700,
                                    fontSize:15,
                                    position: 'top',
                                    formatter: function (e){
                                        return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                                    }
                                }
                            },
                            barWidth:20,
                            barCategoryGap:'20%',
                            barGap:'50%'
                        }
                    ],
                    grid:{
                        containLabel:true,
                        right:'0%',
                        left:0
                    },
                    tooltip: {
                        position:['0%','50%']
                    }
                }
            }
        ]
        
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
}).fail(function() {
    
});
   
}


/**
 * Obtiene los datos e inicializa el gráfico de Top 10 Proveedores por Montos de Contrato
 */
function Top10Proveedores(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    
    MostrarReloj('#top10Proveedores',true);
$.get(api+"/dashboardoncae/topproveedores/",parametros).done(function( datos ) {

    
OcultarReloj('#top10Proveedores');
if(datos&&datos.resultados&&Array.isArray(datos.resultados.montoContratado)  && datos.resultados.montoContratado.length==0){
    MostrarSinDatos('#top10Proveedores',true);
    return;
}
var grafico=echarts.init(document.getElementById('top10Proveedores'));
var opciones = {
    baseOption:{
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross',
                crossStyle: {
                    color: '#999'
                }
            },
            formatter:  function (e){


                
                var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';

                e.forEach(function(valor,indice){
                    cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value) :valor.value) +' '+(valor.seriesIndex==0?'HNL':'')+'<br>'
                });
                return cadena;
            }
        },
        toolbox: {
            orient:'horizontal',
            itemsize:20,
            itemGap:15,
            right:20,
            top:25,
            feature: {
                dataView: {show: true, readOnly: false,title:'Vista',lang: ['Vista de Datos', 'Cerrar', 'Actualizar'] },
                magicType: {show: true, type: ['line', 'bar'],title:{line:'Linea',bar:'Barra',stack:'Pila',tiled:'Teja'}},
                restore: {show: true,title:'Restaurar'},
                saveAsImage: {show: true,title:'Descargar'}
            },
            emphasis:{
                iconStyle:{
                    textPosition:'top'
                }
            }
        },
        xAxis: [
            {
                type: 'value',
                
                                axisLabel: {
                                    formatter: '{value} HNL',
                                    rotate:45,
                            showMinLabel:false
                                },
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
                                    }
                                },
                                name:'Monto\nContratado'
            }
        ],
        yAxis: [
            {
                name:'Proveedores',
                type: 'category',
                data: datos.resultados.nombreProveedores.reverse(),
                axisPointer: {
                    type: 'shadow',
                    label:{
                        formatter:function(params){
                            return  ObtenerParrafo(params.value,40);
                        }
                    }
                },
                axisLabel:{
                    interval:0,
                    //rotate:45,
                    formatter:function(e){
                        return ObtenerParrafo(e,40);
                    },
                    showMinLabel:false,
                    padding:[0,0,0,0]
                }
            }
        ],
        series: [
            {
                name:'Monto Contratado',
                type:'bar',
                data:datos.resultados.montoContratado.reverse(),
                itemStyle:{
                    color: ObtenerColores('Pastel1')[2]
                },
                label: {
                    normal: {
                        show:true,
                        fontFamily:'Poppins',
                        fontWeight:700,
                        fontSize:15,
                        position: 'right',
                        formatter: function (e){
                            return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                        },
                        color:'gray'
                    }
                },
                barWidth:30,
                barCategoryGap:'20%',
                barGap:'50%'
            }
        ],
        grid:{
            containLabel:true,
            right:'15%',
            bottom:'10%'
        }
    },
    media:[
        {
            query:{
                maxWidth:600
            },
            option:{
                tooltip:{
                    position:['0%','50%']
                },
                xAxis: [
                    {
                        name:'Proveedores',
                        type: 'category',
                        data: datos.resultados.nombreProveedores.reverse(),
                        axisPointer: {
                            type: 'shadow',
                            label:{
                                formatter:function(params){
                                    return  ObtenerParrafo(params.value,40);
                                }
                            }
                            
                        },
                        axisLabel:{
                            interval:0,
                            rotate:90,
                            showMinLabel:false,
                            padding:[0,0,0,0],
                            formatter:function(e){
                                return ObtenerParrafo(e,50);
                            }
                        }
                    }
                   
                ],
                yAxis: [
                    {
                        type: 'value',
                                        axisLabel: {
                                            formatter: '{value} HNL',
                                            rotate:65,
                                    showMinLabel:false
                                        },
                                        axisPointer: {
                                            label: {
                                                formatter: '{value} HNL'
                                            }
                                        }
                    }
                ],
                series: [
                    {
                        type:'bar',
                        data:datos.resultados.montoContratado.reverse(),
                        itemStyle:{
                            color: ObtenerColores('Pastel1')[2]
                        },
                        label: {
                            normal: {
                                show:false,
                                fontFamily:'Poppins',
                                fontWeight:700,
                                fontSize:15,
                                position: 'top',
                                formatter: function (e){
                                    return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                                }
                            }
                        },
                        barWidth:20,
                        barCategoryGap:'20%',
                        barGap:'50%'
                    }
                ],
                grid:{
                    containLabel:true,
                    right:'0%',
                    bottom:'0%',
                    left:0
                }
            }
        }
    ]
    
};
grafico.setOption(opciones, true);


window.addEventListener("resize", function(){
    grafico.resize();
});
}).fail(function() {
    
});
   
}

/**
 * Anima el conteo de los gráficos númericos
 */
function InicializarConteo(){
    $('.conteo.moneda').countTo({
        formatter: function (value, options) {
          value = value.toFixed(2/*options.decimals*/);
          value = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
          return value;
      }
      });
}

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Procesos de Contratación
 */
function CargarCajonesCantidadProcesos(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
$.get(api+"/dashboardoncae/estadisticacantidaddeprocesos/",parametros).done(function( datos ) {
    $('#CantidadProcesosPromedio').attr('data-to',datos.resultados.promedio);
    $('#CantidadProcesosPromedio').parent().css({'color':ObtenerColores('Pastel1')[0]});
    $('#CantidadProcesosMenor').attr('data-to',datos.resultados.menor);
    $('#CantidadProcesosMenor').parent().css({'color':ObtenerColores('Pastel1')[0]});
    $('#CantidadProcesosMayor').attr('data-to',datos.resultados.mayor);
    $('#CantidadProcesosMayor').parent().css({'color':ObtenerColores('Pastel1')[0]});
    $('#CantidadProcesosTotal').attr('data-to',datos.resultados.total);
    $('#CantidadProcesosTotal').parent().css({'color':ObtenerColores('Pastel1')[0]});
      $('.cantidadProcesos .conteo').not('.moneda').each(function(index,elemento){
        $(elemento).countTo({
            formatter: function (value, options) {
                value = value.toFixed(options.decimals);
                value = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
                return value;
            },
            from: 0, to: $(elemento).attr('data-to'),'data-speed':$(elemento).attr('data-speed')
          });
      });
  }).fail(function() {
      
      
    });
}

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Contratos
 */
function CargarCajonesCantidadContratos(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
$.get(api+"/dashboardoncae/estadisticacantidaddecontratos/",parametros).done(function( datos ) {
    
    $('#CantidadContratosPromedio').attr('data-to',datos.resultados.promedio);
    $('#CantidadContratosPromedio').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadContratosMenor').attr('data-to',datos.resultados.menor);
    $('#CantidadContratosMenor').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadContratosMayor').attr('data-to',datos.resultados.mayor);
    $('#CantidadContratosMayor').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadContratosTotal').attr('data-to',datos.resultados.total);
    $('#CantidadContratosTotal').parent().css({'color':ObtenerColores('Pastel1')[1]});

      $('.cantidadContratos .conteo').not('.moneda').each(function(index,elemento){
        $(elemento).countTo({
            formatter: function (value, options) {
                value = value.toFixed(options.decimals);
                value = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
                return value;
            },
            from: 0, to: $(elemento).attr('data-to'),'data-speed':$(elemento).attr('data-speed')
          });
      });
  }).fail(function() {
      
      
    });
}

/**
 * Obtiene los datos e inicializa el gráfico de Montos de Contratos
 */
function CargarCajonesMontoContratos(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros)
$.get(api+"/dashboardoncae/estadisticamontosdecontratos/",parametros).done(function( datos ) {
    $('#MontoContratosPromedio').attr('data-to',datos.resultados.promedio);
    
    $('#MontoContratosMenor').attr('data-to',datos.resultados.menor);
    $('#MontoContratosMayor').attr('data-to',datos.resultados.mayor);
    $('#MontoContratosTotal').attr('data-to',datos.resultados.total);
      $('.montoContratos .conteo.moneda').each(function(index,elemento){
        $(elemento).countTo({
            formatter: function (value, options) {
                value = value.toFixed(2/*options.decimals*/);
                value = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
                return value;
            },
            from: 0, to: $(elemento).attr('data-to'),'data-speed':$(elemento).attr('data-speed')
          });
      });
  }).fail(function() {
      
      
    });
}

