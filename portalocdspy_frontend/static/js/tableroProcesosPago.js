/**
 * @file tableroProcesosPago.js Este archivo se incluye en la sección de Tablero de Proceso de Pago del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**
 * Variable de filtros aplicables con su  titulo y parametro
 * @type {Object} 
 */
 var filtrosAplicables={
    monedas: {titulo:'Moneda',parametro:'moneda'},
    instituciones: {titulo:'Institución Compradora',parametro:'institucion'},
    años: {titulo:'Año',parametro:'año'},
    proveedores: {titulo:'Proveedor',parametro:'proveedor'},
    fuentes: {titulo:'Fuente de Financiamiento',parametro:'fuentefinanciamiento'},
    objetosGasto : {titulo:'Objeto de Gasto',parametro:'objetosgasto'}
  };

/**
 * Variable de filtros aplicables con su  titulo y parametro con su parametro como llave
 * @type {Object} 
 */
  var filtrosAplicablesR={
    moneda: {titulo:'Moneda',parametro:'monedas'},
    institucion: {titulo:'Institución Compradora',parametro:'instituciones'},
    año: {titulo:'Año',parametro:'años'},
    proveedor: {titulo:'Proveedor',parametro:'proveedores'},
    fuentefinanciamiento: {titulo:'Fuente de Financiamiento',parametro:'fuentes'},
    objetosgasto : {titulo:'Objeto de Gasto',parametro:'objetosGasto'}
    
  };

/**
 * Arreglo para definir el orden en el que se presentan los filtros
 * @type {string[]} 
 */
  var ordenFiltros=['años','monedas','instituciones','proveedores','fuentes'];
/**
 * Objeto para obtener traducciones e informacion de algunos códigos el OCDS
 * @type {Object} 
 */
  var traducciones={
    'goods':{titulo:'Bienes y provisiones',descripcion:'El proceso de contrataciones involucra bienes o suministros físicos o electrónicos.'},
    'works':{titulo:'Obras',descripcion:'El proceso de contratación involucra construcción reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
    'services':{titulo:'Servicios',descripcion:'El proceso de contratación involucra servicios profesionales de algún tipo, generalmente contratado con base de resultados medibles y entregables. Cuando el código de consultingServices está disponible o es usado por datos en algún conjunto da datos en particular, el código de servicio sólo debe usarse para servicios no de consultoría.'},
    'consultingServices':{titulo:'Servicios de consultoría',descripcion:'Este proceso de contratación involucra servicios profesionales provistos como una consultoría.'}
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

    
    InicializarCantidadPagos();
    InicializarMontoPagos();
    Top10Proveedores();
    Top10Compradores();
    CargarCajonesMontos();
    CargarCajonesCantidad();
    MontoPagosEtapas();
    Top10CantidadProcesosObjetosGasto();
}


/**
 * Obtiene los datos de los filtros a mostrar
 */
function ObtenerFiltros(){
    
    var parametros=ObtenerJsonFiltrosAplicados({});
    $.get(api+"/dashboardsefin/filtros/",parametros).done(function( datos ) {

        if(datos.respuesta.objetosGasto){
            delete datos.respuesta.objetosGasto;
        }
      
       
  
    MostrarListaElastica(datos,'#elastic-list');
    MostrarEtiquetaListaElasticaAplicada();
    MostrarListaElasticaAplicados();
  
  
      
        
      }).fail(function() {
          
          
        });

}




function ObtenerJsonFiltrosAplicados(parametros){
    if(Validar(ObtenerValor('moneda'))){
        parametros['moneda']=decodeURIComponent(ObtenerValor('moneda'));
    }
    if(Validar(ObtenerValor('institucion'))){
    parametros['institucion']=decodeURIComponent(ObtenerValor('institucion'));
    }
    if(Validar(ObtenerValor('año'))){
      parametros['año']=decodeURIComponent(ObtenerValor('año'));
    }
    if(Validar(ObtenerValor('proveedor'))){
        parametros['proveedor']=decodeURIComponent(ObtenerValor('proveedor'));
    }
    if(Validar(ObtenerValor('fuentefinanciamiento'))){
      parametros['fuentefinanciamiento']=decodeURIComponent(ObtenerValor('fuentefinanciamiento'));
    }
    if(Validar(ObtenerValor('objetosgasto'))){
        parametros['objetosgasto']=decodeURIComponent(ObtenerValor('objetosgasto'));
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
    var direccion=('/tableroProcesosPago/?'+
    (ValidarCadena(opciones.moneda)? '&moneda='+encodeURIComponent(opciones.moneda): (ValidarCadena(ObtenerValor('moneda'))&&!desUrl?'&moneda='+ObtenerValor('moneda'):''))+
    (ValidarCadena(opciones.institucion)? '&institucion='+encodeURIComponent(opciones.institucion): (ValidarCadena(ObtenerValor('institucion'))&&!desUrl?'&institucion='+ObtenerValor('institucion'):''))+
   
    (ValidarCadena(opciones.año)? '&año='+encodeURIComponent(opciones.año): (ValidarCadena(ObtenerValor('año'))&&!desUrl?'&año='+ObtenerValor('año'):''))+
    (ValidarCadena(opciones.proveedor)? '&proveedor='+encodeURIComponent(opciones.proveedor): (ValidarCadena(ObtenerValor('proveedor'))&&!desUrl?'&proveedor='+ObtenerValor('proveedor'):''))+
    (ValidarCadena(opciones.fuentefinanciamiento)? '&fuentefinanciamiento='+encodeURIComponent(opciones.fuentefinanciamiento): (ValidarCadena(ObtenerValor('fuentefinanciamiento'))&&!desUrl?'&fuentefinanciamiento='+ObtenerValor('fuentefinanciamiento'):''))+
    (ValidarCadena(opciones.objetosgasto) ? '&objetosgasto='+encodeURIComponent(opciones.objetosgasto):(ValidarCadena(ObtenerValor('objetosgasto'))&&!desUrl?'&objetosgasto='+ObtenerValor('objetosgasto'):''))+
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
      );
    });
    $('.filtrosContenedoFiltrosBusqueda').attr('style','height:calc(100vh - '+($('#extencionFiltrosAplicados').height()?123:110)+'px - '+($('#extencionFiltrosAplicados').height() + ($('#extencionFiltrosAplicados').height()?4:0))+'px)');

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
        if(filtrosAplicablesR[llave]){
            $('ul#ul'+ filtrosAplicablesR[llave].parametro ).find(
                'li[valor="'+(valor).toString()+'"]'
              ).addClass('active');
        }
      
    });
  }


/**
 * Muestra el contenido de los filtros aplicados, las opciones de selección
 */
  function MostrarListaElastica(datos,selector){
    $(selector).html('');
    var arregloFiltros=[];
    $.each(datos.respuesta,function(llave,valor){
        arregloFiltros.push({
            'posicion':ordenFiltros.includes(llave)?ordenFiltros.indexOf(llave):99,
            'llave':llave,
            'valor':valor
        });
    });
    arregloFiltros=arregloFiltros.sort(function(a, b){return a.posicion - b.posicion;});
    $.each(arregloFiltros,function(indice,elemento){

      $(selector).append(
        $('<div class="list-container col-md-12 2">').append(
          $('<div class="panel panel-default ">').append(
            $('<div class="panel-heading">').text(
              filtrosAplicables[elemento.llave]?filtrosAplicables[elemento.llave].titulo:elemento.llave
            ),
            $('<input>',{type:'text', class:'elastic-filter',placeholder:filtrosAplicables[elemento.llave]?filtrosAplicables[elemento.llave].titulo:elemento.llave ,filtro:elemento.llave,on:{
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
            }}),
            $('<style>',{id:'style'+elemento.llave}),
            $('<ul >',{class:'list-group',id:'ul'+elemento.llave}).append(
              AgregarPropiedadesListaElastica(elemento.valor,elemento.llave)
            ),
            ['instituciones','proveedores'].includes(elemento.llave)&&elemento.valor&&elemento.valor.buckets&&elemento.valor.buckets.length>=50?
              $('<a>',{
                class:'enlaceTablaGeneral ptextoColorPrimario pcursorMano',
                href:'javascript:void(0)',
                style:'width:150px;padding:5px 15px',
                text: elemento.valor.buckets.length==50? 'Mostrar Todos...':'Mostrar Menos...',
                toolTexto:elemento.valor.buckets.length==50?'Mostrar más resultados':'Mostrar menos resultados',
                toolCursor:'true',
                llave:elemento.llave,
                on:{
                  click:MostrarMasResultados
                }
              })
            :null
              
            
          )
        )
      );
      
      
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
 * Agrega los elementos que pueden ser seleccionados en un categoría
 * @param {Object[]} valor - arreglo de valores seleccionables en un filtro
 * @param {string} llave - llave de un filtro 
 */
function AgregarPropiedadesListaElastica(valor,llave){
    var elementos=[]
    $.each(valor.buckets,function(i,propiedades){

      elementos.push(
        $('<li >',{
        class:'list-group-item',
        valor:propiedades.key_as_string?propiedades.key_as_string:propiedades.key, 
        formato: (propiedades.key_as_string?propiedades.key_as_string:(traducciones[propiedades.key]?traducciones[propiedades.key].titulo:propiedades.key)).toString().toLowerCase(),'llave':llave,
        toolTexto:propiedades.key_as_string?propiedades.key_as_string:(traducciones[propiedades.key]?traducciones[propiedades.key].titulo:propiedades.key),
        toolCursor:'true',
        on:{
          click:function(e){
            var filtro=$(e.currentTarget);
            if(filtro.hasClass('active')){
              filtro.removeClass('active')
            }else{
              filtro.parent().find('.list-group-item.active').removeClass('active');
              filtro.addClass('active');
            }

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
          $('<div class="badge">').text( ValorNumerico(propiedades.doc_count)),
          $('<div >',{
          class:'elastic-data',
          
          text:propiedades.key_as_string?propiedades.key_as_string:(traducciones[propiedades.key]?traducciones[propiedades.key].titulo:propiedades.key)}
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
              $('<input>',{type:'text', class:'elastic-filter',placeholder:filtrosAplicables[elemento]?filtrosAplicables[elemento].titulo:elemento ,filtro:elemento}),
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
 * Obtiene los datos e inicializa el gráfico de Cantidad de Pagos por Mes
 */
function InicializarCantidadPagos(){

    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#cantidadPagos',true);
    $.get(api+"/dashboardsefin/cantidaddepagos/",parametros).done(function( datos ) {
        
        OcultarReloj('#cantidadPagos');
        if((datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadpagos)  && datos.resultados.cantidadpagos.length==0)||datos.resultados.cantidadpagos.map(function(e){return ObtenerNumero(e);}).reduce(function(a, b){return a + b;}, 0)==0){
            MostrarSinDatos('#cantidadPagos',true);
            return;
        }
        var grafico=echarts.init(document.getElementById('cantidadPagos'));
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
                            cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorNumerico(valor.value):valor.value) +' '+(valor.seriesIndex==0?'Pagos':'%')+'<br>'
                        });
                        return cadena;
                    }
                },
            legend: {
                plain: 'scroll',
                orient: 'horizontal',
                position:'bottom'
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
                        }
                    }
                ],
                grid:{
                    containLabel:true,
                    right:'15%'
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
                                formatter: '{value} Pagos'
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
                        name:'Cantidad de Pagos',
                        type:'bar',
                        data:datos.resultados.cantidadpagos,
                        itemStyle:{
                            color: ObtenerColores('Pastel1')[0]
                        }
                    },
                    {
                        name:'Porcentaje en Relación al Año',
                        type:'line',
                        data:datos.resultados.promediopagos.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
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
                ]
            },
            media:[
                {
                    query:{
                        maxWidth:600
                    },
                    option:{
                        yAxis: [
                            {
                                type: 'value',
                                name: 'Cantidad',
                                min: 0,
                                axisLabel: {
                                    formatter: '{value}',
                                    rotate:65
                                },
                                position:'left',
                                axisPointer: {
                                    label: {
                                        formatter: '{value} Pagos'
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
                        tooltip: {
                            position:['0%','50%']
                        },
                        grid:{
                            left:0,
                            right:0
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
 * Obtiene los datos e inicializa el gráfico de Monto de Pagos por Mes
 */
function InicializarMontoPagos(){

    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#montoPagos',true);
    $.get(api+"/dashboardsefin/montosdepagos/",parametros).done(function( datos ) {
        OcultarReloj('#montoPagos');
        if((datos&&datos.resultados&&Array.isArray(datos.resultados.montopagos)  && datos.resultados.montopagos.length==0)||datos.resultados.montopagos.map(function(e){return ObtenerNumero(e);}).reduce(function(a, b){return a + b;}, 0)==0){
            MostrarSinDatos('#montoPagos',true);
            return;
        }
        var grafico=echarts.init(document.getElementById('montoPagos'));
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
                    cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value):valor.value) +' '+(valor.seriesIndex==0?'HNL':'%')+'<br>'
                });
                return cadena;
                }
            },
            legend: {
                plain: 'scroll',
                orient: 'horizontal',
                position:'bottom'
            },
            grid:{
                containLabel:true,
                right:'15%'
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
                    }
                }
            ],
            yAxis: [
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
                    name:'Monto Pagado',
                    type:'bar',
                    data:datos.resultados.montopagos,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[1]
                    }
                },
                {
                    name:'Porcentaje en Relación al Año',
                    type:'line',
                    data:datos.resultados.promediopagos.map(function(e){return ObtenerNumero((ObtenerNumero(e)*100).toFixed(2))}),
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
            ]
        },
            media:[
                {
                    query:{
                        maxWidth:600
                    },
                    option:{
                        yAxis: [
                            {
                                type: 'value',
                                name: 'Monto',
                                min: 0,
                                axisLabel: {
                                    formatter: '{value} HNL',
                                    rotate:65
                                },
                                name:'Lempiras',
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
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
                        tooltip: {
                            position:['0%','50%']
                        },
                        grid:{
                            left:0,
                            right:0
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
 * Obtiene los datos e inicializa el gráfico de Etapas de Pagos Realizados en un Proceso de Compra
 */

function MontoPagosEtapas(){
    var parametros={};
        parametros=ObtenerJsonFiltrosAplicados(parametros);
        MostrarReloj('#montoPagosEtapas',true);
        $.get(api+"/dashboardsefin/etapaspago/",parametros).done(function( datos ) {
            
            OcultarReloj('#montoPagosEtapas');
            if(datos&&datos.resultados&&Array.isArray(datos.resultados.montos)  && datos.resultados.montos.length==0){
                MostrarSinDatos('#montoPagosEtapas',true);
                return;
            }
    var grafico=echarts.init(document.getElementById('montoPagosEtapas'));
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
                    return "{b0}<br>{a0} {c0} HNL ({p0}%)".replace('{p0}',ValorNumerico(datos.resultados.porcentajes[e[0].dataIndex].toFixed(2) ) ).replace('{a0}',e[0].marker).replace('{b0}',e[0].name).replace('{c0}',ValorMoneda( e[0].value));
                }
            },
            grid: {
                containLabel: true,
                left: '3%',
                right: '4%',
                bottom: '3%',
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
                    
                        type : 'value',
                        splitLine:{show:false},
                        axisLabel: {
                                formatter: function (e){
                                    
                                    return "{c} HNL ".replace('{c}',ValorMoneda( e) );
                                    
                                },rotate:45,
                                showMinLabel:false
                            },
                            
                        axisPointer: {
                            label: {
                                formatter: '{value} HNL'
                            }
                        }
                }
            ],
            yAxis: [
                
                
                {
                    type : 'category',
                    axisTick : {show: false},
                    axisLine:{show:false},
                    data : datos.resultados.series
                }
            ],
            series: [
                {
                    type:'bar',
                    data:datos.resultados.montos,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[6]
                    },
                    seriesLayoutBy: 'row',
                    label:{
                        show:true,
                        fontFamily:'Poppins',
                        fontWeight:700,
                        fontSize:20,
                        align:'right',
                        formatter:  function (e){
                            return "{c} HNL".replace('{c}',ValorMoneda( e.value));
                        }
                    }
                }
            ]
        }
        ,
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    xAxis: [
                        {
                            
                                type : 'value',
                                splitLine:{show:false},
                                axisLabel: {
                                        formatter: function (e){
                                            
                                            return "{c} HNL ".replace('{c}',ValorMoneda( e) );
                                            
                                        },
                                        rotate:90,
                                        showMinLabel:false
                                    },
                                    
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
                                    }
                                }
                        }
                    ],
                    yAxis: [
                        
                        
                        {
                            type : 'category',
                            axisTick : {show: false},
                            axisLine:{show:false},
                            axisLabel:{
                                rotate:65,
                                showMinLabel:false,
                                interval:0
                            },
                            data : datos.resultados.series,
                            
                        }
                    ],
                    series: [
                        {
                            type:'bar',
                            data:datos.resultados.montos,
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[3]
                            },
                            seriesLayoutBy: 'row',
                            label:{
                                show:false,
                                fontFamily:'Poppins',
                                fontWeight:700,
                                fontSize:20,
                                align:'right',
                                formatter:  function (e){
                                    return "{c} HNL".replace('{c}',ValorMoneda( e.value));
                                }
                            }
                        }
                    ],
                    tooltip: {
                        position:['0%','50%']
                    },
                    grid:{
                        left:0,
                        right:'10%'
                    }
                }
            }
        ]
    };
    grafico.setOption(opciones, true);

    
    window.addEventListener("resize", function(){
        grafico.resize();
    });
});
}


/**
 * Obtiene los datos e inicializa el gráfico de Top 10 Compradores por Monto Pagado
 */
function Top10Compradores(){
  
    var parametros={};
        parametros=ObtenerJsonFiltrosAplicados(parametros);
        MostrarReloj('#top10Compradores',true);
        $.get(api+"/dashboardsefin/topcompradores/",parametros).done(function( datos ) {
            
            OcultarReloj('#top10Compradores');
            if(datos&&datos.resultados&&Array.isArray(datos.resultados.montos)  && datos.resultados.montos.length==0){
                MostrarSinDatos('#top10Compradores',true);
                return;
            }
            var grafico=echarts.init(document.getElementById('top10Compradores'));
            var opciones = {
                baseOption:{
                    tooltip: {
                        show:true,
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
                            }
                        }
                    ],
                    grid:{
                        containLabel:true,
                        right:'15%'
                    },
                    yAxis: [
                        {
                            
                            type: 'category',
                            data: datos.resultados.compradores.reverse(),
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
                                showMinLabel:false,
                                padding:[0,0,0,0],
                                formatter:function(e){
                                    
                                    return ObtenerParrafo(e,30);
                                }
                            },
                            
                        }
                    ],
                    series: [
                        {
                            name:'Monto Pagado',
                            type:'bar',
                            data:datos.resultados.montos.reverse(),
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[9]
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
                                    }
                                }
                            },
                            barWidth:30,
                            barCategoryGap:'20%',
                            barGap:'50%'
                        }
                    ],
                    label:{
                        color:'gray'
                    }
                }
                ,
                media:[
                    {
                        query:{
                            maxWidth:600
                        },
                        option:{
                            xAxis: [
                                
                                {
                                    
                                    type: 'category',
                                    data: datos.resultados.compradores.reverse(),
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
                                        showMinLabel:false,
                                        padding:[0,0,0,0],
                                        interval:0,
                                        rotate:90,
                                        
                                        formatter:function(e){
                                            return ObtenerParrafo(e,40);
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
                                    name:'Monto Pagado',
                                    type:'bar',
                                    data:datos.resultados.montos.reverse(),
                                    itemStyle:{
                                        color: ObtenerColores('Pastel1')[9]
                                    },
                                    label: {
                                        normal: {
                                            show:false,
                                                fontFamily:'Poppins',
                                                fontWeight:700,
                                                fontSize:11,
                                            position: 'right',
                                            formatter: function (e){
                                                return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                                            }
                                        }
                                    },
                                    barWidth:20,
                                    barCategoryGap:'10%',
                                    barGap:'10%'
                                }
                            ],
                            tooltip: {
                                position:['0%','50%']
                            },
                            grid:{
                                left:0,
                                right:0
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
 * Obtiene los datos e inicializa el gráfico de Top 10 Proveedores por Monto Pagado y Moneda
 */
function Top10Proveedores(){
    var parametros={};
parametros=ObtenerJsonFiltrosAplicados(parametros);
MostrarReloj('#top10Proveedores',true);
                $.get(api+"/dashboardsefin/topproveedores/",parametros).done(function( datos ) {
                    OcultarReloj('#top10Proveedores');
                    
                    if(datos&&datos.resultados&&Array.isArray(datos.resultados.montos)  && datos.resultados.montos.length==0){
                        MostrarSinDatos('#top10Proveedores',true);
                        return;
                    }
                    var grafico=echarts.init(document.getElementById('top10Proveedores'));
                    var opciones ={
                        baseOption:{
                            tooltip : {
                                trigger: 'axis',
                                axisPointer : {           
                                    type : 'cross',
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
                            grid: {
                                left: '3%',
                                right:'15%',
                                bottom: '3%',
                                containLabel: true
                            },
                            xAxis:  {
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
                                }
                            },
                            yAxis: {
                                type: 'category',
                                data: datos.resultados.proveedores.reverse(),
                                axisLabel:{
                                    interval:0,
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        },
                        showMinLabel:false,
                        padding:[0,0,0,0]
                                }
                            },
                            series: [
                                {
                                    name: 'Monto de Contrato, Pagados en HNL',
                                    type: 'bar',
                                    stack: 'Monto de Contrato',
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
                                    data: datos.resultados.montos.reverse(),
                                    itemStyle:{
                                        color: ObtenerColores('Pastel1')[0]
                                    }
                                }
                            ]
                        }
                        ,
                        media:[
                            {
                                query:{
                                    maxWidth:600
                                },
                                option:{
                                    xAxis:  {
                                        type: 'category',
                                        data: datos.resultados.proveedores.reverse(),
                                        axisLabel:{
                                            rotate:90,
                                            interval:0,
                                            formatter:function(e){
                                                return ObtenerParrafo(e,40);
                                            },
                                            showMinLabel:false,
                                            padding:[0,0,0,0]
                                        },
                                        axisPointer:{
                                            
                                        label:{
                                            formatter:function(params){
                                                return  ObtenerParrafo(params.value,40);
                                            }
                                        }
                                        }
                                    },
                                    yAxis: {
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
                                    },
                                    series: [
                                        {
                                            name: 'Monto de Contrato, Pagados en HNL',
                                            type: 'bar',
                                            stack: 'Monto de Contrato',
                                            label: {
                                                normal: {
                                                    show:false,
                                                    fontFamily:'Poppins',
                                                    fontWeight:700,
                                                    fontSize:15,
                                                    position: 'right',
                                                    formatter: function (e){
                                                        return "{c} HNL".replace('{c}',ValorMoneda(e.value));
                                                    }
                                                }
                                            },
                                            data: datos.resultados.montos.reverse(),
                                            itemStyle:{
                                                color: ObtenerColores('Pastel1')[0]
                                            }
                                        }
                                    ],
                                    tooltip: {
                                        position:['0%','50%']
                                    },
                                    grid:{
                                        left:0,
                                        right:0
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
 * Obtiene los datos e inicializa el gráfico de Top 10 de Objetos de Gasto por Cantidad de Procesos
 */
function Top10CantidadProcesosObjetosGasto(){
    
    var parametros={};
parametros=ObtenerJsonFiltrosAplicados(parametros);
MostrarReloj('#Top10CantidadProcesosObjetosGasto',true);
                $.get(api+"/dashboardsefin/topobjetosgasto/",parametros).done(function( datos ) {
                    OcultarReloj('#Top10CantidadProcesosObjetosGasto');
                    if(datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadProcesos)  && datos.resultados.cantidadProcesos.length==0){
                        MostrarSinDatos('#Top10CantidadProcesosObjetosGasto',true);
                        return;
                    }
                    var grafico=echarts.init(document.getElementById('Top10CantidadProcesosObjetosGasto'));
                    var opciones ={
                        baseOption:{

                        
                        tooltip : {
                            trigger: 'axis',
                            axisPointer : {           
                                type : 'cross',
                                crossStyle: {
                                    color: '#999'
                                }
                            },
                            formatter:  function (e){
                
                
                                
                                var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
                
                                e.forEach(function(valor,indice){
                                    cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorNumerico(valor.value) :valor.value) +' '+(valor.seriesIndex==0?' Procesos ':'')+'<br>'
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
                        grid: {
                            left: '3%',
                            right:'15%',
                            bottom: '3%',
                            containLabel: true
                        },
                        xAxis:  {
                            type: 'value',
                            axisLabel: {
                                formatter: '{value}',
                                rotate:45,
                            showMinLabel:false
                            },
                            axisPointer: {
                                label: {
                                    formatter: '{value}'
                                }
                            }
                        },
                        yAxis: {
                            type: 'category',
                            data: datos.resultados.objetosGasto.reverse(),
                            axisLabel:{
                                interval:0,
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        },
                        showMinLabel:false,
                        padding:[0,0,0,0]
                            },
                            axisPointer:{
                                
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                            }
                        },
                        series: [
                            {
                                name: 'Cantidad de Pagos',
                                type: 'bar',
                                stack: 'Cantidad de Pagos',
                                label: {
                                    normal: {
                                        show:true,
                                    fontFamily:'Poppins',
                                    fontWeight:700,
                                    fontSize:15,
                                        position: 'right',
                                        formatter: function (e){
                                            return "{c}".replace('{c}',ValorNumerico(e.value));
                                        },
                                        color:'gray'
                                    }
                                },
                                data: datos.resultados.montos.reverse(),
                                itemStyle:{
                                    color: ObtenerColores('Pastel1')[2]
                                }
                            }
                        ]},
                        media:[
                            {
                                query:{
                                    maxWidth:600
                                },
                                option:{
                                    xAxis:  
                                    {
                                        type: 'category',
                                        data: datos.resultados.objetosGasto.reverse(),
                                        axisLabel:{
                                            rotate:90,
                                            showMinLabel:false,
                                            padding:[0,0,0,0],
                                            interval:0,
                                            formatter:function(e){
                                                return ObtenerParrafo(e,30);
                                            }
                                        },
                                        axisPointer:{
                                            
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                                        }
                                    },
                                    yAxis: {
                                        type: 'value',
                                        axisLabel: {
                                            formatter: '{value}',
                                            rotate:65,
                                            showMinLabel:false
                                        },
                                        axisPointer: {
                                            label: {
                                                formatter: '{value}'
                                            }
                                        }
                                    },
                                    series: [
                                        {
                                            name: 'Cantidad de Pagos',
                                            type: 'bar',
                                            stack: 'Cantidad de Pagos',
                                            label: {
                                                normal: {
                                                    show:false,
                                                fontFamily:'Poppins',
                                                fontWeight:700,
                                                fontSize:11,
                                                    position: 'right',
                                                    formatter: function (e){
                                                        return "{c}".replace('{c}',ValorNumerico(e.value));
                                                    }
                                                }
                                            },
                                            data: datos.resultados.montos.reverse(),
                                            itemStyle:{
                                                color: ObtenerColores('Pastel1')[2]
                                            }
                                        }
                                    ],
                                    tooltip: {
                                        position:['0%','50%']
                                    },
                                    grid:{
                                        left:0,
                                        right:0
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
 * Obtiene los datos e inicializa el gráfico de Montos de Pagos por Año
 */
function CargarCajonesMontos(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
$.get(api+"/dashboardsefin/estadisticamontosdepagos/",parametros).done(function( datos ) {
    
    $('#MontoPagosPromedio').attr('data-to',datos.resultados.promedio);
    $('#MontoPagosMenor').attr('data-to',datos.resultados.menor);
    $('#MontoPagosMayor').attr('data-to',datos.resultados.mayor);
    $('#MontoPagosTotal').attr('data-to',datos.resultados.total);


  $('.conteo.moneda').each(function(index,elemento){
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

/**
 * Obtiene los datos e inicializa el gráfico de Cantidad de Pagos por Año
 */
function CargarCajonesCantidad(){
    var parametros={};
    parametros=ObtenerJsonFiltrosAplicados(parametros);
$.get(api+"/dashboardsefin/estadisticacantidaddepagos/",parametros).done(function( datos ) {
    $('#CantidadPagosPromedio').attr('data-to',datos.resultados.promedio);
    $('#CantidadPagosPromedio').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadPagosMenor').attr('data-to',datos.resultados.menor);
    $('#CantidadPagosMenor').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadPagosMayor').attr('data-to',datos.resultados.mayor);
    $('#CantidadPagosMayor').parent().css({'color':ObtenerColores('Pastel1')[1]});
    $('#CantidadPagosTotal').attr('data-to',datos.resultados.total);
    $('#CantidadPagosTotal').parent().css({'color':ObtenerColores('Pastel1')[1]});

      $('.conteo').not('.moneda').each(function(index,elemento){
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
