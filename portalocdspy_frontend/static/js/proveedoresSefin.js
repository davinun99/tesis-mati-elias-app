/**
 * @file proveedorSefin.js Este archivo se incluye en la sección de Proveedores de SEFIN del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

  $(function(){
  
   /* $.datepicker.regional['es'] = {
    closeText: 'Cerrar',
    prevText: '< Ant',
    nextText: 'Sig >',
    currentText: 'Hoy',
    monthNames: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
    monthNamesShort: ['Ene','Feb','Mar','Abr', 'May','Jun','Jul','Ago','Sep', 'Oct','Nov','Dic'],
    dayNames: ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'],
    dayNamesShort: ['Dom','Lun','Mar','Mié','Juv','Vie','Sáb'],
    dayNamesMin: ['Do','Lu','Ma','Mi','Ju','Vi','Sá'],
    weekHeader: 'Sm',
    dateFormat: 'dd/mm/yy',
    firstDay: 1,
    isRTL: false,
    showMonthAfterYear: false,
    yearSuffix: ''
    };
    $.datepicker.setDefaults( $.datepicker.regional[ "es" ] );


    $('.fecha').datepicker({
           "dateFormat": 'yy-mm-dd'
       });*/
       /*$('#cambioProveedores').change(function(evento){
        if(evento.currentTarget.checked){
          location.href='/proveedores_sefin/?pagina=1&paginarPor=5&anio='+ObtenerAnio();
        }else{
          location.href='/proveedores/?pagina=1&paginarPor=5&anio='+ObtenerAnio();
        }
      });*/
    var configuracionNumerica={ 
      decimalCharacter:'.',
      decimalPlaces:0,
      digitalGroupSpacing:3,
      digitGroupSeparator:','
      }
     var elementosNumericos=[];
     for(let i=0;i<5;i++){
      elementosNumericos[i]= new AutoNumeric($('.elementoNumerico')[i], configuracionNumerica );
     }
    /*$('.fecha').attr('data-field','date');

    $('#dtBox').DateTimePicker({
      buttonsToDisplay:	["HeaderCloseButton", "SetButton"],
      dateFormat:'yyyy-MM-dd',
      language:'es'
    });*/
    //
    ObtenerAniosProveedores();
    $('.fecha').mask('0000-00-00');
  
    $('.OpcionFiltroBusquedaNumerico input').on('change',function(evento){
      cambiarFiltroNumerico(evento.currentTarget);
    });
    
    $('input.campoAzulBusqueda').on('change',function(evento){
      var elemento=$('input.campoAzulBusqueda');
      var filtros={};
      filtros=ObtenerFiltros();
      filtros[elemento.attr('filtro')]=(elemento.val());
      if(!ValidarCadena(filtros[elemento.attr('filtro')])){
        delete filtros[elemento.attr('filtro')];
      }
      filtros['pagina']=1;
      InputFiltro(filtros,true);
    });
    $('#buscarInformacion').on('click',function(evento){
      var elemento=$('input.campoAzulBusqueda');
      var filtros={};
      filtros=ObtenerFiltros();
      filtros[elemento.attr('filtro')]=(elemento.val());
      if(!ValidarCadena(filtros[elemento.attr('filtro')])){
        delete filtros[elemento.attr('filtro')];
      }
      filtros['pagina']=1;
      InputFiltro(filtros,true);
    });
    AsignarEventosFiltro();
    AsignarOrdenTablaFiltros(OrdenFiltro);
    NumeroResultados();
    InicializarDescargas();


  });
  function NumeroResultados(){
    $('#paginacionBusqueda').on('change',function(e){
      CantidadResultados($('#paginacionBusqueda').val()?$('#paginacionBusqueda').val():5);
    });
  }

  var filtrosAPropiedades={
    fua: 'fecha_ultimo_proceso',
    identificacion:'id',
    mamc:'mayor_monto_pagado',
    memc:'menor_monto_pagado',
    nombre:'name',
    cp:'procesos',
    pmc:'promedio_monto_pagado',
    tmc:'total_monto_pagado'
  };


  CargarProveedores();
  window.onpopstate = function(e){
    location.reload();
  }

  function AgregarResultados(datos,selector){
    var resultados=datos.resultados;
    $(selector).html('');
    for(var i=0;i<resultados.length;i++){
      $(selector).append(
        $('<tr>').append(
          $('<td>',{'data-label':'Proveedor'}).append(
            $('<a>',{class:'enlaceTablaGeneral',href:'/proveedor/'+encodeURIComponent(resultados[i].id)}).text(resultados[i].name)
          ),
          $('<td>',{'data-label':'RTN' ,class:'textoAlineadoDerecha'}).text(resultados[i].id),
          $('<td>',{'data-label':'Contratos' ,class:'textoAlineadoCentrado'}).text(ValorNumerico(resultados[i].procesos)),
          $('<td>',{'data-label':'Total de Monto Pagado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].total_monto_pagado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
          $('<td>',{'data-label':'Promedio de Monto Pagado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].promedio_monto_pagado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
          $('<td>',{'data-label':'Mayor Monto Pagado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].mayor_monto_pagado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
          $('<td>',{'data-label':'Menor Monto Pagado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].menor_monto_pagado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
          $('<td>',{'data-label':'Fecha de Última Transacción' ,class:'textoAlineadoCentrado'}).append(
            $('<span>',{class:resultados[i].fecha_ultimo_proceso&&resultados[i].fecha_ultimo_proceso!='NaT'?'':'textoColorGris' }).text(
              resultados[i].fecha_ultimo_proceso&&resultados[i].fecha_ultimo_proceso!='NaT'?ObtenerFecha(resultados[i].fecha_ultimo_proceso,'fecha'):'No Disponible'
            )
            
          ),
          $('<td>',{'data-label':'Año' ,class:'textoAlineadoCentrado'}).text(ObtenerAnio())


          
        )
      )
    }
    if(!resultados.length){
      $(selector).append(
        $('<tr>',{style:''}).append(
          $('<td>',{'data-label':'','colspan':8}).append(
            $('<h4>',{class:'titularColor textoColorPrimario mt-3 mb-3'}).text('No se Encontraron Proveedores')
          )))
    }
  }

function ObtenerFiltros(){
  var parametros={
    pagina : ObtenerNumero(ObtenerValor('pagina')) ? ObtenerNumero(ObtenerValor('pagina')) : 1,
    paginarPor : ObtenerNumero(ObtenerValor('paginarPor')) ? ObtenerNumero(ObtenerValor('paginarPor')) : 5
  }
  if(Validar(ObtenerValor('nombre'))){
    parametros['nombre']=decodeURIComponent(ObtenerValor('nombre'));
  }
  if(Validar(ObtenerValor('identificacion'))){
    parametros['identificacion']= decodeURIComponent(ObtenerValor('identificacion'));
  }
  if(Validar(ObtenerValor('tmc'))){
    parametros['tmc']=decodeURIComponent(ObtenerValor('tmc')) ;
  }
  if(Validar(ObtenerValor('pmc'))){
    parametros['pmc']=decodeURIComponent(ObtenerValor('pmc')) ;
  }
  if(Validar(ObtenerValor('mamc'))){
    parametros['mamc']=decodeURIComponent(ObtenerValor('mamc'));
  }
  if(Validar(ObtenerValor('fua'))){
    parametros['fua']=decodeURIComponent(ObtenerValor('fua'));
  }
  if(Validar(ObtenerValor('memc'))){
    parametros['memc']=decodeURIComponent(ObtenerValor('memc'));
  }
  if(Validar(ObtenerValor('cp'))){
    parametros['cp']=decodeURIComponent(ObtenerValor('cp'));
  }
  if(Validar(ObtenerValor('anio'))){
    parametros['anio']=decodeURIComponent(ObtenerValor('anio'));
  }
  if(Validar(ObtenerValor('ordenarPor'))){
    parametros['ordenarPor']=decodeURIComponent(ObtenerValor('ordenarPor'));
  }
  return parametros;
}

function AccederProveedores(opciones,desUrl){
  var direccion=('/proveedores_sefin/?'+

  'pagina='+(opciones.pagina?opciones.pagina:(ObtenerNumero(ObtenerValor('pagina')) ? ObtenerNumero(ObtenerValor('pagina')) : 1))+
  '&paginarPor='+(opciones.paginarPor?opciones.paginarPor:(ObtenerNumero(ObtenerValor('paginarPor')) ? ObtenerNumero(ObtenerValor('paginarPor')) : 5))+

  (ValidarCadena(opciones.nombre)? '&nombre='+encodeURIComponent(opciones.nombre): (ValidarCadena(ObtenerValor('nombre'))&&!desUrl?'&nombre='+ObtenerValor('nombre'):''))+
  (ValidarCadena(opciones.identificacion)? '&identificacion='+encodeURIComponent(opciones.identificacion): (ValidarCadena(ObtenerValor('identificacion'))&&!desUrl?'&identificacion='+ObtenerValor('identificacion'):''))+
  (ValidarCadena(opciones.tmc)? '&tmc='+encodeURIComponent(reemplazarValor(opciones.tmc,',','')): (ValidarCadena(ObtenerValor('tmc'))&&!desUrl?'&tmc='+ObtenerValor('tmc'):''))+
  (ValidarCadena(opciones.pmc)? '&pmc='+encodeURIComponent(reemplazarValor(opciones.pmc,',','')): (ValidarCadena(ObtenerValor('pmc'))&&!desUrl?'&pmc='+ObtenerValor('pmc'):''))+
  (ValidarCadena(opciones.mamc)? '&mamc='+encodeURIComponent(reemplazarValor(opciones.mamc,',','')): (ValidarCadena(ObtenerValor('mamc'))&&!desUrl?'&mamc='+ObtenerValor('mamc'):''))+
  (ValidarCadena(opciones.fua) ? '&fua='+encodeURIComponent(reemplazarValor(opciones.fua,',','')):(ValidarCadena(ObtenerValor('fua'))&&!desUrl?'&fua='+ObtenerValor('fua'):''))+
  (ValidarCadena(opciones.memc) ? '&memc='+encodeURIComponent(reemplazarValor(opciones.memc,',','')):(ValidarCadena(ObtenerValor('memc'))&&!desUrl?'&memc='+ObtenerValor('memc'):''))+
  (ValidarCadena(opciones.cp) ? '&cp='+encodeURIComponent(reemplazarValor(opciones.cp,',','')):(ValidarCadena(ObtenerValor('cp'))&&!desUrl?'&cp='+ObtenerValor('cp'):''))+
  (ValidarCadena(opciones.anio) ? '&anio='+encodeURIComponent(reemplazarValor(opciones.anio,',','')):(ValidarCadena(ObtenerValor('anio'))&&!desUrl?'&anio='+ObtenerValor('anio'):''))+
  (ValidarCadena(opciones.ordenarPor) ? '&ordenarPor='+encodeURIComponent(opciones.ordenarPor):(ValidarCadena(ObtenerValor('ordenarPor'))&&!desUrl?'&ordenarPor='+ObtenerValor('ordenarPor'):''))

  );


  
  return direccion;
}
function MostrarPaginacion(datos){
  
  var paginacion=ObtenerPaginacion(datos.paginador.page, datos.paginador.num_pages)
  $('.navegacionTablaGeneral').html('');
  if(datos.paginador.has_previous){
    $('.navegacionTablaGeneral').append(
      $('<a href="javascript:void(0)"  pagina="'+datos.paginador.previous_page_number+'"  class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-left"></i></span></a>').on({
        click:function(e){
          Pagina($(e.currentTarget).attr('pagina'))
        }
        
      })
    );
  }
  
  for(var i=0; i<paginacion.length;i++){
    if(paginacion[i]=='...'){
      $('.navegacionTablaGeneral').append(
        $('<a href="javascript:void(0)" class="numerosNavegacionTablaGeneral numeroNormalNavegacionTablaGeneral">').append($('<span>').text(paginacion[i]))
      );
    }else{
      $('.navegacionTablaGeneral').append(
        $('<a href="javascript:void(0)" pagina="'+paginacion[i]+'"  class="numerosNavegacionTablaGeneral '+((paginacion[i]==datos.paginador.page)?'current':'')+'">').on({
          click:function(e){
            Pagina($(e.currentTarget).attr('pagina'))
          }
          
        }).append($('<span>').text(paginacion[i]))
      );
    }
  }
  if(datos.paginador.has_next){
    $('.navegacionTablaGeneral').append(
      $('<a href="javascript:void(0)" pagina="'+datos.paginador.next_page_number+'" class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-right"></i></span></a>').on({
        click:function(e){
          Pagina($(e.currentTarget).attr('pagina'))
        }
        
      })
    );
  }

  $('#totalResultado').html(ValorNumerico(datos.paginador['total.items']) );
  $('#inicioResultado').html((ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina)))-ObtenerNumero(datos.parametros.paginarPor));
  $('#finResultado').html(((ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina))>ObtenerNumero(datos.paginador['total.items'])) ? ObtenerNumero(datos.paginador['total.items']): (ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina))) ));
  
  
}

function Pagina(numero){
  PushDireccion(AccederProveedores({pagina:numero}));
}
function CantidadResultados(numero){
  PushDireccion(AccederProveedores({pagina:1,paginarPor:numero}));
}
function PushDireccion(direccion){
  window.history.pushState({}, document.title,direccion);
  CargarProveedores();
}
function InputFiltro(filtros,desUrl){
  PushDireccion(AccederProveedores(filtros,desUrl));
}
function OrdenFiltro(filtro,orden){
  switch(orden){
    case 'ascendente':
        PushDireccion(AccederProveedores({pagina:1,ordenarPor:'asc('+filtrosAPropiedades[filtro]+')'}));
      break;
    case 'descendente':
        PushDireccion(AccederProveedores({pagina:1,ordenarPor:'desc('+filtrosAPropiedades[filtro]+')'}));
      break;
    case 'neutro':
        var filtros=ObtenerFiltros();
        if(filtros['ordenarPor']){
          delete filtros['ordenarPor'];
        }
        filtros['pagina']=1;
        PushDireccion(AccederProveedores(filtros,true));
      break;
    default:
        var filtros=ObtenerFiltros();
        if(filtros['ordenarPor']){
          delete filtros['ordenarPor'];
        }
        filtros['pagina']=1;
        PushDireccion(AccederProveedores(filtros,true));
      break;

  }
}
function ObtenerAnio(){
  var fecha = new Date();
  var parametros=ObtenerFiltros();
  if(ObtenerNumero(parametros.anio)){
    return ObtenerNumero(parametros.anio);
  }else{
    return fecha.getFullYear();
  }
}

function ObtenerAniosProveedores(){

  $.get(api+"/proveedores-sefin/filtros/",{}).done(function( datos ) {
    console.dir(datos)
    if(datos&&datos.respuesta&&datos.respuesta.length){
      var anios=datos.respuesta.map(function(e){
        return ObtenerNumero(e.key_as_string);
      }).sort(function(a, b){
        return b - a;
      });
      anios.forEach(function(elemento){
        var atributos={value:elemento,text:elemento};
        if(elemento==ObtenerNumero(ObtenerAnio())){
          atributos['selected']='selected';
        }
        $('.campoFiltrado[filtro="anio"] select').append(
          $('<option>',atributos)
        )
      });

     
    }else{
      $('.campoFiltrado[filtro="anio"] select').append(
        $('<option>',{value:ObtenerAnio(),text:ObtenerAnio()})
      )
    }

  }).fail(function() {
    /*Error de Conexion al servidor */
    console.dir('error de api');
    $('.campoFiltrado[filtro="anio"] select').append(
      $('<option>',{value:ObtenerAnio(),text:ObtenerAnio()})
    )
    
    
  });
}

function CargarProveedores(){
$('#resultadosProveedores').html(
  $('<tr>').append(
    $('<td>',{style:'height:300px;position:relative',colspan:'9',id:'cargando'})
  ));
MostrarEspera('#cargando');
var parametros=ObtenerFiltros();
parametros.anio= ObtenerAnio();
if(ValidarCadena(parametros['fua'])){
//  parametros['fua']='"'+parametros['fua']+'"';
}
EliminarEventoModalDescarga('descargaJsonProveedores');
EliminarEventoModalDescarga('descargaCsvProveedores');
EliminarEventoModalDescarga('descargaXlsxProveedores');
$.get(api+"/proveedores-sefin",parametros).done(function( datos ) {
  console.dir(datos);

  AgregarResultados(datos,'#resultadosProveedores');
  MostrarPaginacion(datos);
  
  
    AgregarToolTips();
    VerificarIntroduccion('INTROJS_PROVEEDORES',1);
    ObtenerDescargaProveedores(datos);

  
}).fail(function() {
    /*Error de Conexion al servidor */
    console.dir('error de api');


    AgregarResultados({resultados:[]},'#resultadosProveedores');
    AgregarToolTips();
    VerificarIntroduccion('INTROJS_PROVEEDORES',1);

    
    
  });
}
function ObtenerDescargaProveedores(resultados){
  /*var parametros=ObtenerFiltros();
  parametros['pagina']=1;
  parametros['paginarPor']=resultados.paginador['total.items'];
  $.get(api+"/proveedores-sefin/",parametros).done(function( datos ) {*/
 
    AgregarEventoModalDescarga('descargaJsonProveedores',function(){

      var descarga=resultados.resultados.map(function(e){
        return {
          'Proveedor':e.name,
          'Id':e.id,
          'Procesos':e.procesos,
          'TotalMontoPagado':e.total_monto_pagado,
          'PromedioMontoPagado':e.promedio_monto_pagado,
          'MayorMontoPagado':e.mayor_monto_pagado,
          'MenorMontoPagado':e.menor_monto_pagado,
          'FechaUltimaTransaccion':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
          'Enlace':url+'/proveedor/'+encodeURIComponent(e.id)
        };
      });
      DescargarJSON(descarga,'Proveedores');
    });
    AgregarEventoModalDescarga('descargaCsvProveedores',function(){
      var descarga=resultados.resultados.map(function(e){
        return {
          'Proveedor':e.name,
          'Id':e.id,
          'Procesos':e.procesos,
          'TotalMontoPagado':e.total_monto_pagado,
          'PromedioMontoPagado':e.promedio_monto_pagado,
          'MayorMontoPagado':e.mayor_monto_pagado,
          'MenorMontoPagado':e.menor_monto_pagado,
          'FechaUltimaTransaccion':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
          'Enlace':url+'/proveedor/'+encodeURIComponent(e.id)
        };
      });
      DescargarCSV(ObtenerMatrizObjeto(descarga) ,'Proveedores');
    });
    AgregarEventoModalDescarga('descargaXlsxProveedores',function(){
      var descarga=resultados.resultados.map(function(e){
        return {
          'Proveedor':e.name,
          'Id':e.id,
          'Procesos':e.procesos,
          'TotalMontoPagado':e.total_monto_pagado,
          'PromedioMontoPagado':e.promedio_monto_pagado,
          'MayorMontoPagado':e.mayor_monto_pagado,
          'MenorMontoPagado':e.menor_monto_pagado,
          'FechaUltimaTransaccion':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
          'Enlace':url+'/proveedor/'+encodeURIComponent(e.id)
        };
      });
      DescargarXLSX(ObtenerMatrizObjeto(descarga) ,'Proveedores');
    });
  
    /*
  }).fail(function() {
     
      console.dir('error de api descargas');
      
    });*/
  
}

function AsignarEventosFiltro(){
  $('.campoFiltrado input[type="text"], .campoFiltrado select.campoBlancoTextoSeleccion').on(
    {'change': function(e){
      var elemento=$(e.currentTarget);
      var elementoPadre=elemento.closest('.campoFiltrado');
      var filtros={};
      filtros=ObtenerFiltros();
      switch(elementoPadre.attr('tipo')){
        case 'fecha':
            filtros[elementoPadre.attr('filtro')]=ValidarCadena(elemento.val())?(elemento.attr('opcion')+elemento.val()):'';
            if(!ValidarCadena(filtros[elementoPadre.attr('filtro')])){
              delete filtros[elementoPadre.attr('filtro')];
            }
            filtros['pagina']=1;
            InputFiltro(filtros,true);
        break;
        case 'numero':
            filtros[elementoPadre.attr('filtro')]=ValidarCadena(elemento.val())?(elemento.attr('opcion')+elemento.val()):'';
            if(!ValidarCadena(filtros[elementoPadre.attr('filtro')])){
              delete filtros[elementoPadre.attr('filtro')];
            }
            filtros['pagina']=1;
            InputFiltro(filtros,true);
        break;
        default:
            filtros[elementoPadre.attr('filtro')]=(elemento.val());
            if(!ValidarCadena(filtros[elementoPadre.attr('filtro')])){
              delete filtros[elementoPadre.attr('filtro')];
            }
            filtros['pagina']=1;
            InputFiltro(filtros,true);
        break;
      }

    }}
  )
}


function InicializarDescargas(){

   AbrirModalDescarga('descargaJsonProveedores','Descarga JSON',true);/*Crear Modal Descarga */
   AbrirModalDescarga('descargaCsvProveedores','Descarga CSV',true);/*Crear Modal Descarga */
   AbrirModalDescarga('descargaXlsxProveedores','Descarga XLSX',true);/*Crear Modal Descarga */
   $('#descargaJSON').on('click',function(e){
     AbrirModalDescarga('descargaJsonProveedores','Descarga JSON');
   });
   $('#descargaCSV').on('click',function(e){
     AbrirModalDescarga('descargaCsvProveedores','Descarga CSV');
   });
   $('#descargaXLSX').on('click',function(e){
     AbrirModalDescarga('descargaXlsxProveedores','Descarga XLSX');
   });
 }
