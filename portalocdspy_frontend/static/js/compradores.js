/**
 * @file compradores.js Este archivo se incluye en la sección de compradores del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
  $(function(){

  
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
    $('#dependencias').on('change',function(evento){
      filtros=ObtenerFiltros();
      if(evento.currentTarget.checked){
        filtros['dependencias']=1;
      }else{
        filtros['dependencias']=0;
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
  })


  function CargarCompradores(){
    $('#resultadosCompradores').html(
      $('<tr>').append(
        $('<td>',{style:'height:300px;position:relative',colspan:'7',id:'cargando'})
      ));
    MostrarEspera('#cargando');
    var parametros=ObtenerFiltros();
    EliminarEventoModalDescarga('descargaJsonCompradores');
    EliminarEventoModalDescarga('descargaCsvCompradores');
    EliminarEventoModalDescarga('descargaXlsxCompradores');
    parametros['tid']='id';
    $.get(api+"/compradores",parametros).done(function( datos ) {

      console.dir(datos);
    
      AgregarResultados(datos,'#resultadosCompradores');
      MostrarPaginacion(datos);
      
      
        AgregarToolTips();
        VerificarIntroduccion('INTROJS_COMPRADORES',1);
        ObtenerDescargaCompradores(datos);
      
    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');
        AgregarResultados({resultados:[]},'#resultadosCompradores');
        AgregarToolTips();
        VerificarIntroduccion('INTROJS_COMPRADORES',1);
        
      });
    }

    function AsignarEventosFiltro(){
      $('.campoFiltrado input[type="text"]').on(
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

    function Pagina(numero){
      PushDireccion(AccederCompradores({pagina:numero}));
    }
    function CantidadResultados(numero){
      PushDireccion(AccederCompradores({pagina:1,paginarPor:numero}));
    }
    function PushDireccion(direccion){
      window.history.pushState({}, document.title,direccion);
      CargarCompradores();
    }
    function InputFiltro(filtros,desUrl){
      PushDireccion(AccederCompradores(filtros,desUrl));
    }
    function OrdenFiltro(filtro,orden){
      switch(orden){
        case 'ascendente':
            PushDireccion(AccederCompradores({pagina:1,ordenarPor:'asc('+filtrosAPropiedades[filtro]+')'}));
          break;
        case 'descendente':
            PushDireccion(AccederCompradores({pagina:1,ordenarPor:'desc('+filtrosAPropiedades[filtro]+')'}));
          break;
        case 'neutro':
            var filtros=ObtenerFiltros();
            if(filtros['ordenarPor']){
              delete filtros['ordenarPor'];
            }
            filtros['pagina']=1;
            PushDireccion(AccederCompradores(filtros,true));
          break;
        default:
            var filtros=ObtenerFiltros();
            if(filtros['ordenarPor']){
              delete filtros['ordenarPor'];
            }
            filtros['pagina']=1;
            PushDireccion(AccederCompradores(filtros,true));
          break;
    
      }
    }

    function AccederCompradores(opciones,desUrl){
      var direccion=('/compradores/?'+
    
      'pagina='+(opciones.pagina?opciones.pagina:(ObtenerNumero(ObtenerValor('pagina')) ? ObtenerNumero(ObtenerValor('pagina')) : 1))+
      '&paginarPor='+(opciones.paginarPor?opciones.paginarPor:(ObtenerNumero(ObtenerValor('paginarPor')) ? ObtenerNumero(ObtenerValor('paginarPor')) : 5))+
      '&dependencias='+(ValidarCadena(opciones.dependencias)?opciones.dependencias:(ObtenerNumero(ObtenerValor('dependencias')) ? ObtenerNumero(ObtenerValor('dependencias')) : 0))+
    
      (ValidarCadena(opciones.nombre)? '&nombre='+encodeURIComponent(opciones.nombre): (ValidarCadena(ObtenerValor('nombre'))&&!desUrl?'&nombre='+ObtenerValor('nombre'):''))+
      (ValidarCadena(opciones.identificacion)? '&identificacion='+encodeURIComponent(opciones.identificacion): (ValidarCadena(ObtenerValor('identificacion'))&&!desUrl?'&identificacion='+ObtenerValor('identificacion'):''))+
      (ValidarCadena(opciones.tmc)? '&tmc='+encodeURIComponent(reemplazarValor(opciones.tmc,',','')): (ValidarCadena(ObtenerValor('tmc'))&&!desUrl?'&tmc='+ObtenerValor('tmc'):''))+
      (ValidarCadena(opciones.pmc)? '&pmc='+encodeURIComponent(reemplazarValor(opciones.pmc,',','')): (ValidarCadena(ObtenerValor('pmc'))&&!desUrl?'&pmc='+ObtenerValor('pmc'):''))+
      (ValidarCadena(opciones.mamc)? '&mamc='+encodeURIComponent(reemplazarValor(opciones.mamc,',','')): (ValidarCadena(ObtenerValor('mamc'))&&!desUrl?'&mamc='+ObtenerValor('mamc'):''))+
      (ValidarCadena(opciones.fup) ? '&fup='+encodeURIComponent(reemplazarValor(opciones.fup,',','')):(ValidarCadena(ObtenerValor('fup'))&&!desUrl?'&fup='+ObtenerValor('fup'):''))+
      (ValidarCadena(opciones.memc) ? '&memc='+encodeURIComponent(reemplazarValor(opciones.memc,',','')):(ValidarCadena(ObtenerValor('memc'))&&!desUrl?'&memc='+ObtenerValor('memc'):''))+
      (ValidarCadena(opciones.cp) ? '&cp='+encodeURIComponent(reemplazarValor(opciones.cp,',','')):(ValidarCadena(ObtenerValor('cp'))&&!desUrl?'&cp='+ObtenerValor('cp'):''))+
      //(ValidarCadena(opciones.dependencias) ? '&cp='+encodeURIComponent(opciones.dependencias):(ValidarCadena(ObtenerValor('dependencias'))&&!desUrl?'&dependencias='+ObtenerValor('dependencias'):''))+
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
    
      $('#totalResultado').html(datos.paginador['total.items']);
      $('#inicioResultado').html((ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina)))-ObtenerNumero(datos.parametros.paginarPor));
      $('#finResultado').html(((ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina))>ObtenerNumero(datos.paginador['total.items'])) ? ObtenerNumero(datos.paginador['total.items']): (ObtenerNumero(datos.parametros.paginarPor)*(ObtenerNumero(datos.parametros.pagina))) ));
      
      
    }
    function NumeroResultados(){
      $('#paginacionBusqueda').on('change',function(e){
        CantidadResultados($('#paginacionBusqueda').val()?$('#paginacionBusqueda').val():5);
      });
    }
  
    var filtrosAPropiedades={
      fup: 'fecha_ultimo_proceso',
      identificacion:'id',
      mamc:'mayor_monto_contratado',
      memc:'menor_monto_contratado',
      nombre:'name',
      cp:'procesos',
      pmc:'promedio_monto_contratado',
      tmc:'total_monto_contratado'
    };
  
  
    CargarCompradores();
    window.onpopstate = function(e){
      location.reload();
    }
  
    function AgregarResultados(datos,selector){
      var resultados=datos.resultados;
      $(selector).html('');
      for(var i=0;i<resultados.length;i++){
        $(selector).append(
          $('<tr>').append(
            $('<td>',{'data-label':'Comprador'}).append(
              $('<a>',{class:'enlaceTablaGeneral',href:'/comprador/'+encodeURIComponent(resultados[i].id/*uri*/)}).text(resultados[i].name)
            ),
            $('<td>',{'data-label':'Procesos' ,class:'textoAlineadoCentrado'}).text(ValorNumerico(resultados[i].procesos)),
            $('<td>',{'data-label':'Total de Monto Contratado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].total_monto_contratado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
            $('<td>',{'data-label':'Promedio de Monto Contratado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].promedio_monto_contratado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
            $('<td>',{'data-label':'Mayor Monto Contratado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].mayor_monto_contratado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
            $('<td>',{'data-label':'Menor Monto Contratado' ,class:'textoAlineadoDerecha'}).append(ValorMoneda(resultados[i].menor_monto_contratado),$('<span>',{class:'textoColorPrimario',text:' HNL'})),
            $('<td>',{'data-label':'Fecha de Último Proceso' ,class:'textoAlineadoCentrado'}).append(
              $('<span>',{class:resultados[i].fecha_ultimo_proceso&&resultados[i].fecha_ultimo_proceso!='NaT'?'':'textoColorGris' }).text(
                resultados[i].fecha_ultimo_proceso&&resultados[i].fecha_ultimo_proceso!='NaT'?ObtenerFecha(resultados[i].fecha_ultimo_proceso,'fecha'):'No Disponible'
              )
              
              )
          )
        )
      }
      if(!resultados.length){
        $(selector).append(
          $('<tr>',{style:''}).append(
            $('<td>',{'data-label':'','colspan':8}).append(
              $('<h4>',{class:'titularColor textoColorPrimario mt-3 mb-3'}).text('No se Encontraron Compradores')
            )))
      }
    }
  
  function ObtenerFiltros(){
    var parametros={
      pagina : ObtenerNumero(ObtenerValor('pagina')) ? ObtenerNumero(ObtenerValor('pagina')) : 1,
      paginarPor : ObtenerNumero(ObtenerValor('paginarPor')) ? ObtenerNumero(ObtenerValor('paginarPor')) : 5,
      dependencias : ObtenerNumero(ObtenerValor('dependencias'))==1 ? ObtenerNumero(ObtenerValor('dependencias')) : 0
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
    if(Validar(ObtenerValor('fup'))){
      parametros['fup']=decodeURIComponent(ObtenerValor('fup'));
    }
    if(Validar(ObtenerValor('memc'))){
      parametros['memc']=decodeURIComponent(ObtenerValor('memc'));
    }
    if(Validar(ObtenerValor('cp'))){
      parametros['cp']=decodeURIComponent(ObtenerValor('cp'));
    }
    if(Validar(ObtenerValor('ordenarPor'))){
      parametros['ordenarPor']=decodeURIComponent(ObtenerValor('ordenarPor'));
    }
    
    return parametros;
  }
  
  function InicializarDescargas(){

    AbrirModalDescarga('descargaJsonCompradores','Descarga JSON',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaCsvCompradores','Descarga CSV',true);/*Crear Modal Descarga */
    AbrirModalDescarga('descargaXlsxCompradores','Descarga XLSX',true);/*Crear Modal Descarga */
    $('#descargaJSON').on('click',function(e){
      AbrirModalDescarga('descargaJsonCompradores','Descarga JSON');
    });
    $('#descargaCSV').on('click',function(e){
      AbrirModalDescarga('descargaCsvCompradores','Descarga CSV');
    });
    $('#descargaXLSX').on('click',function(e){
      AbrirModalDescarga('descargaXlsxCompradores','Descarga XLSX');
    });
  }
  function ObtenerDescargaCompradores(resultados){
    var datos=resultados;
    /*var parametros=ObtenerFiltros();
    parametros['pagina']=1;
    parametros['paginarPor']=resultados.paginador['total.items'];
    parametros['tid']='id';
    $.get(api+"/compradores",parametros).done(function( datos ) {

*/
      console.dir('Descargas Compradores')
      console.dir(datos);
    
      AgregarEventoModalDescarga('descargaJsonCompradores',function(){
        var descarga=datos.resultados.map(function(e){
          return {
            'Comprador':e.name,
            //'Id':e.id,
            'Procesos':e.procesos,
            'TotalMontoContratado':e.total_monto_contratado,
            'PromedioMontoContratado':e.promedio_monto_contratado,
            'MayorMontoContratado':e.mayor_monto_contratado,
            'MenorMontoContratado':e.menor_monto_contratado,
            'FechaUltimoProceso':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
            'Enlace':url+'/comprador/'+encodeURIComponent(e.id/*uri*/)
          };
        });
        DescargarJSON(descarga,'Compradores');
      });
      AgregarEventoModalDescarga('descargaCsvCompradores',function(){
        var descarga=datos.resultados.map(function(e){
          return {
            'Comprador':e.name,
            //'Id':e.id,
            'Procesos':e.procesos,
            'TotalMontoContratado':e.total_monto_contratado,
            'PromedioMontoContratado':e.promedio_monto_contratado,
            'MayorMontoContratado':e.mayor_monto_contratado,
            'MenorMontoContratado':e.menor_monto_contratado,
            'FechaUltimoProceso':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
            'Enlace':url+'/comprador/'+encodeURIComponent(e.id/*uri*/)
          };
        });
        DescargarCSV(ObtenerMatrizObjeto(descarga) ,'Compradores');
      });
      AgregarEventoModalDescarga('descargaXlsxCompradores',function(){
        var descarga=datos.resultados.map(function(e){
          return {
            'Comprador':e.name,
            //'Id':e.id,
            'Procesos':e.procesos,
            'TotalMontoContratado':e.total_monto_contratado,
            'PromedioMontoContratado':e.promedio_monto_contratado,
            'MayorMontoContratado':e.mayor_monto_contratado,
            'MenorMontoContratado':e.menor_monto_contratado,
            'FechaUltimoProceso':e.fecha_ultimo_proceso?((e.fecha_ultimo_proceso=='NaT')?'':e.fecha_ultimo_proceso):'',
            'Enlace':url+'/comprador/'+encodeURIComponent(e.id/*uri*/)
          };
        });
        DescargarXLSX(ObtenerMatrizObjeto(descarga) ,'Compradores');
      });
    
     /* 
    }).fail(function() {
       
        console.dir('error de api descargas');
        
      });*/
    
  }