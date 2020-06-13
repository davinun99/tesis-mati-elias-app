/**
 * @file pesosContratos.js Este archivo se incluye en la sección de Visualización de Pesos de Contratos del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
$(function(){
    MostrarEspera('#VisualizacionGeneral')
    MostrarSeleccionFecha();
    $('#botonEmbed').click(function(e){
      var direccion=''
      var parametros=ObtenerJsonFiltrosAplicados({});
      direccion=url+AccederUrlPagina(parametros).replace('pesosContratos','pesosContratosEmbed')
      AbrirModalEmbeber('EmbeberBurbujasContratos','Código Embebible','<iframe src="'+direccion+'" style="width: 1200px; height: 1200px; border: none; padding: 0; margin: 0;"></iframe>');

      
    });
});
window.onpopstate = function(e){
  location.reload();
}
var traducciones={
  goodsOrServices:{titulo:'Bienes y/o Servicios'},
  works:{titulo:'Obras'},
  consultingServices:{titulo:'Consultorías'},
  services:{titulo:'Servicios'},
  ND:{titulo:'Sin Categoría'}
}
var colores={
  /*goodsOrServices:ObtenerColores('Pastel1')[0],
  works:ObtenerColores('Pastel1')[1],
  consultingServices:ObtenerColores('Pastel1')[2],
  services:ObtenerColores('Pastel1')[4],
  ND:ObtenerColores('Pastel1')[5],*/
  'Bienes y/o Servicios':ObtenerColores('Pastel1')[0],
  'Obras':ObtenerColores('Pastel1')[1],
  'Consultorías':ObtenerColores('Pastel1')[2],
  'Servicios':ObtenerColores('Pastel1')[4],
  'Sin Categoría':ObtenerColores('Pastel1')[5]

}


function ObtenerFiltros(selector){
    DebugFecha();
    $.get(api+"/visualizacionesoncae/filtros/",{}).done(function( datos ) {
      OcultarEspera('#VisualizacionGeneral')
        console.dir('filtros')
        DebugFecha()
    console.dir(datos);

        if(datos&&datos.respuesta&&datos.respuesta.length){
          $('#'+selector).html('').append(
            $('<div>',{class:'contenedorFechas'}).append(
              $('<h4>',{class:'titularCajonSombreado textoAlineadoCentrado',text:'Cantidad de Contratos por Año'}),
              $('<div>',{class:'row'}).append(
                $('<div>',{class:'col-xs-6 col-sm-4 col-md-4 col-lg-4 col-xl-4'}).append(
                  $('<table>').append(
                    $('<tbody>').append(
                      $('<tr>').append(
                        $('<td>').append(
                          $('<i>',{class:'far fa-check-square textoColorPrimario',style:'font-size:50px'})
                        ),
                        $('<td>').append(
                          $('<div>',{
                            style:'padding-left:10px;text-align:left'
                          }).append(
                            $('<b>',{class:'textoColorGrisNormal',text:'Selecciona algún año del cual desees agrupar los contratos por institución.'})
                          )
                          )
                      )
                    )
                  )
                ),
                  $('<div>',{class:'col-xs-6 col-sm-4 col-md-4 col-lg-4 col-xl-4'}).append(
                    $('<table>').append(
                      $('<tbody>').append(
                        $('<tr>').append(
                          $('<td>').append(
                            $('<i>',{class:'far fa-question-circle textoColorPrimario',style:'font-size:50px'})
                          ),
                          $('<td>').append(
                            $('<div>',{
                              style:'padding-left:10px;text-align:left'
                            }).append(
                              $('<b>',{class:'textoColorGrisNormal',text:'Cada círculo representa un contrato, en donde el tamaño está dado por el monto y el color por el tipo de contrato.'})
                            )
                            )
                        )
                      )
                    )
                  ),
                    $('<div>',{class:'col-xs-6 col-sm-4 col-md-4 col-lg-4 col-xl-4'}).append(
                      $('<table>').append(
                        $('<tbody>').append(
                          $('<tr>').append(
                            $('<td>').append(
                              $('<i>',{class:'fas fa-mouse-pointer textoColorPrimario',style:'font-size:50px'})
                            ),
                            $('<td>').append(
                              $('<div>',{
                                style:'padding-left:10px;text-align:left'
                              }).append(
                                $('<b>',{class:'textoColorGrisNormal',text:' Haciendo click sobre una institución se pueden ver los contratos individuales en el gráfico de más abajo.'})
                              )
                              )
                          )
                        )
                      )
                    )
              
              )
            )
          );
          var arregloAnos=datos.respuesta.map(function(e){return ObtenerNumero(e.key_as_string);});

          arregloAnos=arregloAnos.sort(function(a, b){return a-b});

          arregloAnos.forEach(function(elemento) {
            $('#'+selector+' .contenedorFechas').append(
              $('<div>',{class:'botonGeneral fondoColorPrimario cursorMano'+((ObtenerNumero(decodeURIComponent(ObtenerValor('año')))==elemento)?' fondoColorSecundario':''),href:'javascript:void(0)',text:elemento, style:'color:white;margin:3px;display:inline-block;', fecha:elemento,
            on:{
              click:function(e){
                //$(e.currentTarget).attr('fecha')
                $(e.currentTarget).parent().find('.botonGeneral.fondoColorPrimario.cursorMano').removeClass('fondoColorSecundario');
                  $(e.currentTarget).addClass('fondoColorSecundario');
                var parametros=ObtenerJsonFiltrosAplicados({});
                  parametros['año']=$(e.currentTarget).attr('fecha');
                  if(parametros.institucion){
                    delete parametros.institucion;
                  }

                  if($('#PesosContratosGrafico').length){
                    $('#PesosContratosGrafico').empty();
                  }
                  $('#PesosContratos').html('');
                  $('#PesosContratos').hide();

                  AccederUrlPagina(parametros);
                  PushDireccion(AccederUrlPagina(parametros,true));
                  MostrarContratosGenerales();
              }
            }})
            )
          });
          
        }


       
        var parametros=ObtenerJsonFiltrosAplicados({})
          if(parametros.año&&parametros.institucion){
           // ObtenerContratos();
           MostrarContratosGenerales();
           MostrarContratosInstitucion(false,true);
          }else if(parametros.año){
          /*  $('#pesosContratosGraficos').html(
              '<div class="textoColorGris titularColor mt-5 ">Selecciona una Institución</div>'
              );*/
              MostrarContratosGenerales();
          }

      }).fail(function() {
          
          
        });

}


function ObtenerJsonFiltrosAplicados(parametros){

    if(Validar(ObtenerValor('institucion'))){
    parametros['institucion']=decodeURIComponent(ObtenerValor('institucion'));
    }
    if(Validar(ObtenerValor('año'))){
      parametros['año']=decodeURIComponent(ObtenerValor('año'));
    }
    parametros['masinstituciones']=1;
    

    return parametros;
  }

  function AccederUrlPagina(opciones,desUrl){
    var direccion=(window.location.pathname+ '?'+
    
    (ValidarCadena(opciones.año)? 'año='+encodeURIComponent(opciones.año): (ValidarCadena(ObtenerValor('año'))&&!desUrl?'&año='+ObtenerValor('año'):''))+
    (ValidarCadena(opciones.institucion)? '&institucion='+encodeURIComponent(opciones.institucion): (ValidarCadena(ObtenerValor('institucion'))&&!desUrl?'&institucion='+ObtenerValor('institucion'):''))
    );
    return direccion;
  }
  function PushDireccion(direccion){
    window.history.pushState({}, document.title,direccion);
  }
function MostrarSeleccionFecha(){
    var selector='FechasSeleccion';
    ObtenerFiltros(selector);
   /// <span class="botonGeneral fondoColorPrimario cargaEfecto" id="promedioMonto">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
}

function AbrirModalEmbeber(selector,titulo,codigo){
  if(!$('body #'+selector).length){
      $('body').append(
          $('<div>',{class:'modal fade',id:selector,tabindex:'-1',role:'dialog','aria-hidden':'true','aria-labelledby':'modalDescarga'}).append(
              $('<div>',{class:'modal-dialog',role:'document'}).append(
                  $('<div>',{class:'modal-content'}).append(
                      $('<div>',{class:'modal-header'}).append(
                          $('<span>',{class:'titularCajonSombreado'}).text(
                              titulo
                          )
                      ),
                      $('<div>',{class:'modal-body '}).append(
                          $('<div>',{style:'',class:'form-group'}).append(
                            $('<p>',{class:' mt-2',text:'Copia y pega este código en tu página web.'}),
                              $('<textarea >',{class:'form-control',rows:"3"}).text(codigo)
                          )
                          
                      ),
                      $('<div>',{class:'modal-footer'}).append(
                          $('<button>',{class:'botonGeneral fondoColorSecundario','data-dismiss':'modal',type:'button'}).text(
                              'Cerrar'
                          )
                      )
                  )
              )
          )
      )
  }
  $('body #'+selector).modal();
  
  

}



