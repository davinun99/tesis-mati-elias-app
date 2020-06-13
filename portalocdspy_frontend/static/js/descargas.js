/**
 * @file descargas.js Este archivo se incluye en la sección de descargas del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**contenedor donde se desea crear la tabla
 * @type {string}
 */
var selector='#descargas';

/**
 * Arreglo de datos donde estan la informacion de las descargas
 * @type {Object[]}
 */
var descargasGenerales=[];

/**
 * Arreglo de campos para generar la tabla de descargas
 * @type {Object[]}
 */
var camposTabla=[
  {
    id:'publicador',
    etiqueta:'Publicador',
    tipo:'texto'
  },{
    id:'fuente',
    etiqueta:'Fuente',
    tipo:'texto'
  },{
    id:'fecha',
    etiqueta:'Año',
    tipo:'texto',
    clase:'textoAlineadoCentrado'
  },
  {
    id:'mes',
    etiqueta:'Mes',
    tipo:'texto',
    clase:'textoAlineadoCentrado'

  }, {
  id:'urls',
  etiqueta:'',
  tipo:'descarga',
  formato:function(descarga){
    return $('<div>',{class:'contenedorClasesArchivos'}).append(
        descarga.json?$('<a>',{href:descarga.json,target:'_blank'}).append(

          $('<div>',{class:'textoColorSecundario textoAlineadoDerecha p-1 cursorMano transparencia enlaceArchivoDescarga transicion titularColor ',style:'display:inline-block;font-size:14px'}).append(
            $('<i>',{class:'fas fa-file-download'}),
            ' .JSON'
          )
        ):null,
        descarga.csv?$('<a>',{href:descarga.csv,target:'_blank'}).append(
          $('<div>',{class:'textoColorSecundario textoAlineadoDerecha p-1 cursorMano transparencia enlaceArchivoDescarga transicion titularColor',style:'display:inline-block;font-size:14px'}).append(
            $('<i>',{class:'fas fa-file-download'}),
            ' .CSV'
          )
        ):null,
        $('<br>'),

        descarga.xlsx?$('<a>',{href:descarga.xlsx,target:'_blank'}).append(
          $('<div>',{class:'textoColorSecundario textoAlineadoDerecha p-1 cursorMano transparencia enlaceArchivoDescarga transicion titularColor',style:'display:inline-block;font-size:14px'}).append(
            $('<i>',{class:'fas fa-file-download'}),
            ' .XLSX'
          )
        ):null,
        descarga.md5?$('<a>',{href:descarga.md5,target:'_blank'}).append(
          $('<div>',{class:'textoColorSecundario textoAlineadoDerecha p-1 cursorMano transparencia enlaceArchivoDescarga transicion titularColor',style:'display:inline-block;font-size:14px'}).append(
            $('<i>',{class:'fas fa-file-download'}),
            ' .MD5'
          )
        ):null
      );
  }
  }
];

/**
 * Inicializa la tabla de descargas y llama al método para obtener los datos
 */
$(function(){
  InicializarTabla(selector,'Descargas');
  ObtenerDescargas(selector);
});

/**
 * Inicializa la tabla de descargas
 * @param {string} - contenedor html donde estará la tabla
 * @param {string} -titulo de la tabla
 */
function InicializarTabla(selector,titulo){
$(selector).append(
  $('<div>',{class:'cajonSombreado'}).append(
    $('<div>',{class:'row'}).append(
      $('<div>',{class:'col-md-12 mt-1'}).append(
        
        )
    ),
    $('<div>',{class:'row mt-1 mb-3'}).append(
      $('<div>',{class:'col-lg-6'}).append(
        $('<h1>',{class:'textoColorPrimario mt-3 tituloDetalleProceso'}).text(titulo)
      ),
      $('<div>',{class:'col-lg-6 textoAlineadoDerecha'}).append(
        $('<h5>',{class:'tituloTablaGeneral textoColorTitulo mt-3 mb-0 enLinea'}).html('Mostrar&nbsp;'),
        $('<select>',{class:'campoSeleccion ancho70 mt-3 enLinea cantidadResultados',on:{change:function(e){
          AgregarResultados(1);
        }}}).append(
          $('<option>',{value:5,text:'5'}),
          $('<option>',{value:10,text:'10'}),
          $('<option>',{value:20,text:'20'}),
          $('<option>',{value:50,text:'50'}),
          $('<option>',{value:100,text:'100'})
        ),
        $('<h5>',{class:'tituloTablaGeneral textoColorTitulo mt-3 mb-0 enLinea'}).html('&nbsp;Resultados')
        )
    ),
    $('<table>',{class:'tablaGeneral mostrarEncabezados'}).append(
      $('<thead>',{}).append(
        $('<tr>').append(
          ObtenerEncabezados(camposTabla)
        )
      ),
      $('<tbody>',{ class:'resultadosTabla'}).append(
        $('<tr>').append(
          $('<td>',{style:'height:300px;position:relative',colspan:ObtenerEncabezados(camposTabla).length,class:'contenedorCargando'})
        )
      )

),
    $('<div>',{class:'row'}).append(
      $('<div>',{class:'col-md-12 textoAlineadoCentrado'}).append(
        $('<h5>',{class:'tituloTablaGeneral textoColorPrimario mt-3 mb-0 mostrandoTablaGeneral'})
      ),
      $('<div>',{class:'col-md-12 textoAlineadoCentrado'}).append(
        $('<nav>',{class:'navegacionTablaGeneral'})
        )
    )
  )
);
}

/**
 * Obtiene los elementos html de los encabezados de la tabla
 * @param {Object[]} campos -Arreglo de columnas
 */
function ObtenerEncabezados(campos){
  var elementos=[];
  campos.forEach(function(campo){
      elementos.push( ObtenerCampo(campo));
        });
      return elementos;       
}

/**
 * Obtiene el elemento html correspondiente a una columna
 * @param {Object} campo - Objeto de campo de la tabla
 */
function ObtenerCampo(campo){
  switch(campo.tipo){
    case 'descarga':
        return (
          $('<th>',{class:'textoAlineadoCentrado campoFiltrado'}).append(
            $('<table>',{class:'alineado'}).append(
              $('<tbody>').append(
                $('<tr>').append(
                  $('<td>').append(
                    $('<span>').append(
                      $('<span>',{text:campo.etiqueta})
                    )
                  )
                )
              )
              
            )
          )
        );
      break;
    default:
        return (
          $('<th>',{class:'textoAlineadoCentrado campoFiltrado'}).append(
            $('<table>',{class:'alineado'}).append(
              $('<tbody>').append(
                $('<tr>').append(
                  $('<td>').append(
                    $('<span>').append(
                      $('<span>',{text:campo.etiqueta})
                    )
                  )
                )
              )
             
            ),
            $('<input>',{class:'campoBlancoTextoSeleccion', placeholder:campo.etiqueta,identificador:campo.id, type:'text',on:{
              change:function(e){
                AgregarResultados(1);
              }
            }
            })
          )
        );
        break;
      }
}

/**
 * Consulta el método para obtener las descargas disponibles.
 * @param {string} selector -Cadena correspondiente al identificador de un elemento html
 */
function ObtenerDescargas(selector){
  DebugFecha();
  MostrarEspera(selector+' .contenedorCargando');

  $.get(api+"/v1/descargas/",function(datos){
      descargasGenerales=[];
      DebugFecha();
      OcultarEspera(selector+' .contenedorCargando');
      if(datos&&!$.isEmptyObject(datos)){
        $.each(datos,function(indice,valor){
          if(valor&&!$.isEmptyObject(valor.urls)){
            descargasGenerales.push(
              { 
                "fuente":valor.sistema,
                "fecha":valor.year,
                urls:{
                  "json":valor.urls.json,
                  "md5":valor.urls.md5,
                  "csv":valor.urls.csv,
                  "xlsx":valor.urls.xlsx,
                },
               
                "mes":ObtenerMesNombre(valor.month),
                "publicador":valor.publicador
             }
            );
            
          }
        })
      };
      descargasGenerales=descargasGenerales.sort(function(a, b){return ((a.fecha > b.fecha) ? 1 : -1)});
      AgregarResultados(1);
  }).fail(function(error) {
  console.dir('ERROR GET');
  console.dir(error);
  
});
}


/**
 * Obtiene un arreglo de datos con los resultados filtrados por los campos
 */
function ObtenerResultadosFiltrados(){
  var comparar=[];
  $(selector+' .campoFiltrado input.campoBlancoTextoSeleccion').each(function(i,campo){
    if(ValidarCadena($(campo).val())){
      comparar.push(
        $(campo).attr('identificador')
      );
    }
  });
  return descargasGenerales.filter(function(elemento){
    if(comparar.length==0){
      return true;
    }
    for(let i=0;i<comparar.length;i++){
      var termino=$(selector+' .campoFiltrado input.campoBlancoTextoSeleccion[identificador="'+comparar[i]+'"]').val().trim();
      if(!ContenerCadena(elemento[comparar[i]],termino)){
        return false;
      }
    }
    return true;
  });
}

/**
 * Obtiene el valor actual de la cantidad de resultados a mostrar por página
 */
function ObtenerResultadosPagina(){
  return ObtenerNumero($(selector+' .cantidadResultados').val())?ObtenerNumero($(selector+' .cantidadResultados').val()):5;
}

/**
 * Esta función muestra los resultados filtrados
 * @param {number} pagina - Número de página
 */
function AgregarResultados(pagina){
  var filtrados=ObtenerResultadosFiltrados();
  $(selector +' .resultadosTabla').html('');
  $.each(filtrados,function(i,descarga){
    if(!(i<=((ObtenerResultadosPagina()*pagina)-1)&&i>=((ObtenerResultadosPagina()*pagina)-ObtenerResultadosPagina()))){
      return;
    }
    $(selector+' .resultadosTabla').append(
      $('<tr>').append(
        ObtenerFila(descarga)
      )
    );
  });
  if(filtrados.length==0){
    $(selector+' .resultadosTabla').append(
      $('<tr>',{style:''}).append(
        $('<td>',{'data-label':'','colspan':5}).append(
          $('<h4>',{class:'titularColor textoColorPrimario mt-3 mb-3'}).text('No se Encontraron Descargas')
        ))
    );
  }

  MostrarPaginacion(pagina);

}

/**
 * Devuelve un arreglo html con todos los elementos html td por cada columna
 * @param {Object} dato - Objeto que corresponde a los datos de una descarga
 * @return {Object[]}
 */
function ObtenerFila(dato){
  var elementos=[];

  camposTabla.forEach(
    function(campo){
      elementos.push(
        $('<td>', { 'data-label': campo.etiqueta, class: campo.clase?campo.clase:'' }).html(
            campo.formato?campo.formato(dato[campo.id]):dato[campo.id]
          )
      );
    }
  );  
  
  return elementos;
}

/**
 * Muestra los elementos HTML correspondiente a la paginación
 * @param {number} pagina - Número de página que se piensa mostrar
 */
function MostrarPaginacion(pagina){
  $(selector+' .mostrandoTablaGeneral').html(
    'Mostrando '+(ObtenerResultadosPagina()*pagina-ObtenerResultadosPagina()+1)+' a '+(Math.ceil(ObtenerResultadosFiltrados().length/ObtenerResultadosPagina())==pagina?ObtenerResultadosFiltrados().length:ObtenerResultadosPagina()*pagina)+' de '+ObtenerResultadosFiltrados().length+' Descargas'
  )
  var paginacion=ObtenerPaginacion(pagina, Math.ceil(ObtenerResultadosFiltrados().length/ObtenerResultadosPagina()))  ;
  $(selector+' .navegacionTablaGeneral').html('');
  if(pagina>1){
    $(selector+' .navegacionTablaGeneral').append(
      $('<a href="javascript:AgregarResultados('+(pagina-1)+')" class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-left"></i></span></a>')
    );
  }
  
  for(var i=0; i<paginacion.length;i++){
    if(paginacion[i]=='...'){
      $(selector+'.navegacionTablaGeneral').append(
        $('<a href="javascript:void(0)" class="numerosNavegacionTablaGeneral numeroNormalNavegacionTablaGeneral">').append($('<span>').text(paginacion[i]))
      );
    }else{
      $(selector+' .navegacionTablaGeneral').append(
        $('<a href="javascript:AgregarResultados('+paginacion[i]+')" class="numerosNavegacionTablaGeneral '+((paginacion[i]==pagina)?'current':'')+'">').append($('<span>').text(paginacion[i]))
      );
    }
  }
  if(pagina<=(Math.ceil(ObtenerResultadosFiltrados().length/ObtenerResultadosPagina())-1)){
    $(selector+' .navegacionTablaGeneral').append(
      $('<a href="javascript:AgregarResultados('+(pagina+1)+')" class="numerosNavegacionTablaGeneral"><span><i class="fa fa-angle-right"></i></span></a>')
    );
  }
  
}