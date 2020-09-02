
/**
 * @file api.js Este archivo se incluye en la sección de API del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**
 * Ejecuta las funciones una vez carge el dom.
 */
$(function(){
  DibujarElementosApi('#documentacionApi');
  AgregarToolTips();
})

/**
 * Arreglo de definicion de elementos para mostrar los elementos en la API del PCA.
 * @type {Object[]}
 */
var apiJSON=[
  {
    titulo : 'RELEASES',
    descripcionCorta : 'Muestra el Listado de los Releases en el Sistema',
    descripcionDetallada : 'Toda nueva información sobre un proceso de contratación se describe dentro de un release.<br>Los <b>releases</b> son inmutables – presentando información sobre un evento particular en la vida de un proceso de contrataciones. Los publicadores no deben de editar un <b>release</b> después de su publicación; un nuevo <b>release</b> puede crearse modificando el identificador <b>id</b> y la fecha <b>date</b> del <b>release</b>.',
    metodos : [
      {
        tipo : 'GET',
        descripcionCorta : 'Muestra Todos los Releases en el Sistema',
        url : '/api/v1/release/',
        urlReal:api+'/v1/release/',
        parametros : [
        ] 

      }, 
      {
        tipo : 'GET',
        descripcionCorta : 'Muestra los Releases de un Determinado OCID',
        url : '/api/v1/release/{OCID}',
        urlReal:api+'/v1/release/P2017-801-1-57-2017-12-22T08:53:42Z',
        parametros : [
          {
            nombre : 'OCID',
            valor: 'ocds-XXX-CCC-999',
            descripcion: 'El ID de Contrataciones Abiertas (ocid) es un identificador único global utilizado para unir los datos en todas las etapas de un proceso de contratación',
            tipoUso : 'path',
            tipoDato : 'string'
          }
        ] 

      }
    ]
  },
  {
    titulo : 'RECORDS',
    descripcionCorta : 'Muestra el Listado de los Records en el Sistema',
    descripcionDetallada : 'Aunque pueden haber múltiples <b>releases</b> sobre un proceso de contrataciones, debe haber un solo <b>record</b> por proceso de contrataciones, uniendo todas los <b>releases</b> disponibles para el proceso de contrataciones.<br> Un <b>record</b> <b>debe</b> contener: <ul><li>Un <b>OCID</b> </li><li>Una lista de <b>releases</b> sobre este proceso de contrataciones- puede ser dando un URL de dónde se puedan encontrar estos <b>releases</b>, o incorporando una copia completa del <b>release</b>.</li></ul> <br>Esto permite al registro funcionar como un índice de todas las entregas sobre un proceso de contrataciones.<br>Los <b>records</b> <b>deberían</b> contener:<ul><li>Una compiledRelease - la última versión de todos los campos del proceso de contratación abierta, representada usando el esquema de <b>release</b>.</li></ul><br>Los <b>records</b> <b>pueden</b> contener:<ul><li><b>versionedRelease</b>- contiene una historia de los datos en el <b>compiledRelease</b>, con todos los valores pasados de cualquier campo y el <b>release</b> de la que viene la información.</li></ul><br>El formato de <b>versioned release</b> está diseñado para apoyar el análisis de cómo han cambiado los datos entre <b>releases</b> y normalmente se generarán por los usuarios más que por los publicadores',
    metodos : [
      {
        tipo : 'GET',
        descripcionCorta : 'Muestra Todos los Records en el Sistema',
        url : '/api/v1/record/',
        urlReal: api+'/v1/record/',
        parametros : [
        ] 

      }, 
      {
        tipo : 'GET',
        descripcionCorta : 'Muestra el Record de un Determinado OCID',
        url : '/api/v1/record/{OCID}',
        urlReal:api+'/v1/record/ocds-lcuori-Zr5zxL-LPN-MPC-GAF-01-2015-2/',
        parametros : [
          {
            nombre : 'OCID',
            valor: 'ocds-XXX-CCC-999',
            descripcion: 'El ID de Contrataciones Abiertas (ocid) es un identificador único global utilizado para unir los datos en todas las etapas de un proceso de contratación',
            tipoUso : 'path',
            tipoDato : 'string'
          }
        ] 

      }
    ]
  }
];

/**Inserta el HTML de los elementos padre de la API
 * 
 * @param {string} contenedor -Query string que hace referencia al elemento html donde se añadiran los elementos html de la API
 */
function DibujarElementosApi(contenedor){
  var elementos=[];
  apiJSON.forEach(
    function (elemento){
      elementos.push(
        $('<div>',{class:'elementoApiContenedor abierto'}).append(
          $('<div>',{class:'fondoColorSecundario elementoApi posicionRelativa'}).append(
            $('<span>',{class:'tituloGrueso textoColorBlanco',text:elemento.titulo}),
            $('<span>',{class:'transparencia textoColorBlanco',text:' '+elemento.descripcionCorta}),
            $('<i>',{class:'fas fa-chevron-down textoColorBlanco p-1 cursorMano botonAbrirElementoApi transicion','toolTexto':'Expandir Métodos',on:{
              click:function(e){
              if($(e.currentTarget).parent().parent().hasClass('abierto')){
                $(e.currentTarget).parent().parent().removeClass('abierto');
              }else{
                $(e.currentTarget).parent().parent().addClass('abierto');
              }
             $(e.currentTarget).parent().parent().find('.elementoApiInformacion').slideToggle( "slow", function() {
            });
            }
          }})
          ),
          $('<div>',{class:'elementoApiInformacion'}).append(
            $('<div>',{class:'p-2'}).append(
              ObtenerMetodos(elemento)
            )
          )
        )
      )
    }
  );
  $(contenedor).append(
    elementos
  );
}

/**
 * Agrega la tabla de métodos de la API
 * @param {Object} elemento - Objeto del Arreglo de la API
 * @return {Object[]} -retorna un arreglo de elementos HTML
 */
function ObtenerMetodos(elemento){
  var elementos=[];
  elemento.metodos.forEach(
    function(metodo){
      elementos.push(
        $('<div>',{class:'metodoApi  abierto'}).append(
          $('<div>',{class:'fondoColorClaro metodo cursorMano',toolTexto:'Expandir',toolPosicion:'top-end',on:{
            click:function(e){
              if($(e.currentTarget).parent().hasClass('abierto')){
                $(e.currentTarget).parent().removeClass('abierto');
              }else{
                $(e.currentTarget).parent().addClass('abierto');
              }
             $(e.currentTarget).parent().find('.metoApiInformacion').slideToggle( "slow", function() {
            });
            }
          }}).append(
            $('<div>',{class:'row'}).append(
              $('<div>',{class:'col-12 col-sm-6 col-md-6 col-lg-6'}).append(
                $('<div>',{class:'metodoApiTipo tituloGrueso metodoApiTipoGet textoColorBlanco fondoColorGrisOscuroClaro',text:metodo.tipo}),
                $('<div>',{class:'enlaceMetodoApi'}).append(
                  $('<a>',{href:metodo.urlReal,text:metodo.url,target:'_blank'})
                )
              ),
              $('<div>',{class:'col-12 col-sm-6 col-md-6 col-lg-6 textoAlineadoDerecha'}).append(
                $('<div>',{class:'descripcionMetodoApi  transparencia pr-1',text:metodo.descripcionCorta})
              )
            )
          ),
          $('<div>',{class:'metoApiInformacion'}).append(
            $('<table>',{class:'tablaGeneral'}).append(
              $('<thead>').append(
                $('<tr>').append(
                  $('<th>',{ class:'textoColorGris',text:'Parametro'}),
                  $('<th>',{ class:'textoColorGris',text:'Valor'}),
                  $('<th>',{ class:'textoColorGris',text:'Descripción'}),
                  $('<th>',{ class:'textoColorGris textoColorGris',text:'Tipo de Parametro'}),
                  $('<th>',{ class:'textoColorGris textoColorGris',text:'Tipo de Dato'})
                )
              ),
              $('<tbody>',{class:''}).append(
                ObtenerParametros(metodo)
              )
            )
          )
          
        )
      )
    }
  );
  elementos.push(
    $('<div>',{class:'pt-3 textoAlineadoJustificado',html:elemento.descripcionDetallada})
  );
  return elementos;

}

/**
 * Agrega los elementos HTML de los parametros del método de la API
 * @param {Object} elemento - Objeto del Arreglo de la API
 * @return {Object[]} - retorna un arreglo de elementos HTML.
 */
function ObtenerParametros(elemento){
  var elementos=[];
  elemento.parametros.forEach(
    function(parametro){
      elementos.push($('<tr>').append(
        $('<td>',{'data-label':'Parametro',text:parametro.nombre}),
        $('<td>',{'data-label':'Valor',text:parametro.valor}),
        $('<td>',{'data-label':'Descripción',text:parametro.descripcion}),
        $('<td>',{'data-label':'Tipo de Parametro',text:parametro.tipoUso}),
        $('<td>',{'data-label':'Tipo de Dato',text:parametro.tipoDato})
      )
      );
    }
  );
  if(elementos.length==0){
    elementos.push($('<tr>').append(
      $('<td>',{text:'Este método no posee parámetros.','colspan':5})
    )
    );
  }
  return elementos;

}