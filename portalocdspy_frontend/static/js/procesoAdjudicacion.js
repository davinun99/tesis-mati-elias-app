/**
 * @file procesoAdjudicacion.js Este archivo se incluye en la sección de Visualización de un Proceso de Contratación del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
var estadosAdjudicacion={
  'pending':{titulo:'Pendiente',descripcion:'Esta adjudicación se ha propuesto pero no ha entrado en vigor. Esto puede ser por un período de reflexión o algún otro proceso.'},
  'active':{titulo:'Activo',descripcion:'Se ha adjudicado y está en proceso.'},
  'cancelled':{titulo:'Cancelado',descripcion:'Esta adjudicación se ha cancelado.'},
  'unsuccessful':{titulo:'Sin Éxito',descripcion:'Esta adjudicación no pudo realizarse exitosamente. Si los artículos o detalles del proveedor se incluyen en la sección de adjudicación, estos limitan el alcance de la licitación fallida (e.j. si la adjudicación de artículos notados o una adjudicación al proveedor notado no fue exitosa, pero puede haber otras adjudicaciones exitosas para diferentes artículos en la oferta, o para distintos proveedores).'}
 }
function obtenerEstadoAdjudicacion(estado){
  if(estadosAdjudicacion[estado]){
    return estadosAdjudicacion[estado];
  }else{
    return {titulo:estado,descripcion:''};
  }
}
function DefinirElementosAdjudicacion(){
    if(procesoRecord.compiledRelease){
      if(procesoRecord.compiledRelease.awards&&procesoRecord.compiledRelease.awards.length){

      
        $('.adjudicacion.informacion').append(
          $('<div>',{class:'row'}).append(
            $('<h4>',{class:'col-6 col-sm-6 col-md-6 titularCajonSombreado',text:'Datos de la Adjudicación'}),
            $('<div>',{class:'col-6 col-sm-6 col-md-6 textoAlineadoDerecha'}).append(
              $('<h4>',{class:'descargaIconos enLinea'}).append(
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaJSONAdjudicacion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.JSON'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaCSVAdjudicacion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.CSV'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaXLSXAdjudicacion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.XLSX'
                )
              ),
              $('<h4>',{class:'enLinea mb-0 enLinea alineadoArriba'}).append(
                $('<a>',{href:'/preguntas'}).append(
                  $('<div>',{class:'textoAlineadoCentrado cursorMano botonAyuda transicion', id:'informacionTipoDatos'}).append(
                  
                    $('<i>',{class:'fas fa-question'})
                  
                  )
                )
                
              )
            )
          ),

          $('<nav>').append(
            $('<div>',{class:'nav nav-tabs',role:'tablist'}).append(


            $('<a>',{class:'nav-item nav-link active','data-toggle':'tab',role:'tab','aria-controls':'informacionTabAdjudicacion',href:'#informacionTabAdjudicacion','aria-selected':'true'}).append($('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Información'})),

            $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'itemsTabAdjudicacion',href:'#itemsTabAdjudicacion','aria-selected':'true'}).append($('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Artículos Solicitados'}))
            ,
            $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'documentosTabAdjudicacion',href:'#documentosTabAdjudicacion','aria-selected':'true'}).append($('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Documentos'}))
            )
          //AdjuntarInformacionAdjudicacion(procesoRecord.compiledRelease.awards),
          
        ),
              
      $('<div>',{class:'tab-content cajonSombreado',id:'contenedorTabAdjudicacion'}).append(
        $('<div>',{class:'tab-pane fade show active',role:'tabpanel','aria-labelledby':'informacionTabAdjudicacion',id:'informacionTabAdjudicacion'}).append(
          $('<div>',{class:'contenedorProceso informacionProceso'}).append(
          $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
            $('<table>').append(
              $('<tbody>').append(
                procesoRecord.compiledRelease.buyer&&procesoRecord.compiledRelease.buyer.name ? 
                $('<tr>').append(
                  $('<td>',{class:'tituloTablaCaracteristicas',text:'Comprador:',toolTexto:"buyer"}),
                  $('<td>',{class:'contenidoTablaCaracteristicas'}).append(ObtenerElementosParte(procesoRecord.compiledRelease.buyer.id))
                ) : null
                )
              )
            )
          ),
          procesoRecord.compiledRelease.awards?AnadirTablaAdjudicacion(procesoRecord.compiledRelease.awards):null
          ),
          $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'itemsTabAdjudicacion',id:'itemsTabAdjudicacion'}).append(

            ),
          $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'documentosTabAdjudicacion',id:'documentosTabAdjudicacion'}).append(

            )
        )
        
        )

        var items = ObtenerTodoslosItemsAdjudicacion();
    if( items.length){
      $('#itemsTabAdjudicacion').append(
        $('<div>', {class:''}).append(
          $('<table>',{class:'tablaGeneral'}).append(
            $('<thead>').append(
              $('<tr>').append(
                $('<th>',{text:'Id', toolTexto:'awards[n].items[n].classification.id'}),
                $('<th>',{text:'Id Adjudicación', toolTexto:'awards[n].id'}),
                $('<th>',{text:'Clasificación', toolTexto:'awards[n].items[n].classification.scheme'}),
                $('<th>',{text:'Descripción', toolTexto:'awards[n].items[n].classification.description'}),
                /*$('<th>',{text:'Especificaciones'}),*/
                $('<th>',{text:'Cantidad', toolTexto:'awards[n].items[n].quantity'}),
                $('<th>',{text:'Precio', toolTexto:'awards[n].items[n].unit.value.amount'}),
                $('<th>',{text:'Unidad', toolTexto:'awards[n].items[n].unit.name'})
              )
            ),
            $('<tbody>').append(
              items
            )
          )
        )
      )
    }else{
      $('#itemsTabAdjudicacion').html('<h4 class="titularColor textoColorPrimario mt-3">Esta etapa no posee items</h4>')
    }

    var documentos=ObtenerTodoslosDocumentosAdjudicacion();
    if(documentos.length){
      $('#documentosTabAdjudicacion').append(
        $('<div>', {class:''}).append(
          $('<table>',{class:'tablaGeneral'}).append(
            $('<thead>').append(
              $('<tr>').append(
                $('<th>',{text:'Id Adjudicación'}),
                $('<th>',{text:'Nombre', toolTexto:'awards[n].documents[n].title'}),
                $('<th>',{text:'Descripción', toolTexto:'awards[n].documents[n].description'}),
                //$('<th>',{text:'Tipo', toolTexto:'awards[n].documents[n].documentType'}),
                $('<th>',{text:'Fecha', toolTexto:'awards[n].documents[n].datePublished'}),
                $('<th>',{text:''})
              )
            ),
            $('<tbody>').append(
              documentos
            )
          )
        )
      )
    }else{
      $('#documentosTabAdjudicacion').html('<h4 class="titularColor textoColorPrimario mt-3">Esta etapa no posee documentos</h4>')
    
    }
        
      }
    }
  }

/*Mostrar el Contenedor Inicial de la Informacion de Adjudicacion del Proceso de Contratacion */
function AdjuntarInformacionAdjudicacion(adjudicaciones){
  var elementos=[];
  for(var i=0;i<adjudicaciones.length;i++){
    elementos.push(
      $('<div>',{class:'cajonSombreado contenedorDetalleProcesoDatos'}).append(
        $('<div>',{class:'contenedorProceso informacionProceso'}).append(
          (adjudicaciones[i].title||adjudicaciones[i].description?
              $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                  $('<table>').append(
                    $('<tbody>').append(
                          adjudicaciones[i].title?$('<tr>').append(
                          $('<td>',{class:'tituloTablaCaracteristicas',text:adjudicaciones[i].title,style:'color:#333'})):null,
                          adjudicaciones[i].description?$('<tr>').append(
                              $('<td>',{class:'',text:adjudicaciones[i].description,style:'color:#333'})):null
                    ))):null),
          $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
            $('<table>').append(
              $('<tbody>').append(
                (procesoRecord.compiledRelease.buyer&&procesoRecord.compiledRelease.buyer.name&&procesoRecord.compiledRelease.parties ? 
                  ObtenerCompradores(procesoRecord.compiledRelease.parties,procesoRecord.compiledRelease.buyer) : null),
                (adjudicaciones[i].suppliers&&adjudicaciones[i].suppliers.length ? 
                  ObtenerProveedoresAdjudicados(adjudicaciones[i].suppliers) : null),
                (adjudicaciones[i].status ? 
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Estado'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:obtenerEstadoAdjudicacion(adjudicaciones[i].status).titulo})
                  ) : null),
                (adjudicaciones[i].date ? 
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Adjudicación'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(adjudicaciones[i].date)})
                  ) : null),
                adjudicaciones[i].contractPeriod ? ([
                  adjudicaciones[i].contractPeriod.startDate ? 
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio de Adjudicación del Contrato'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(adjudicaciones[i].contractPeriod.startDate)})
                  ) : null,
                  adjudicaciones[i].contractPeriod.endDate ?
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización de Adjudicación del Contrato'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(adjudicaciones[i].contractPeriod.endDate)})
                  ) : null,
                  adjudicaciones[i].contractPeriod.maxExtentDate ?
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Máxima para la Renovación o Extensión de Este Período'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(adjudicaciones[i].contractPeriod.maxExtentDate)})
                  ) : null,
                  adjudicaciones[i].contractPeriod.durationInDays ?
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Duración en Días'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:adjudicaciones[i].contractPeriod.durationInDays})
                  ) : null             
                ]
                ) : null,
                (adjudicaciones[i].id ? 
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'ID Adjudicación'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:adjudicaciones[i].id})
                  ) : null)
              )
            )
          ),
          (adjudicaciones[i].value&&!$.isEmptyObject(adjudicaciones[i].value)?
            $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
              $('<table>',{class:'tablaAncho'}).append(
                $('<tbody>').append(
                  $('<tr>').append(
                    $('<td>',{class:'textoAlineadoDerecha'}).append(
                      $('<div>',{
                        class:'montoTotalProceso pr-3'
                      }).append(
                        $('<img>',{class:'imagenMonto mr-1',src:'/static/img/otros/monedasHonduras.png'}),
                        $('<div>',{class:'contenedorMonto procesoMonto'}).append(
                          $('<div>',{class:'textoColorGris',text:'Monto'}),
                          $('<div>',{class:'valorMonto'}).append(
                            ValorMoneda(adjudicaciones[i].value.amount),
                            $('<span>',{class:'textoColorPrimario',text:adjudicaciones[i].value.currency})
                          )
    
                          
                        )
                      )
                    )
                  )
                  )
                  )
            ):null)
        )
      )
    
    );/*
    if(adjudicaciones[i].amendments){
      elementos=elementos.concat(ObtenerEnmiendasAdjudicacion(adjudicaciones[i].amendments,adjudicaciones[i]));
    }
    */
    /*if(adjudicaciones[i].documents&&adjudicaciones[i].documents.length){
      $('#documentosTabAdjudicacion').append(
        $('<div>', {class:' cajonSombreadox '}).append(
          $('<table>',{class:'tablaGeneral'}).append(
            $('<thead>').append(
              $('<tr>').append(
                $('<th>',{text:'Nombre'}),
                $('<th>',{text:'Descripción'}),
                $('<th>',{text:'Tipo'}),
                $('<th>',{text:'Fecha'}),
                $('<th>',{text:''})
              )
            ),
            $('<tbody>').append(
              ObtenerDocumentos(adjudicaciones[i].documents)
            )
          )
        )
      )
    }else{
      $('#documentosTabAdjudicacion').html('<h4 class="titularColor textoColorPrimario mt-3">Esta etapa no posee documentos</h4>')
    
    }*/
  }
  
  return elementos;
}

function ObtenerProveedoresAdjudicados(proveedores){
  var elementos=[]
  for(i=0;i<proveedores.length;i++){
    if(proveedores[i].name){
      elementos.push(
        $('<tr>').append(
        $('<td>',{class:'tituloTablaCaracteristicas',text:'Proveedor Adjudicado'}),
        $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
          $('<a>',{text:proveedores[i].name,class:'enlaceTablaGeneral',href:'/proveedor/'+proveedores[i].id})
        )
      )
      )
      
    }
  }
  return elementos;
}

function ObtenerEnmiendasAdjudicacion(enmiendas,adjudicacion){
  var elementos=[];
  for(var i=0;i<enmiendas.length;i++){
    if(enmiendas[i].id){
      elementos.push(
        $('<div>',{class:'row mt-3'}).append(
          $('<h4>',{class:'col-12 col-sm-12 col-md-12 titularCajonSombreado',text:'Enmienda Adjudicación '+adjudicacion.id})
          ),
        $('<div>', {class:'cajonSombreado contenedorDetalleProcesoDatos mt-1'}).append(
          $('<div>',{class:'contenedorProceso informacionProceso'}).append(
            $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
              $('<table>').append(
                $('<tbody>').append(
                  (enmiendas[i].date?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha:'}),
                      $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoJustificado',text:ObtenerFecha(enmiendas[i].date)})
                    )
                    :null),
                  (enmiendas[i].rationale?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Razón:'}),
                      $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoJustificado',text:enmiendas[i].rationale})
                    )
                    :null),
                  (enmiendas[i].description?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Descripción:'}),
                      $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoJustificado',text:enmiendas[i].description})
                    )
                    :null),
                  (enmiendas[i].id?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'ID'}),
                      $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoJustificado',text:enmiendas[i].id})
                    )
                    :null)
                )
              )
            )
          )
        )
      );
    }
  }
  return elementos;
}

function ObtenerContratos(id){
  var arreglo=[];
  if(procesoRecord&&procesoRecord.compiledRelease&&procesoRecord.compiledRelease.contracts){
    var contratos=procesoRecord.compiledRelease.contracts;
   
  for(var i = 0; i < contratos.length;i++){
    if(contratos[i].awardID==id){
      arreglo.push(contratos[i]);
    }else{
    }
  }
  }
  return arreglo;
}
function BotonesContratosAdjudicacion(id){
  var contratos=ObtenerContratos(id);
  var elementos=[];
  for( var i = 0; i < contratos.length ; i++){
    elementos.push($('<button>',{style:'border:none;outline:0;margin-bottom:5px;',class:'botonGeneral fondoColorSecundario',onclick:'location.href="/proceso/'+encodeURIComponent( procesoRecord.ocid)+'/?contrato='+encodeURIComponent(contratos[i].id)+'"' }).text('Ver Contrato'))
  }
  return elementos;
}

function AnadirTablaAdjudicacion(adjudicaciones){
  return $('<div>', {class:'contenedorTablaCaracteristicas'}).append(
    $('<div>', {class:'contenedorProceso informacionProceso'}).append(
      $('<table>',{class:'tablaGeneral'}).append(
        $('<thead>').append(
          $('<tr>').append(
            $('<th>',{text:'Id Adjudicación', toolTexto : "awards[n].id"}),
            $('<th>',{text:'Proveedores', toolTexto : "awards[n].suppliers"}),
            $('<th>',{text:'Descripción', toolTexto : ""}),
            /*$('<th>',{text:'Especificaciones'}),*/
            $('<th>',{text:'Fecha de adjudicación', toolTexto : "awards[n].date"}),
            $('<th>',{text:'Monto', toolTexto : "awards[n].value.amount"}),
            $('<th>',{text:'Contratos', toolTexto : "contracts[n].awardID igual a awards[n].id "})
          )
        ),
        $('<tbody>').append(
          ObtenerAdjudicaciones(adjudicaciones)
        )
      )
    )
    
  )
}


function ObtenerAdjudicaciones(adjudicaciones){
  var elementos=[];
  for(var i=0;i<adjudicaciones.length;i++){
    elementos.push(
      $('<tr>').append(
        $('<td>',{'data-label':'Id Adjudicación',text:adjudicaciones[i].id}),
        $('<td>',{'data-label':'Proveedores'}).append(ObtenerProveedores(adjudicaciones[i].suppliers)),
        $('<td>',{'data-label':'Descripción',text:'No Disponible'}),
        /*$('<td>',{text:items[i].description}),*/
        $('<td>',{'data-label':'Fecha de adjudicación'}).append(
          adjudicaciones[i].date?ObtenerFecha(adjudicaciones[i].date):'No Disponible'
        ),
        $('<td>',{'data-label':'Monto'}).append(
          (adjudicaciones[i].value&&Validar(adjudicaciones[i].value.amount))?(
          $('<span>',{class: 'textoColorPrimario'}).text(ValorMoneda(adjudicaciones[i].value.amount) ),
          $('<span>',{class: 'textoColorPrimario'}).text(adjudicaciones[i].value.currency)):$('<span>',{class:'textoColorGris',text:'No Disponible'})
        ),
        $('<td>',{'data-label':'Contratos'}).append(
          BotonesContratosAdjudicacion(adjudicaciones[i].id)
        )
        
      )
    )
  }
  return elementos;
}



function ObtenerItemsAdjudicaciones(items,id){
  var elementos=[];
  if(items){
    for(var i=0;i<items.length;i++){
      elementos.push(
        $('<tr>').append(
          $('<td>',{'data-label':'id',text:((items[i].classification&&items[i].classification.id)?items[i].classification.id:null)}),
          $('<td>',{'data-label':'Id Adjudicación',text:id}),
          $('<td>',{'data-label':'Clasificación',text:((items[i].classification&&items[i].classification.scheme)?items[i].classification.scheme:null)}),
          $('<td>',{'data-label':'Descripción',text:((items[i].classification&&items[i].classification.description)?items[i].classification.description:null)}),
          /*$('<td>',{text:items[i].description}),*/
          $('<td>',{'data-label':'Cantidad',text:items[i].quantity}),
          $('<td>',{'data-label':'Precio',text:((items[i].unit&&items[i].unit.value&&items[i].unit.value.amount)?items[i].unit.value.amount:null)}),
          $('<td>',{'data-label':'Unidad',text:((items[i].unit&&items[i].unit.name)?items[i].unit.name:null)})
          
        ),
        $('<tr>').append(
          $('<th>',{text:'Especificaciones',style:'vertical-align:top'}),
          $('<td>',{'colspan':4,text:items[i].description,class:'textoAlineadoJustificado'})
        )
      )
    }
  }
  
  return elementos;
}

function ObtenerTodoslosItemsAdjudicacion(){
  var elementos=[];
  if(procesoRecord&&procesoRecord.compiledRelease&&procesoRecord.compiledRelease.awards){
    for(var i=0;i<procesoRecord.compiledRelease.awards.length;i++){
      elementos=elementos.concat(ObtenerItemsAdjudicaciones(procesoRecord.compiledRelease.awards[i].items,procesoRecord.compiledRelease.awards[i].id));
    }
  }
  console.dir('items')
  console.dir(elementos)
  return elementos;
}

function ObtenerTodoslosDocumentosAdjudicacion(){
  var elementos=[];
  if(procesoRecord&&procesoRecord.compiledRelease&&procesoRecord.compiledRelease.awards){
    for(var i=0;i<procesoRecord.compiledRelease.awards.length;i++){
      elementos=elementos.concat(ObtenerDocumentosAdjudicaciones(procesoRecord.compiledRelease.awards[i].documents,procesoRecord.compiledRelease.awards[i].id));
    }
  }
  return elementos;
}

function ObtenerDocumentosAdjudicaciones(documentos,id){
  var elementos=[];
  if(documentos){
    for(var i=0;i<documentos.length;i++){
      elementos.push(
        $('<tr>').append(
          $('<td>',{'data-label':'Id Adjudicación',text:id}),
          $('<td>',{'data-label':'Nombre',text:documentos[i].title}),
          $('<td>',{'data-label':'Descripción',text:documentos[i].description}),
          //$('<td>',{'data-label':'Tipo',text:TraduceTexto(documentos[i].documentType)}),
          $('<td>',{'data-label':'Fecha',text:((documentos[i].datePublished)?ObtenerFecha(documentos[i].datePublished):null)}),
          $('<td>',{class:'textoAlineadoDerecha','data-label':''}).append(
            $('<h4>',{class:'descargaIconos'}).append(
              $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion'}).append(
                $('<a>',{href:documentos[i].url,download:'a',class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',target:'_blank'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                '&nbsp;'+ObtenerExtension(documentos[i].url)
                )
                
              )
            )
          )
          
        )
      );
    }
  }
  
  return elementos;
}