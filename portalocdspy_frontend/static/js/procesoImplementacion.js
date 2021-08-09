
function DefinirElementosImplementacion(){
    if(procesoRecord.compiledRelease){
      if(procesoRecord.compiledRelease.contracts&&procesoRecord.compiledRelease.contracts.length){
        $('.implementacion.informacion').append(
          $('<div>',{class:'row'}).append(
            $('<h4>',{class:'col-6 col-sm-6 col-md-6 titularCajonSombreado',text:'Datos de Implementación'}),
            $('<div>',{class:'col-6 col-sm-6 col-md-6 textoAlineadoDerecha'}).append(
              $('<h4>',{class:'descargaIconos enLinea'}).append(
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.JSON'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.CSV'
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
          AdjuntarInformacionImplementacion(procesoRecord.compiledRelease.contracts),
          
        );
        
      }
    }
  }
  
  function AdjuntarInformacionImplementacion(contratos){
    var elementos=[];
    for(var i=0;i<contratos.length;i++){
      elementos.push(
        $('<div>',{class:'cajonSombreado contenedorDetalleProcesoDatos'}).append(
          $('<div>',{class:'contenedorProceso informacionProceso'}).append(
            (contratos[i].title||contratos[i].description?
                $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                    $('<table>').append(
                      $('<tbody>').append(
                            contratos[i].title?$('<tr>').append(
                            $('<td>',{class:'tituloTablaCaracteristicas',text:contratos[i].title,style:'color:#333'})):null,
                            contratos[i].description?$('<tr>').append(
                                $('<td>',{class:'',text:contratos[i].description,style:'color:#333'})):null
                      ))):null),
            $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
              $('<table>').append(
                $('<tbody>').append(
                  (contratos[i].buyer&&contratos[i].buyer.name ? 
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Comprador'}),
                      $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
                        $('<a>',{text:contratos[i].buyer.name,class:'enlaceTablaGeneral',href:'/comprador/'+contratos[i].buyer.id})
                      )
                    ) : null),
                  (contratos[i].suppliers&&contratos[i].suppliers.length ? 
                    ObtenerProveedoresContratos(contratos[i].suppliers) : null)
                  ,
                  (contratos[i].dateSigned ? 
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Firma'}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(contratos[i].dateSigned)})
                      ) : null),
                  $('<tr>').append(
                    $('<td>',{class:'tituloTablaCaracteristicas',text:'ID Proceso (OCID):'}),
                    $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.ocid})
                  )
                  )
                  )
            ),
            (contratos[i].value&&contratos[i].value.amount?
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
                              ValorMoneda(contratos[i].value.amount),
                              $('<span>',{class:'textoColorPrimario',text:contratos[i].value.currency})
                            )
  
                            
                          )
                        )
                      )
                    )
                    )
                    )
              ):null)
          )
        ),
          (procesoRecord.compiledRelease.parties&&procesoRecord.compiledRelease.contracts[i]&&procesoRecord.compiledRelease.contracts[i].suppliers ? 
          $('<div>',{class:'row mb-5 mt-5'}).append(
            ObtenerDatosContacto(procesoRecord.compiledRelease.parties,'supplier',['Unidad de Proveedor:','Proveedor:'])
          ) : null)
      );
      if(contratos[i].items&&contratos[i].items.length){
        $('.contrato.solicitados').append(
          $('<div>', {class:' cajonSombreado '}).append(
            $('<table>',{class:'tablaGeneral'}).append(
              $('<thead>').append(
                $('<tr>').append(
                  $('<th>',{text:'Id'}),
                  $('<th>',{text:'Clasificación'}),
                  $('<th>',{text:'Descripción'}),
                  /*$('<th>',{text:'Especificaciones'}),*/
                  $('<th>',{text:'Cantidad'}),
                  $('<th>',{text:'Precio'}),
                  $('<th>',{text:'Unidad'})
                )
              ),
              $('<tbody>').append(
                ObtenerItems(contratos[i].items)
              )
            )
          )
        )
      }
      if(contratos[i].documents&&contratos[i].documents.length){
        $('.contrato.documentos').append(
          $('<div>', {class:' cajonSombreado '}).append(
            $('<table>',{class:'tablaGeneral'}).append(
              $('<thead>').append(
                $('<tr>').append(
                  $('<th>',{text:'Nombre'}),
                  $('<th>',{text:'Descripción'}),
                  //$('<th>',{text:'Tipo'}),
                  $('<th>',{text:'Fecha'}),
                  $('<th>',{text:''})
                )
              ),
              $('<tbody>').append(
                ObtenerDocumentos(contratos[i].documents)
              )
            )
          )
        )
      }
    }
    return elementos;
  }
  
  function ObtenerProveedoresContratos(proveedores){
    var elementos=[]
    for(i=0;i<proveedores.length;i++){
      if(proveedores[i].name){
        elementos.push(
          $('<tr>').append(
          $('<td>',{class:'tituloTablaCaracteristicas',text:'Contrato de Proveedor'}),
          $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
            $('<a>',{text:proveedores[i].name,class:'enlaceTablaGeneral',href:'/proveedor/'+proveedores[i].id})
          )
        )
        );
        
      }
    }
    return elementos;
  }

  function VerificarImplementacion(){}