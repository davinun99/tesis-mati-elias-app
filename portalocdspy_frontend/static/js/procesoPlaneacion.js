/**
 * @file procesoPlaneacion.js Este archivo se incluye en la sección de Visualización de un Proceso de Contratación del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
function DefinirElementosPlaneacion(){
    if(procesoRecord.compiledRelease){
      if(procesoRecord.compiledRelease.planning){
        $('.planificacion.informacion').append(
          $('<div>',{class:'row'}).append(
            $('<h4>',{class:'col-6 col-sm-6 col-md-6 titularCajonSombreado',text:'Datos de la Planificación'}),
            $('<div>',{class:'col-6 col-sm-6 col-md-6 textoAlineadoDerecha'}).append(
              $('<h4>',{class:'descargaIconos enLinea'}).append(
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaJSONPlaneacion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.JSON'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaCSVPlaneacion'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.CSV'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaXLSXPlaneacion'}).append(
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
          
          $('<div>',{class:'cajonSombreadox contenedorDetalleProcesoDatos'}).append(
            $('<nav>').append(
              $('<div>',{class:'nav nav-tabs',role:'tablist'}).append(
              $('<a>',{class:'nav-item nav-link active','data-toggle':'tab',role:'tab','aria-controls':'informacionTabPlaneacion',href:'#informacionTabPlaneacion','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Información'})
              ),
              $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'documentosTabPlaneacion',href:'#documentosTabPlaneacion','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Documentos'})
              )
                )
            ),
            $('<div>',{class:'tab-content cajonSombreado',id:'contenedorTabPlaneacion'}).append(
              $('<div>',{class:'tab-pane fade show active',role:'tabpanel','aria-labelledby':'informacionTabPlaneacion',id:'informacionTabPlaneacion'}).append(
                $('<div>',{class:'contenedorProceso informacionProceso'}).append(

                  (procesoRecord.compiledRelease.planning.rationale?
                      $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                          $('<table>').append(
                            $('<tbody>').append(
                              $('<tr>').append(
                                  $('<td>',{class:'tituloTablaCaracteristicas',text:procesoRecord.compiledRelease.planning.rationale,style:'color:#333',toolTexto:"planning.rationale"}))
                            ))):null),
                $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                  $('<table>').append(
                    $('<tbody>').append(
                      /*Consultas*/
                      (procesoRecord.compiledRelease.buyer&&procesoRecord.compiledRelease.buyer.name&&procesoRecord.compiledRelease.parties ?
                        $('<tr>').append(
                          $('<td>',{class:'tituloTablaCaracteristicas',text:'Comprador:',toolTexto:"buyer"}),
                          $('<td>',{class:'contenidoTablaCaracteristicas'}).append(ObtenerElementosParte(procesoRecord.compiledRelease.buyer.id))
                        ) : null),
/*
                        procesoRecord.compiledRelease.tender&&procesoRecord.compiledRelease.tender.enquiryPeriod ? ([
                        procesoRecord.compiledRelease.tender.enquiryPeriod.startDate ? 
                        $('<tr>').append(
                          $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio de Consultas'}),
                          $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.enquiryPeriod.startDate)})
                        ) : null,
                        procesoRecord.compiledRelease.tender.enquiryPeriod.endDate ?
                        $('<tr>').append(
                          $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización de Consultas'}),
                          $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.enquiryPeriod.endDate)})
                        ) : null
                      ]
                      ) : null
                      ,*/
                      (procesoRecord.compiledRelease.planning.milestones ? 
                      ObtenerHitosRelacionados(procesoRecord.compiledRelease.planning.milestones)
                      : null),
                      (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.id?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'ID partida presupuestaria:',toolTexto:"planning.budget.id"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.planning.budget.id})
                      ):null),
                      (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.description?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fuente del presupuesto:',toolTexto:"planning.budget.description"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.planning.budget.description})
                      ):null),
                      (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.project?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Proyecto de financiamiento:',toolTexto:"planning.budget.project"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.planning.budget.project})
                      ):null),
                      (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.projectID?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'ID Proyecto de financiamiento:',toolTexto:"planning.budget.projectID"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.planning.budget.projectID})
                      ):null),
                      (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.uri?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Enlace Presupuesto:',toolTexto:"planning.budget.uri"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
                          $('<a>',{text:'Presupuesto',class:'enlaceTablaGeneral',href:procesoRecord.compiledRelease.planning.budget.uri})
                        )
                      ):null)
                      )
                      )
                ),
                (procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.amount?
                  $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                    $('<table>',{class:'tablaAncho'}).append(
                      $('<tbody>').append(
                        $('<tr>').append(
                          $('<td>',{class:'textoAlineadoDerecha'}).append(
                            $('<div>',{
                              class:'montoTotalProceso pr-3'
                            }).append(
                              /*$('<img>',{class:'imagenMonto mr-1',src:'/static/img/otros/monedasHonduras.png'}),*/
                              $('<div>',{class:'contenedorMonto procesoMonto'}).append(
                                $('<div>',{class:'textoColorGris',text:'Presupuesto'}),
                                $('<div>',{class:'valorMonto'}).append(
                                  $('<span>',{toolTexto:"planning.budget.amount.amount"}).append(ValorMoneda(procesoRecord.compiledRelease.planning.budget.amount.amount)),
                                  $('<span>',{class:'textoColorPrimario',text:' '+procesoRecord.compiledRelease.planning.budget.amount.currency, toolTexto:"planning.budget.amount.currency"})
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
                $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'documentosTabPlaneacion',id:'documentosTabPlaneacion'}).append(

                  )
            )
            
          )
        );/*
        if(procesoRecord.compiledRelease.tender&&procesoRecord.compiledRelease.tender.items&&procesoRecord.compiledRelease.tender.items.length){
          $('.planificacion.solicitados').append(
            $('<div>', {class:' cajonSombreado '}).append(
              $('<table>',{class:'tablaGeneral'}).append(
                $('<thead>').append(
                  $('<tr>').append(
                    $('<th>',{text:'Id'}),
                    $('<th>',{text:'Clasificación'}),
                    $('<th>',{text:'Descripción'}),
                    $('<th>',{text:'Cantidad'}),
                    $('<th>',{text:'Precio'}),
                    $('<th>',{text:'Unidad'})
                  )
                ),
                $('<tbody>').append(
                  ObtenerItems(procesoRecord.compiledRelease.tender.items)
                )
              )
            )
          )
        }*/

        if(procesoRecord.compiledRelease.planning.documents&&procesoRecord.compiledRelease.planning.documents.length){
          $('#documentosTabPlaneacion').append(
            $('<div>', {class:' cajonSombreadox '}).append(
              $('<table>',{class:'tablaGeneral'}).append(
                $('<thead>').append(
                  $('<tr>').append(
                    $('<th>',{text:'Nombre', toolTexto:'planning.documents[n].title'}),
                    $('<th>',{text:'Descripción', toolTexto:'planning.documents[n].description'}),
                    //$('<th>',{text:'Tipo', toolTexto:'planning.documents[n].documentType'}),
                    $('<th>',{text:'Fecha', toolTexto:'planning.documents[n].datePublished'}),
                    $('<th>',{text:''})
                  )
                ),
                $('<tbody>').append(
                  ObtenerDocumentos(procesoRecord.compiledRelease.planning.documents)
                )
              )
            )
          )
        }else{
          $('#documentosTabPlaneacion').html('<h4 class="titularColor textoColorPrimario mt-3">Esta etapa no posee documentos</h4>')
        }/*
        DefinirHistorialPresupuesto();*/
        if(procesoRecord.compiledRelease.planning.budget&&procesoRecord.compiledRelease.planning.budget.budgetBreakdown){
          $('#informacionTabPlaneacion').append(
            $('<div style="border-bottom: 3px solid #dee2e6;" class="mt-1 mb-1">'),
            $('<div>',{class:'row'}).append(
              $('<div>',{class:'col-md-12 mt-3'}).append(
                $('<h4>',{class:'titularCajonSombreado',text:'Desglose de Presupuesto'})
                )
            ),
            ObtenerEstructuraPresupuestaria(procesoRecord.compiledRelease.planning.budget.budgetBreakdown)
          )
        }
      }
    }
  }
  
  /*Planning*/
  function ObtenerProporcionadoresFondos(fondos){
    var elementos=[]
    for(i=0;i<fondos.length;i++){
      if(fondos[i].description){
        elementos.push(
          $('<tr>').append(
          $('<td>',{class:'tituloTablaCaracteristicas',text:'Origen de Fondos'}),
          $('<td>',{class:'contenidoTablaCaracteristicas',text:fondos[i].description})
        )
        )
      }
      if(fondos[i].sourceParty&&fondos[i].sourceParty.name){
        elementos.push(
          $('<tr>').append(
          $('<td>',{class:'tituloTablaCaracteristicas',text:'Fuente del Presupuesto'}),
          $('<td>',{class:'contenidoTablaCaracteristicas',text:fondos[i].sourceParty.name})
        )
        )
      }
    }
    return elementos;
  }
  function ObtenerHistorialPresupuesto(){
    var historico=[]
    if(procesoRecord.versionedRelease&&procesoRecord.versionedRelease.planning&&procesoRecord.versionedRelease.planning.budget&&procesoRecord.versionedRelease.planning.budget.budgetBreakdown){
      var desglosePresupuesto=procesoRecord.versionedRelease.planning.budget.budgetBreakdown;
      
      for(var i=0;i<desglosePresupuesto.length;i++){
        var comprometido=[];
        var precomprometido=[];
        if(desglosePresupuesto[i].measures){
          if(desglosePresupuesto[i].measures.ajusteComprometido){
            comprometido=comprometido.concat(desglosePresupuesto[i].measures.ajusteComprometido.map(function(e){e["type"]='ajusteComprometido';e["name"]='Ajuste Comprometido';return e;}));
          }
          if(desglosePresupuesto[i].measures.comprometido){
            comprometido=comprometido.concat(desglosePresupuesto[i].measures.comprometido.map(function(e){e["type"]='comprometido';e["name"]='Comprometido';return e;}));
          }
          if(desglosePresupuesto[i].measures.ajustePrecomprometido){
            precomprometido=precomprometido.concat(desglosePresupuesto[i].measures.ajustePrecomprometido.map(function(e){e["type"]='ajustePrecomprometido';e["name"]='Ajuste Pre-Comprometido';return e;}));
          }
          if(desglosePresupuesto[i].measures.precomprometido){
            precomprometido=precomprometido.concat(desglosePresupuesto[i].measures.precomprometido.map(function(e){e["type"]='precomprometido';;e["name"]='Pre-Comprometido';return e;}));
          }
          historico.push({
            "precomprometido":precomprometido.sort(function(a,b){return  new Date(a.releaseDate) - new Date(b.releaseDate);}),
            "comprometido":comprometido.sort(function(a,b){return  new Date(a.releaseDate) - new Date(b.releaseDate);})
          });
        }
      }
  
  
    }
    return historico;
  }
function DefinirHistorialPresupuesto(){
    var historial=ObtenerHistorialPresupuesto();
    for(var i=0;i<historial.length;i++){
    var desglose=historial[i];
    var precomprometido=desglose.precomprometido.filter(function(arreglo) {
        return arreglo.value!=0;
    })
    var comprometido=desglose.comprometido.filter(function(arreglo) {
        return arreglo.value!=0;
    });
    if(precomprometido.length>1||comprometido.length>1){
        $('.planificacion.informacion').append(
            $('<div>',{class:'row'}).append(

                (precomprometido.length>1?$('<div>',{class:'mt-3 '+(comprometido.length>1?'col-md-6':'col-md-12')}).append(
                $('<h4>',{class:'titularCajonSombreado'}).append(
                    'Historial Pre-Comprometido',
                    (VerificarReversion(i)?$('<span>',{class:'textoColorAdvertencia',text:' (Reversión)'}):null)
                ),
                $('<div>',{class:'cajonSombreado contenedorDetalleProcesoDatos'}).append(
                    $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                        $('<table>',{class:'tablaAncho'}).append(
                          $('<tbody>').append(
                            ObternerElementosSuma(precomprometido,VerificarReversion(i))
                            )
                        )
                      )
                )
                ):null),
                (comprometido.length>1?$('<div>',{class:'mt-3 '+(precomprometido.length>1?'col-md-6':'col-md-12')}).append(
                    $('<h4>',{class:'titularCajonSombreado'}).append(
                        'Historial Comprometido',
                        (VerificarReversion(i)?$('<span>',{class:'textoColorAdvertencia',text:' (Reversión)'}):null)
                    ),
                    $('<div>',{class:'cajonSombreado contenedorDetalleProcesoDatos'}).append(
                        $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                            $('<table>',{class:'tablaAncho'}).append(
                              $('<tbody>').append(
                                ObternerElementosSuma(comprometido,VerificarReversion(i))
                                )
                            )
                          )
                    )
                    ):null)
                
            )
            
        )
    }
    
    }
}

function ObternerElementosSuma(datos,reversion){
    var elementos=[];
    var suma=0;
    for(var i=0;i<datos.length;i++){
        suma+=ObtenerNumero(datos[i].value);
        elementos.push(
            $('<tr>').append(
                $('<td>',{class:'textoAlineadoIzquierda textoColorGris titularColor fuente18',text:datos[i].name,'colspan':2})
            ),
            $('<tr>').append(
                $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoCentrado textoColorGrisNormal ',text:ObtenerFecha(datos[i].releaseDate)}),
                $('<td>',{class:'textoAlineadoDerecha titularColor textoColorGrisNormal fuente18'}).append(
                    ValorMoneda(datos[i].value),
                    $('<span>',{class:'textoColorPrimario',text:' HNL'})
                )
              ) 
            )
    }
    elementos.push(
        $('<tr>').append(
                $('<td>',{class:'contenidoTablaCaracteristicas textoAlineadoCentrado titularColor fuente18 '+'textoColorPrimario',text:'Total'}),
                $('<td>',{class:'textoAlineadoDerecha titularColor textoColorGrisNormal fuente18'}).append(
                    ValorMoneda(suma),
                    $('<span>',{class:'textoColorPrimario',text:' HNL'})
                )
              ) 
    )
    return elementos;
}
function VerificarReversion(i){
    var desglose=procesoRecord.compiledRelease.planning.budget.budgetBreakdown[i];
    return(
        desglose.measures&&
        desglose.measures.ajusteComprometido===0&&
        desglose.measures.ajustePrecomprometido<0&&
        desglose.measures.comprometido===0&&
        desglose.measures.precomprometido===0
    )
}