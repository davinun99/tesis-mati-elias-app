/**
 * @file procesoConvocatoria.js Este archivo se incluye en la sección de Visualización de un Proceso de Contratación del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
var estadosConvocatoria={
  'planning':{titulo:'Planeación',descripcion:'Un proceso de contrataciones futuro que se está considerando. Información temprana sobre el proceso se puede dar en la sección de licitación. Un proceso con este estado puede dar información sobre un compromiso temprano u oportunidades de consulta, durante los cuales los detalles de la próxima licitación pueden formarse.'},
  'planned':{titulo:'Planeado',descripcion:'Un proceso de contratación está programado, pero aún no se ha llevado a cabo. Los detalles de las fechas anticipadas pueden proveerse en el bloque de licitación.'},
  'active':{titulo:'Activo',descripcion:'Un proceso de licitación está en proceso.'},
  'cancelled':{titulo:'Cancelado',descripcion:'El proceso de licitación ha sido cancelado.'},
  'unsuccessful':{titulo:'Sin Éxito',descripcion:'El proceso de licitación no fue exitoso.'},
  'complete':{titulo:'Completo',descripcion:'El proceso de licitación está completo.'},
  'withdrawn':{titulo:'Retirado',descripcion:'No hay más información disponible sobre este proceso bajo este ocid.'}
}
var metodosAdjudicacion={
  'open':{titulo:'Abierta',descripcion:'Todos los proveedores interesados pueden hacer una oferta.'},
  'selective':{titulo:'Selectiva',descripcion:'Sólo los proveedores calificados son invitados a enviar una propuesta.'},
  'limited':{titulo:'Limitada',descripcion:'La entidad licitadora contacta a un número de proveedores de su elección.'},
  'direct':{titulo:'Directa',descripcion:'El contrato se otorga a un solo proveedor sin competencia.'}
}
var categoriaCompra={
  'goods':{titulo:'Bienes y provisiones',descripcion:'El objeto primario de este proceso de contratación involucra bienes físicos o electrónicos o provisiones'},
  'works':{titulo:'Obras',descripcion:'El objeto primario de este proceso de contratación involucra construcción, reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
  'services':{titulo:'Servicios',descripcion:'El objeto primario de este proceso de contratación involucra servicios profesionales de alguna manera, generalmente contratados en la forma de resultados medibles o entregables.'}
}
var categoriaCompraAmpliada={
  'goods':{titulo:'Bienes y provisiones',descripcion:'El proceso de contrataciones involucra bienes o suministros físicos o electrónicos.'},
  'works':{titulo:'Obras',descripcion:'El proceso de contratación involucra construcción reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
  'services':{titulo:'Servicios',descripcion:'El proceso de contratación involucra servicios profesionales de algún tipo, generalmente contratado con base de resultados medibles y entregables. Cuando el código de consultingServices está disponible o es usado por datos en algún conjunto da datos en particular, el código de servicio sólo debe usarse para servicios no de consultoría.'},
  'consultingServices':{titulo:'Servicios de consultoría',descripcion:'Este proceso de contratación involucra servicios profesionales provistos como una consultoría.'}
}
var metodosPresentacion={
  'electronicSubmission':{titulo:'Presentación electrónica',descripcion:'Las ofertas se recibirán a través de una plataforma electrónica de adquisiciones.'},
  'electronicAuction':{titulo:'Subasta electrónica',descripcion:'Las ofertas se recibirán a través de una plataforma electrónica de subasta'},
  'written':{titulo:'Escrita',descripcion:'Las ofertas se recibirán via documentos escritos, entregados como copias físicas, via sistemas de correo electrónico genérico o algún mecanismo similar.'},
  'inPerson':{titulo:'En persona',descripcion:'Las ofertas sólo se recibirán si se entregan en persona en el tiempo y hora especificada en submissionMethodDetails o la documentación adjunta.'}
}
function DefinirElementosConvocatoria(){
    if(procesoRecord.compiledRelease){
      if(procesoRecord.compiledRelease.tender){
        
        $('.convocatoria.informacion').append(
          $('<div>',{class:'row'}).append(
            $('<h4>',{class:'col-6 col-sm-6 col-md-6 titularCajonSombreado',text:'Datos de la Convocatoria'}),
            $('<div>',{class:'col-6 col-sm-6 col-md-6 textoAlineadoDerecha'}).append(
              $('<h4>',{class:'descargaIconos enLinea'}).append(
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaJSONConvocatoria'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.JSON'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaCSVConvocatoria'}).append(
                  $('<i>',{class:'fas fa-file-download'}),
                  '&nbsp;.CSV'
                ),
                $('<span>',{class:'textoColorGris textoAlineadoDerecha p-1 cursorMano transicion',id:'descargaXLSXConvocatoria'}).append(
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
              $('<a>',{class:'nav-item nav-link active','data-toggle':'tab',role:'tab','aria-controls':'informacionTabConvocatoria',href:'#informacionTabConvocatoria','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Información'})
              ),
              $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'ofertadoresTabConvocatoria',href:'#ofertadoresTabConvocatoria','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Ofertadores'})
              ),
              $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'itemsTabConvocatoria',href:'#itemsTabConvocatoria','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Artículos Solicitados'})
              )
              ,
              $('<a>',{class:'nav-item nav-link ','data-toggle':'tab',role:'tab','aria-controls':'documentosTabConvocatoria',href:'#documentosTabConvocatoria','aria-selected':'true'}).append(
                $('<h4>',{class:'titularColor', style:'font-size: 15px',text:'Documentos'})
              )
                )
            ),
            $('<div>',{class:'tab-content cajonSombreado',id:'contenedorTabConvocatoria'}).append(
              $('<div>',{class:'tab-pane fade show active',role:'tabpanel','aria-labelledby':'informacionTabConvocatoria',id:'informacionTabConvocatoria'}).append(
                $('<div>',{class:'contenedorProceso informacionProceso'}).append(
                  (procesoRecord.compiledRelease.tender.title||procesoRecord.compiledRelease.tender.description?
                      $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                          $('<table>').append(
                            $('<tbody>').append(
                                  procesoRecord.compiledRelease.tender.title?$('<tr>').append(
                                  $('<td>',{class:'tituloTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.title,style:'color:#333',toolTexto:"tender.title"})):null,
                                  procesoRecord.compiledRelease.tender.description?$('<tr>').append(
                                      $('<td>',{class:'',text:procesoRecord.compiledRelease.tender.description,style:'color:#333',toolTexto:"tender.description"})):null
                            ))):null),
                $('<div>',{class:'contenedorTablaCaracteristicas'}).append(
                  $('<table>').append(
                    $('<tbody>').append(
                      procesoRecord.compiledRelease.buyer&&procesoRecord.compiledRelease.buyer.name ? 
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Comprador:',toolTexto:"buyer"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas'}).append(ObtenerElementosParte(procesoRecord.compiledRelease.buyer.id))
                      ) : null
                      ,
                      procesoRecord.compiledRelease.tender.status&&estadosConvocatoria[procesoRecord.compiledRelease.tender.status] ? $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Estado',toolTexto:"tender.status"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:estadosConvocatoria[procesoRecord.compiledRelease.tender.status].titulo })
                      ) : null
                      ,
                      procesoRecord.compiledRelease.tender.procuringEntity&&procesoRecord.compiledRelease.tender.procuringEntity.name ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Entidad que gestiona el proceso',toolTexto:"tender.procuringEntitys"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
                          ObtenerElementosParte(procesoRecord.compiledRelease.tender.procuringEntity.id)
                        )
                      ) : null,
                      procesoRecord.compiledRelease.tender.procurementMethod&&metodosAdjudicacion[procesoRecord.compiledRelease.tender.procurementMethod] ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Método de adjudicación',toolTexto:"tender.procurementMethod"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:metodosAdjudicacion[procesoRecord.compiledRelease.tender.procurementMethod].titulo})
                      ) : null,
  
                      procesoRecord.compiledRelease.tender.procurementMethodDetails ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Detalles del método de adjudicación',toolTexto:"tender.procurementMethodDetails"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.procurementMethodDetails})
                      ) : null,
                      procesoRecord.compiledRelease.tender.procurementMethodRationale ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Razón del método de adjuficación',toolTexto:"tender.procurementMethodRationale"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.procurementMethodRationale})
                      ) : null,
                      procesoRecord.compiledRelease.tender.mainProcurementCategory&&categoriaCompra[procesoRecord.compiledRelease.tender.mainProcurementCategory] ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Categoría compra',toolTexto:"tender.mainProcurementCategory"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:categoriaCompra[procesoRecord.compiledRelease.tender.mainProcurementCategory].titulo})
                      ) : null,
                      procesoRecord.compiledRelease.tender.additionalProcurementCategories ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Categoría compra ampliada',toolTexto:"tender.additionalProcurementCategories"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.additionalProcurementCategories.map(function(e){if(categoriaCompraAmpliada[e]){e=categoriaCompraAmpliada[e].titulo;}return e;}).join(', ')})
                      ) : null,
                      procesoRecord.compiledRelease.tender.awardCriteria ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Criterio de adjudicación',toolTexto:"tender.awardCriteria"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.awardCriteria})
                      ) : null,
                      procesoRecord.compiledRelease.tender.awardCriteriaDetails ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Detalles del criterio de adjudicación',toolTexto:"tender.awardCriteriaDetails"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.awardCriteriaDetails})
                      ) : null,
                      procesoRecord.compiledRelease.tender.submissionMethod ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Método de Presentación de Ofertas',toolTexto:"tender.submissionMethod"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.submissionMethod.map(function(e){if(metodosPresentacion[e]){e=metodosPresentacion[e].titulo;}return e;}).join(', ')})
                      ) : null,
                      procesoRecord.compiledRelease.tender.submissionMethodDetails ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Detalle de Presentación de Ofertas',toolTexto:"tender.submissionMethodDetails"}),
                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.submissionMethodDetails})
                      ) : null,

                      procesoRecord.compiledRelease.tender.hasEnquiries ?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Recepción de consultas',toolTexto:"tender.hasEnquiries"}),
                      $('<td>',{class:'contenidoTablaCaracteristicas',text:'Se recibieron consultas durante el proceso de licitación'})
                    ) : null,
                    procesoRecord.compiledRelease.tender.eligibilityCriteria ?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Criterios de elección',toolTexto:"tender.eligibilityCriteria"}),
                      $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.eligibilityCriteria})
                    ) : null,
                    
                    
                      
                      
                    
                    
                    
                    procesoRecord.compiledRelease.tender.numberOfTenderers ?
                    $('<tr>').append(
                      $('<td>',{class:'tituloTablaCaracteristicas',text:'Número de propuestas recibidas',toolTexto:"tender.numberOfTenderers"}),
                      $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.numberOfTenderers})
                    ) : null,
                    /*procesoRecord.compiledRelease.tender.tenderers ?
                    ObtenerOfertadores(procesoRecord.compiledRelease.tender.tenderers) : null,*/
  
                    
                 /*   
                    (procesoRecord.compiledRelease.tender.milestones ? 
                      ObtenerHitosRelacionados(procesoRecord.compiledRelease.tender.milestones)
                      : null),*/
  
                      //categoriaCompraAmpliada
  
                     
                      procesoRecord.compiledRelease.tender.minValue&&!$.isEmptyObject(procesoRecord.compiledRelease.tender.minValue) ?
                      $('<tr>').append(
                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Valor mínimo estimado para la contratación'}),
                        $('<td>',{class:'contenidoTablaCaracteristicas'}).append(
                          $('<span>',{toolTexto:"tender.minValue.amount"}).append(
                            ValorMoneda(procesoRecord.compiledRelease.tender.minValue.amount)
                          )
                          ,
                          $('<span>',{class:'textoColorPrimario',text:procesoRecord.compiledRelease.tender.minValue.currency?procesoRecord.compiledRelease.tender.minValue.currency:'HNL',toolTexto:"tender.minValue.currency"})
                        )
                      ) : null
                      )
                      )

                      ,(procesoRecord.compiledRelease.tender.tenderPeriod||procesoRecord.compiledRelease.tender.enquiryPeriod||procesoRecord.compiledRelease.tender.awardPeriod||procesoRecord.compiledRelease.tender.contractPeriod)?([
                        $('<div style="border-bottom: 3px solid #dee2e6;" class="mt-1 mb-1">'),
                        $('<div class="row" >').append(
                          
                          /*Periodo de Licitación*/
                        procesoRecord.compiledRelease.tender.tenderPeriod ? (
                          $('<div class="contenedorDetalleProcesoDatos col-6 col-sm-6 col-md-6">').append(
                            $('<div class="contenedorProceso informacionProceso">').append(
                              $('<div class="contenedorTablaCaracteristicas">').append(
                                $('<table>').append(
                                  $('<tbody>').append(
                                    $('<tr><td><h4 class="titularColor textoColorPrimario">Recepción de Ofertas</h4></td></tr>'),
                                      procesoRecord.compiledRelease.tender.tenderPeriod.startDate ? 
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio',toolTexto:"tender.tenderPeriod.startDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.tenderPeriod.startDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.tenderPeriod.endDate ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización',toolTexto:"tender.tenderPeriod.endDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.tenderPeriod.endDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.tenderPeriod.maxExtentDate ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha Máxima',toolTexto:"tender.tenderPeriod.maxExtentDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.tenderPeriod.maxExtentDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.tenderPeriod.durationInDays ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Duración en Días',toolTexto:"tender.tenderPeriod.durationInDays"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.tenderPeriod.durationInDays})
                                      ) : null
                                    
                                  )
                                )
                              )
                            )
                          )
                        ) : null,
                        /*Periodo de Consultas*/
                        procesoRecord.compiledRelease.tender.enquiryPeriod ? (
                          $('<div class="contenedorDetalleProcesoDatos col-6 col-sm-6 col-md-6">').append(
                            $('<div class="contenedorProceso informacionProceso">').append(
                              $('<div class="contenedorTablaCaracteristicas">').append(
                                $('<table>').append(
                                  $('<tbody>').append(
                                    $('<tr><td><h4 class="titularColor textoColorPrimario">Consultas</h4></td></tr>'),
                                  
                                      procesoRecord.compiledRelease.tender.enquiryPeriod.startDate ? 
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio',toolTexto:"tender.enquiryPeriod.startDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.enquiryPeriod.startDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.enquiryPeriod.endDate ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización',toolTexto:"tender.enquiryPeriod.endDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.enquiryPeriod.endDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.enquiryPeriod.maxExtentDate ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha Máxima',toolTexto:"tender.enquiryPeriod.maxExtentDate"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.enquiryPeriod.maxExtentDate)})
                                      ) : null,
                                      procesoRecord.compiledRelease.tender.enquiryPeriod.durationInDays ?
                                      $('<tr>').append(
                                        $('<td>',{class:'tituloTablaCaracteristicas',text:'Duración en Días',toolTexto:"tender.enquiryPeriod.durationInDays"}),
                                        $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.enquiryPeriod.durationInDays})
                                      ) : null
                                    
                                  )
                                )
                              )
                            )
                          )
                          
                        ) : null,
                        /*Periodo de Adjudicación*/
                      procesoRecord.compiledRelease.tender.awardPeriod ? (
                        $('<div class="contenedorDetalleProcesoDatos col-6 col-sm-6 col-md-6">').append(
                          $('<div class="contenedorProceso informacionProceso">').append(
                            $('<div class="contenedorTablaCaracteristicas">').append(
                              $('<table>').append(
                                $('<tbody>').append(
                                  $('<tr><td><h4 class="titularColor textoColorPrimario">Adjudicación</h4></td></tr>'),
                                  
                                  procesoRecord.compiledRelease.tender.awardPeriod.startDate ? 
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio',toolTexto:"tender.awardPeriod.startDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.awardPeriod.startDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.awardPeriod.endDate ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización',toolTexto:"tender.awardPeriod.endDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.awardPeriod.endDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.awardPeriod.maxExtentDate ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha Máxima',toolTexto:"tender.awardPeriod.maxExtentDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.awardPeriod.maxExtentDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.awardPeriod.durationInDays ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Duración en Días',toolTexto:"tender.awardPeriod.durationInDays"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.awardPeriod.durationInDays})
                                  ) : null
                                )
                              )
                            )
                          )
                        )
                      ) : null,
                      /*Periodo de Contrato*/
                      procesoRecord.compiledRelease.tender.contractPeriod ? (
                        $('<div class="contenedorDetalleProcesoDatos col-6 col-sm-6 col-md-6">').append(
                          $('<div class="contenedorProceso informacionProceso">').append(
                            $('<div class="contenedorTablaCaracteristicas">').append(
                              $('<table>').append(
                                $('<tbody>').append(
                                  $('<tr><td><h4 class="titularColor textoColorPrimario">Periodo de Contrato</h4></td></tr>'),
                                  procesoRecord.compiledRelease.tender.contractPeriod.startDate ? 
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Inicio',toolTexto:"tender.contractPeriod.startDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.contractPeriod.startDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.contractPeriod.endDate ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha de Finalización',toolTexto:"tender.contractPeriod.endDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.contractPeriod.endDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.contractPeriod.maxExtentDate ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Fecha Máxima',toolTexto:"tender.contractPeriod.maxExtentDate"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:ObtenerFecha(procesoRecord.compiledRelease.tender.contractPeriod.maxExtentDate)})
                                  ) : null,
                                  procesoRecord.compiledRelease.tender.contractPeriod.durationInDays ?
                                  $('<tr>').append(
                                    $('<td>',{class:'tituloTablaCaracteristicas',text:'Duración en Días',toolTexto:"tender.contractPeriod.durationInDays"}),
                                    $('<td>',{class:'contenidoTablaCaracteristicas',text:procesoRecord.compiledRelease.tender.contractPeriod.durationInDays})
                                  ) : null
                                )
                              )
                            )
                          )
                        )
                      ) : null,
                        )]
                      ):null,
                ),
                (procesoRecord.compiledRelease.tender.value&&procesoRecord.compiledRelease.tender.value.amount?
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
                                $('<div>',{class:'textoColorGris',text:'Monto'}),
                                $('<div>',{class:'valorMonto'}).append(
                                  $('<span>',{toolTexto:"tender.value.amount"}).append(
                                    ValorMoneda(procesoRecord.compiledRelease.tender.value.amount)
                                  )
                                  ,
                                  $('<span>',{class:'textoColorPrimario',text:procesoRecord.compiledRelease.tender.value.currency,toolTexto:"tender.value.currency"})
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
                $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'ofertadoresTabConvocatoria',id:'ofertadoresTabConvocatoria'}).append(

                  ),
                $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'itemsTabConvocatoria',id:'itemsTabConvocatoria'}).append(

                  ),
                $('<div>',{class:'tab-pane fade',role:'tabpanel','aria-labelledby':'documentosTabConvocatoria',id:'documentosTabConvocatoria'}).append(

                  )
            )




           
          ),
          (procesoRecord.compiledRelease.tender.amendments?$('<div>').append(
            ObtenerEnmiendasAdjudicacion(procesoRecord.compiledRelease.tender.amendments)
            ):null),
         /* procesoRecord.compiledRelease.parties ? 
          $('<div>',{class:'row mb-5 mt-5'}).append(
            ObtenerDatosContacto(procesoRecord.compiledRelease.parties,'buyer',['Unidad Ejecutora:','Comprador'])
          ) : null*/
        );
        if(procesoRecord.compiledRelease.tender.items&&procesoRecord.compiledRelease.tender.items.length){
          $('#itemsTabConvocatoria').append(
            $('<div>', {class:' cajonSombreadox '}).append(
              $('<table>',{class:'tablaGeneral'}).append(
                $('<thead>').append(
                  $('<tr>').append(
                    $('<th>',{text:'Id', toolTexto:'tender.items[n].classification.id'}),
                    $('<th>',{text:'Clasificación', toolTexto:'tender.items[n].classification.scheme'}),
                    $('<th>',{text:'Descripción', toolTexto:'tender.items[n].classification.description'}),
                    /*$('<th>',{text:'Especificaciones'}),*/
                    $('<th>',{text:'Cantidad', toolTexto:'tender.items[n].quantity'}),
                    $('<th>',{text:'Precio', toolTexto:'tender.items[n].unit.value.amount'}),
                    $('<th>',{text:'Unidad', toolTexto:'tender.items[n].unit.name'})
                  )
                ),
                $('<tbody>').append(
                  ObtenerItems(procesoRecord.compiledRelease.tender.items)
                )
              )
            )
          )
        }
        if(procesoRecord.compiledRelease.tender.documents&&procesoRecord.compiledRelease.tender.documents.length){
          $('#documentosTabConvocatoria').append(
            $('<div>', {class:' cajonSombreadox '}).append(
              $('<table>',{class:'tablaGeneral'}).append(
                $('<thead>').append(
                  $('<tr>').append(
                    $('<th>',{text:'Nombre', toolTexto:'tender.documents[n].title'}),
                    $('<th>',{text:'Descripción',toolTexto:'tender.documents[n].description'}),
                    //$('<th>',{text:'Tipo',toolTexto:'tender.documents[n].documentType'}),
                    $('<th>',{text:'Fecha',toolTexto:'tender.documents[n].datePublished'}),
                    $('<th>',{text:''})
                  )
                ),
                $('<tbody>').append(
                  ObtenerDocumentos(procesoRecord.compiledRelease.tender.documents)
                )
              )
            )
          )
        }else{
            $('#documentosTabConvocatoria').html('<h4 class="titularColor textoColorPrimario mt-3">Esta etapa no posee documentos</h4>')
          
        }

        if(procesoRecord.compiledRelease.tender.tenderers&&procesoRecord.compiledRelease.tender.tenderers.length){
          $('#ofertadoresTabConvocatoria').append(
            $('<div>', {class:' cajonSombreadox '}).append(
              $('<table>',{class:'tablaGeneral'}).append(
                $('<thead>').append(
                  $('<tr>').append(
                    $('<th>',{text:'Identificador', toolTexto:'tender.tenderers[n].id'}),
                    $('<th>',{text:'Ofertador',toolTexto:'tender.tenderers[n].dname'})
                  )
                ),
                $('<tbody>').append(
                  ObtenerOfertadores(procesoRecord.compiledRelease.tender.tenderers)
                )
              )
            )
          );
        }else{
            $('#ofertadoresTabConvocatoria').html('<h4 class="titularColor textoColorPrimario mt-3">No hay ofertadores disponibles</h4>');
          
        }
      }
    }
  }

  function ObtenerOfertadores(ofertadores){
    var elementos=[];
    for(var i=0;i<ofertadores.length;i++){
      elementos.push(
        $('<tr>').append(
          $('<td>',{class:'',text:ofertadores[i].id,'data-label':'Identificador'}),
          $('<td>',{class:'','data-label':'Ofertador'}).append(
            $('<a>',{text:ObtenerTexto(ofertadores[i].name) ,class:'enlaceTablaGeneral',href:url+'/proveedor/'+ofertadores[i].id})
          )
        )
      );
    }
    return elementos;
  }

  function ObtenerEnmiendasConvocatoria(enmiendas){
    var elementos=[];
    for(var i=0;i<enmiendas.length;i++){
      if(enmiendas[i].id){
        elementos.push(
          $('<div>',{class:'row mt-3'}).append(
            $('<h4>',{class:'col-12 col-sm-12 col-md-12 titularCajonSombreado',text:'Enmienda Convocatoria'})
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