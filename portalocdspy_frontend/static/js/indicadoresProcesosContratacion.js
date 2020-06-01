/**
 * @file indicadoresProcesosContratacion.js Este archivo se incluye en la sección de Indicadores de procesos de contratacion del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

 /**
 * Variable de filtros aplicables con su  titulo y parametro
 * @type {Object} 
 */
var filtrosAplicables={
    monedas: {titulo:'Moneda',parametro:'moneda'},
    instituciones: {titulo:'Institución Compradora',parametro:'idinstitucion'},
    años: {titulo:'Año',parametro:'año'},
    proveedores: {titulo:'Proveedor',parametro:'proveedor'},
    categorias: {titulo:/*'Categoría de Compra'*/'Tipo de Contrato',parametro:'categoria'},
    modalidades : {titulo:'Modalidad de Compra',parametro:'modalidad'},
    sistemas :{titulo:'Sistema de Origen', parametro: 'sistema'}
    
  };

/**
 * Variable de filtros aplicables con su  titulo y parametro con su parametro como llave
 * @type {Object} 
 */
  var filtrosAplicablesR={
    moneda: {titulo:'Moneda',parametro:'monedas'},
    idinstitucion: {titulo:'Institución Compradora',parametro:'instituciones'},
    año: {titulo:'Año',parametro:'años'},
    proveedor: {titulo:'Proveedor',parametro:'proveedores'},
    modalidad: {titulo:'Modalidad de Compra',parametro:'modalidades'},
    categoria : {titulo:/*'Categoría de Compra'*/'Tipo de Contrato',parametro:'categorias'},
    sistema: {titulo:'Sistema de Origen', parametro:'sistemas'}
  };

/**
 * Arreglo para definir el orden en el que se presentan los filtros
 * @type {string[]} 
 */
  var ordenFiltros=['años','monedas','instituciones','categorias','modalidades','sistemas'];

/**
 * Objeto para obtener traducciones e informacion de algunos códigos el OCDS
 * @type {Object} 
 */
  var traducciones={
    'goods':{titulo: 'Suministro de Bienes y/o Servicios'/*'Bienes y provisiones'*/,descripcion:'El proceso de contrataciones involucra bienes o suministros físicos o electrónicos.'},
    'works':{titulo:'Obras',descripcion:'El proceso de contratación involucra construcción reparación, rehabilitación, demolición, restauración o mantenimiento de algún bien o infraestructura.'},
    'services':{titulo:'Servicios'/*'Servicios'*/,descripcion:'El proceso de contratación involucra servicios profesionales de algún tipo, generalmente contratado con base de resultados medibles y entregables. Cuando el código de consultingServices está disponible o es usado por datos en algún conjunto da datos en particular, el código de servicio sólo debe usarse para servicios no de consultoría.'},
    'consultingServices':{titulo:'Consultorías',descripcion:'Este proceso de contratación involucra servicios profesionales provistos como una consultoría.'},
    'tender':{titulo:'Licitación',descripcion:'Provee información sobre una nueva licitación (llamado a propuestas). La entrega de licitación debe contener detalles de los bienes o servicios que se buscan.'},
    'awards':{titulo:'Adjudicación',descripcion:'Da información sobre la adjudicación de un contrato. Estarán presentes una o más secciones de adjudicación, y la sección de licitación puede estar poblada con detalles del proceso que llevó a la adjudicación.'},
    'contracts':{titulo:'Contrato',descripcion:'Da información sobre los detalles de un contrato que ha entrado, o entrará, en vigencia. La sección de licitación puede ser poblada con detalles del proceso que lleva al contrato, y la sección de adjudicación puede tener detalles sobre la adjudicación sobre la '},
    'planning':{
        titulo:'Planeación',
        descripcion:'Se propone o planea un proceso de contratación. La información en la sección de licitación describe el proceso propuesto. El campo tender.status debe de usarse para identificar si la planeación está en una etapa temprana o si hay planes detallados para una licitación.'
    },
    'goodsOrServices':{titulo:'Bienes y/o Servicios',descripcion:''}

  }

/**
 * Devuelve información de un filtro como ser que valor usar para mostrar la cantidad en el badge y que valor usar a la hora de que sea seleccionado
 * @param {string} llave - llave de un filtro 
 */
function ValoresLlaves(llave){
    switch(llave){
        case 'años':
            return {valor:'key_as_string',cantidad:'contratos',codigo:'key_as_string'};
        case 'categorias':
            return {valor:'categoria',cantidad:'contratos',codigo:'categoria'};
        case 'instituciones':
            return {valor:'nombre',cantidad:'contratos',codigo:'codigo'};
        case 'modalidades':
            return {valor:'modalidad',cantidad:'contratos',codigo:'modalidad'};
        case 'monedas':
            return {valor:'moneda',cantidad:'contratos',codigo:'moneda'};
        case 'sistemas':
            return {valor:'id',cantidad:'ocids',codigo:'id'};
        default:
            return {valor:'key_as_string',cantidad:'contratos',codigo:'key_as_string'};
    }
}


/**
 * Obtiene los datos e inicializa el gráfico de procesos por modalidad de compra
 */
function ModalidadMontoCantidad(){
    
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros)
    MostrarReloj('#modalidadMontoCantidad',true)
    $.get(api+"/indicadoresoncae/contratospormodalidad/",parametros).done(function( datos ) {

        
    OcultarReloj('#modalidadMontoCantidad');
    if(datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadContratos)  && datos.resultados.cantidadContratos.length==0){
        MostrarSinDatos('#modalidadMontoCantidad',true)
        return;
    }
    var grafico=echarts.init(document.getElementById('modalidadMontoCantidad'));
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
    
    
                    
                    var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
    
                    e.forEach(function(valor,indice){
                        cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorNumerico(valor.value) :ValorMoneda(valor.value)) +' '+(valor.seriesIndex==0?'Contratos':'HNL')+'<br>'
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
                    name:'Modalidad de Compra',
                    type: 'category',
                    data:datos.resultados.nombreModalidades,
                    axisPointer: {
                        type: 'shadow',
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,30);
                            }
                        }
                    },
                    axisLabel:{
                        interval:0,
                        rotate:45,
                        showMinLabel:false,
                        padding:[0,0,0,0]
                    },
                    //boundaryGap:false,
                    scale: true,
                    barGap:'0',
                    barCategoryGap: '0',
                    nameGap:"0",
                    axisTick: {
                        show: false,
                        alignWithLabel: true,
                      //  interval: 8,
                        inside: false,
                        length: 5,
                        padding:0
                    }
                    ,padding:0
                }
            ],
            yAxis: [
                {
                    type: 'value',
                    name: 'Cantidad de Contratos',/*
                    min: 0,
                    max: 250,
                    interval: 50,*/
                    axisLabel: {
                        formatter: '{value}'
                    }
                },
                {
                    type: 'value',
                    name: 'Monto de Contratos',/*
                    min: 0,
                    max: 25,
                    interval: 5,*/
                    axisLabel: {
                        formatter: '{value} HNL',
                        rotate:315
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
                    name:'Cantidad de Contratos',
                    type:'bar',
                    data:datos.resultados.cantidadContratos,
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[1]
                    },
                    barWidth:30,
                    barCategoryGap:'20%',
                    barGap:'50%'/*
                    barMaxWidth:20,
                    barGap:'0%',
                    barCategoryGap: '0%',
                    padding:0*/
                    ,
                            label: {
                                normal: {
                                    show:true,
                                    fontFamily:'Poppins',
                                    fontWeight:700,
                                    fontSize:15,
                                    position: 'right',
                                    formatter: function (e){
                                        return ValorNumerico(e.value);
                                    },color:'gray'
                                }
                            }
                },
                {
                    name:'Monto de Contratos',
                    type:'line',
                    //yAxisIndex: 1,
                    data:datos.resultados.montosContratos,
                    yAxisIndex: 1,/*
                    barMaxWidth:20,
                    barGap:'0%',
                    barCategoryGap: '0%',
                    padding:0,*/
    
    
                    symbol: 'circle',
                    symbolSize: 10,
                    lineStyle: {
                        normal: {
                            color: ObtenerColores('Pastel1')[0],
                            width: 4/*,
                            type: 'dashed'*/
                        }
                    },
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[0]
                    }/*,
                    label: {
                        normal: {
                            show:true,
                            fontFamily:'Poppins',
                            fontWeight:700,
                            fontSize:15,
                            position: 'right',
                            formatter: function (e){
                                return ValorMoneda(e.value)+' HNL';
                            }
                        }
                    }*/
    
                    
                }
            ],
            grid:{
                containLabel:true,
                top:100
            },
            label:{
                
            }
        },
        media:[
            {   query:{
                maxWidth:600
                },
                option:{
                    xAxis: [
                        {
                            type: 'category',
                            data:datos.resultados.nombreModalidades,
                            axisPointer: {
                                type: 'shadow',
                                label:{
                                    formatter:function(params){
                                        return  ObtenerParrafo(params.value,30);
                                    }
                                }
                            },
                            axisLabel:{
                                interval:0,
                                rotate:90,
                                showMinLabel:false,
                                padding:[0,0,0,0]
                            },
                            //boundaryGap:false,
                            scale: true,
                            barGap:'0',
                            barCategoryGap: '0',
                            nameGap:"0",
                            axisTick: {
                                show: false,
                                alignWithLabel: true,
                              //  interval: 8,
                                inside: false,
                                length: 5,
                                padding:0
                            }
                            ,padding:0
                        }
                    ],
                    yAxis: [
                        {
                            type: 'value',
                            name: 'Cantidad de\nContratos',/*
                            min: 0,
                            max: 250,
                            interval: 50,*/
                            axisLabel: {
                                formatter: '{value}',
                                rotate:65
                            }
                        },
                        {
                            type: 'value',
                            name: 'Monto de\nContratos',/*
                            min: 0,
                            max: 25,
                            interval: 5,*/
                            axisLabel: {
                                formatter: '{value} HNL',
                                rotate:295
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
                            name:'Cantidad de Contratos',
                            type:'bar',
                            data:datos.resultados.cantidadContratos,
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[1]
                            },
                            barWidth:20,
                            barCategoryGap:'10%',
                            barGap:'10%'/*
                            barMaxWidth:20,
                            barGap:'0%',
                            barCategoryGap: '0%',
                            padding:0*/
                        },
                        {
                            name:'Monto de Contratos',
                            type:'line',
                            //yAxisIndex: 1,
                            data:datos.resultados.montosContratos,
                            yAxisIndex: 1,/*
                            barMaxWidth:20,
                            barGap:'0%',
                            barCategoryGap: '0%',
                            padding:0,*/
            
            
                            symbol: 'circle',
                            symbolSize: 10,
                            lineStyle: {
                                normal: {
                                    color: ObtenerColores('Pastel1')[0],
                                    width: 4/*,
                                    type: 'dashed'*/
                                }
                            },
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[0]
                            }
            
                            
                        }
                    ],
                    grid:{
                        right:'10%',
                        left:0,
                        top:100
                    },
                    tooltip:{
                        position:['0%','50%']
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
 * Obtiene los datos e inicializa el gráfico de cantidad de contratos por categoría de compra
 */
function CantidadContratosTipoContrato(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros)
    MostrarReloj('#CantidadContratosTipoContrato',true)
$.get(api+"/indicadoresoncae/cantidadcontratosporcategoria/",parametros).done(function( datos ) {

    
OcultarReloj('#CantidadContratosTipoContrato',true)
var datosPastel=[];
    datos.resultados.categorias.forEach(function(valor,indice){
        datosPastel.push(
            {
                name:traducciones[valor.name]?traducciones[valor.name].titulo:valor.name,
                value:valor.value
            }
        )
    });
if(datosPastel.length==0){
    MostrarSinDatos('#CantidadContratosTipoContrato',true)
    return;
}    
var grafico=echarts.init(document.getElementById('CantidadContratosTipoContrato'));
var opciones = {
    baseOption:{
    tooltip : {
        trigger: 'item',
                
        formatter:  function (valor){

            var cadena=valor.name+'<br>';
           
            cadena=cadena+' '+valor.marker/*+' '+valor.seriesName+*/+' '+ValorNumerico(valor.value)+' '+'Contratos'+' ('+valor.percent+'%)'+'<br>';
            return cadena;
        }
    },
    legend: {
        type: 'plain',
        orient: 'horizontal',
        bottom: 0,
        right:'center'/*,
        data: ['lengend data 1','lengend data 2','lengend data 3'],

        selected: [false,false,true]*/,
        textStyle:{
            color:'gray'
        }
    },
    label:{
        color:'gray'
    },
    series : [
        {
            name: 'Cantidad de Contratos por Categoría de Compra',
            type: 'pie',
            radius : '40%',
            center: ['50%', '50%'],
            data: datosPastel,
            itemStyle: {
                color: function(e){
                    var colores=ObtenerColores('Pastel1');
                    return colores[e.dataIndex];
                },
                emphasis: {
                    shadowBlur: 10,
                    shadowOffsetX: 0,
                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
            },
            label:{
                show:true,
                color:'gray',
                formatter:function (e){
                    return ''+e.name+' \n'+ValorNumerico(e.value) +' ('+ ValorMoneda(e.percent) +'%)'
                }
            }
        }
    ],
    grid:{
        containLabel:true
    }},
    media:[
        {
            query:{
                maxWidth:500
            },
            option:{
                
            }
        },
        {
            query:{
                maxWidth:600
            },
            option:{
                series : [
                    {
                        name: 'Cantidad de Contratos por Categoría de Compra',
                        type: 'pie',
                        radius : '40%',
                        center: ['50%', '30%'],
                        data: datosPastel,
                        itemStyle: {
                            color: function(e){
                                var colores=ObtenerColores('Pastel1');
                                return colores[e.dataIndex];
                            },
                            emphasis: {
                                shadowBlur: 10,
                                shadowOffsetX: 0,
                                shadowColor: 'rgba(0, 0, 0, 0.5)'
                            }
                        },
                        label:{
                            show:true,
                            color:'gray',
                            formatter:function (e){
                                return ''+e.name+' \n'+ValorNumerico(e.value) +' ('+ ValorMoneda(e.percent) +'%)'
                            }
                        },
                        labelLine:{
                            show:false
                        }
                    }
                ],legend: {
                    type:'plain',
                    orient:'horizontal',
                    bottom:0,
                    right:'center',
                    formatter: function (e){
                        return e +'. '+ ValorNumerico(datosPastel.filter(function(data){ if(data.name===e){return true;}})[0].value);
                    }
                },
                tooltip: {
                    position:['0%','50%']
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
 * Obtiene los datos e inicializa el gráfico de cantidad de contratos por categoría de compra
 */
function MontoContratosTipoContrato(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#MontoContratosTipoContrato',true);
$.get(api+"/indicadoresoncae/montoporcategoria/",parametros).done(function( datos ) {

    
OcultarReloj('#MontoContratosTipoContrato',true);
var datosPastel=[];
    datos.resultados.categorias.forEach(function(valor,indice){
        datosPastel.push(
            {
                name:traducciones[valor.name]?traducciones[valor.name].titulo:valor.name,
                value:valor.value
            }
        )
    });

    if(datosPastel.length==0){
        MostrarSinDatos('#MontoContratosTipoContrato',true)
        return;
    } 
    var grafico=echarts.init(document.getElementById('MontoContratosTipoContrato'));
    var opciones = {
        baseOption:{
            tooltip : {
                trigger: 'item',
                
                formatter:  function (valor){
    
                    var cadena=valor.name+'<br>';
                   
                    cadena=cadena+' '+valor.marker/*+' '+valor.seriesName+*/+' '+ValorMoneda(valor.value)+' '+'HNL'+' ('+valor.percent+'%)'+'<br>';
                    return cadena;
                }
            },
            legend: {
                type: 'plain',
            orient: 'horizontal',
            bottom: 0,
            right:'center'/*,
                data: ['lengend data 1','lengend data 2','lengend data 3'],
        
                selected: [false,false,true]*/,
                textStyle:{
                    color:'gray'
                }
            },
            series : [
                {
                    name: 'Monto de Contratos por Tipo de Contrato',
                    type: 'pie',
                    radius : '40%',
                    center: ['50%', '50%'],
                    data: datosPastel,
                    itemStyle: {
                        color: function(e){
                            var colores=ObtenerColores('Pastel1');
                            return colores[e.dataIndex];
                        },
                        emphasis: {
                            shadowBlur: 10,
                            shadowOffsetX: 0,
                            shadowColor: 'rgba(0, 0, 0, 0.5)'
                        }
                    },
                    label:{
                        show:true,
                        color:'gray',
                        formatter:function (e){
                            return ''+e.name+' \n'+ValorMoneda(e.value) +' HNL ('+ ValorMoneda(e.percent) +'%)'
                        }
                    }
                }
            ],
            grid:{
                containLabel:true
            },
            label:{
                color:'gray'
            }
        },
        media:[
            {
                query:{
                    maxWidth:500
                },
                option:{
                    
                }
            },
            {
                query:{
                    maxWidth:600
                },
                option:{
                    series : [
                        {
                            name: 'Cantidad de Contratos por Tipo de Contrato',
                            type: 'pie',
                            radius : '40%',
                            center: ['50%', '30%'],
                            data: datosPastel,
                            itemStyle: {
                                color: function(e){
                                    var colores=ObtenerColores('Pastel1');
                                    return colores[e.dataIndex];
                                },
                                emphasis: {
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                                }
                            },
                            label:{
                                show:true,
                                color:'gray',
                                formatter:function (e){
                                    return ''+e.name+' \n'+ValorMoneda(e.value) +' HNL ('+ ValorMoneda(e.percent) +'%)'
                                }
                            },
                            labelLine:{
                                show:false
                            }
                        }
                    
                ],legend: {
                    type:'plain',
                    orient:'horizontal',
                    bottom:0,
                    right:'center',
                    formatter: function (e){
                        return e +'. '+ ValorMoneda(datosPastel.filter(function(data){ if(data.name===e){return true;}})[0].value)+' HNL';
                    }
                },
                tooltip: {
                    position:['0%','50%']
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
 * Obtiene los datos e inicializa el gráfico de Top 10 Compradores por Monto Contratado
 */
function Top10InstitucionesMontos(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#top10InstitucionesMontos',true);
$.get(api+"/indicadoresoncae/topcompradores/",parametros).done(function( datos ) {

    
OcultarReloj('#top10InstitucionesMontos',true);

if(datos&&datos.resultados&&Array.isArray(datos.resultados.montoContratado)  && datos.resultados.montoContratado.length==0){
    MostrarSinDatos('#top10InstitucionesMontos',true)
    return;
}
var grafico=echarts.init(document.getElementById('top10InstitucionesMontos'));
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
    
    
                    
                    var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
    
                    e.forEach(function(valor,indice){
                        cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value) :ValorNumerico(valor.value) ) +' '+(valor.seriesIndex==0?'HNL':'Procesos')+'<br>'
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
            },/*
            legend: {
                data:['蒸发量1','降水量','平均温度3']
            },*/
            xAxis: [
                {
                    type: 'value',
                                /*min: 0,
                                max: 810,*/
                                //interval: 100000,
                                axisLabel: {
                                    formatter: '{value} HNL',
                                    rotate:45,
                            showMinLabel:false
                                },
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
                                    }
                                },
                    position:'bottom',

                    name:'Monto Contratado'
                },
                {
                    type: 'value',
                    axisLabel: {
                        formatter: '{value}'
                    },
                    position:'top',
                    name:'Cantidad de Procesos'
                }
            ],
            yAxis: [
                {
                    
                    type: 'category',
                    data: datos.resultados.nombreCompradores.reverse(),
                    axisPointer: {
                        type: 'shadow',
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                    },
                    axisLabel:{
                        interval:0,
                        //rotate:45,
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        },
                        showMinLabel:false,
                        padding:[0,0,0,0]
                    }
                }
            ],
            series: [
                {
                    name:'Monto Contratado',
                    type:'bar',
                    data:datos.resultados.montoContratado.reverse(),
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[1]
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
                            },
                            color:'gray'
                        }
                    },
                    barWidth:30,
                    barCategoryGap:'20%',
                    barGap:'50%'
                },
                {
                    name:'Cantidad de Procesos de Contratación',
                    type:'line',
                    //yAxisIndex: 1,
                    data:datos.resultados.cantidadOCIDs.reverse(),
                    symbol: 'circle',
                    symbolSize: 10,
                    lineStyle: {
                        normal: {
                            color: ObtenerColores('Pastel1')[0],
                            width: 4/*,
                            type: 'dashed'*/
                        }
                    },
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[0]
                    },
                    xAxisIndex: 1
                }
            ],
            grid:{
                containLabel:true,
                right:'15%'
            }
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    xAxis: [
                        
                        {
                            
                            type: 'category',
                            data: datos.resultados.nombreCompradores.reverse(),
                            axisPointer: {
                                type: 'shadow',
                                label:{
                                    formatter:function(params){
                                        return  ObtenerParrafo(params.value,40);
                                    }
                                }
                            },
                            axisLabel:{
                                interval:0,
                                rotate:90,
                                showMinLabel:false,
                                padding:[0,0,0,0],
                                formatter:function(e){
                                    return ObtenerParrafo(e,40);
                                }
                            }
                        }
                    ],
                    yAxis: [
                        {   name:'Monto\nContratado',
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
                                        },
                            position:'bottom'
                        },
                        {
                            name:'Cantidad de \nProcesos de Contratación',
                            type: 'value',
                            axisLabel: {
                                formatter: '{value}',
                                rotate:295
                            },
                            position:'top'
                        }
                    ],
                    series: [
                        {
                            name:'Monto Contratado',
                            type:'bar',
                            data:datos.resultados.montoContratado.reverse(),
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[1]
                            },
                            label: {
                                normal: {
                                    show:false
                                    ,
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
                        },
                        {
                            name:'Cantidad de Procesos de Contratación',
                            type:'line',
                            //yAxisIndex: 1,
                            data:datos.resultados.cantidadOCIDs.reverse(),
                            symbol: 'circle',
                            symbolSize: 10,
                            lineStyle: {
                                normal: {
                                    color:ObtenerColores('Pastel1')[0],
                                    width: 4/*,
                                    type: 'dashed'*/
                                }
                            },
                            itemStyle:{
                                color:ObtenerColores('Pastel1')[0]
                            },
                            xAxisIndex: 0,
                            yAxisIndex: 1
                        }
                    ]
                    ,
                    grid:{
                        right:'10%',
                        left:0,
                        top:100
                    },
                    tooltip:{
                        position:['0%','50%']
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
 * Obtiene los datos e inicializa el gráfico de Top 10 Catálogo Electrónico por Monto Contratado
 */
function MontoCatalogoElectronico(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#montoCatalogoElectronico',true);
$.get(api+"/indicadoresoncae/catalogos/",parametros).done(function( datos ) {
OcultarReloj('#montoCatalogoElectronico',true);
if(datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadProcesos)  && datos.resultados.cantidadProcesos.length==0){
    MostrarSinDatos('#montoCatalogoElectronico',true)
    return;
}
var grafico=echarts.init(document.getElementById('montoCatalogoElectronico'));
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
    
    
                    
                    var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
    
                    e.forEach(function(valor,indice){
                        cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value) :ValorNumerico(valor.value) ) +' '+(valor.seriesIndex==0?'HNL':'Contratos')+'<br>'
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
                                /*min: 0,
                                max: 810,*/
                                //interval: 100000,
                                axisLabel: {
                                    formatter: '{value} HNL',
                                    rotate:45,
                            showMinLabel:false
                                },
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
                                    }
                                },
                    position:'bottom',
                    name:'Monto de Contratos'
                },
                {
                    type: 'value',
                    axisLabel: {
                        formatter: '{value}'
                    },
                    position:'top',
                    name:'Cantidad de Contratos'
                }
            ],
            yAxis: [
                {
                    
                    type: 'category',
                    data: datos.resultados.nombreCatalogos.reverse(),
                    axisPointer: {
                        type: 'shadow',
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                    },axisLabel: {
                        //rotate:45
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        }
                    },
                    name:'Catálogo Electrónico'
                    
                }
            ],
            series: [
                {
                    name:'Monto de Contrato',
                    type:'bar',
                    data:datos.resultados.montoContratado.reverse(),
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[2]
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
                            },
                            color:'gray'
                        }
                    },
                    barWidth:30,
                    barCategoryGap:'20%',
                    barGap:'50%'
                },
                {
                    name:'Cantidad de Contratos',
                    type:'line',
                    //yAxisIndex: 1,
                    data:datos.resultados.cantidadProcesos.reverse(),
                    symbol: 'circle',
                    symbolSize: 10,
                    lineStyle: {
                        normal: {
                            color: ObtenerColores('Pastel1')[9],
                            width: 4/*,
                            type: 'dashed'*/
                        }
                    },
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[9]
                    },
                    xAxisIndex: 1
                }
            ],
            grid:{
                containLabel:true,
                right:'15%'
            }/*,
            label:{
                show:true,
                fontFamily:'Poppins',
                fontWeight:700,
                fontSize:15,
                color:'gray'
            }*/
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    xAxis: [
                        {
                            
                            type: 'category',
                            data: datos.resultados.nombreCatalogos.reverse(),
                            axisPointer: {
                                type: 'shadow',
                                label:{
                                    formatter:function(params){
                                        return  ObtenerParrafo(params.value,40);
                                    }
                                }
                            },
                            axisLabel: {
                                interval:0,
                                rotate:90,
                                showMinLabel:false,
                                padding:[0,0,0,0],
                                formatter:function(e){
                                    return ObtenerParrafo(e,40);
                                }
                                
                            },
                            name:'Catálogo\nElectrónico'
                            
                        }
                    ],
                    yAxis: [
                        {
                            name:'Monto de\nContrato',
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
                            },
                            position:'bottom'
                        },
                        {
                            name:'Cantidad de\nContratos',
                            type: 'value',
                            axisLabel: {
                                formatter: '{value}',
                                rotate:295
                            },
                            position:'top'
                        }
                        
                    ],
                    series: [
                        {
                            name:'Monto de\nContrato',
                            type:'bar',
                            data:datos.resultados.montoContratado.reverse(),
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[2]
                            },
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
                            barWidth:20,
                            barCategoryGap:'10%',
                            barGap:'10%'
                        },
                        {
                            name:'Cantidad de\nContratos',
                            type:'line',
                            //yAxisIndex: 1,
                            data:datos.resultados.cantidadProcesos.reverse(),
                            symbol: 'circle',
                            symbolSize: 10,
                            lineStyle: {
                                normal: {
                                    color:ObtenerColores('Pastel1')[9],
                                    width: 4/*,
                                    type: 'dashed'*/
                                }
                            },
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[9]
                            },
                            xAxisIndex: 0,
                            yAxisIndex: 1
                        }
                    ],
                    grid:{
                        right:'10%',
                        left:0,
                        top:100
                    },
                    tooltip:{
                        position:['0%','50%']
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
})

window.onpopstate = function(e){
    location.reload();
  }

/**
 * Ejecuta la carga de todos los gráficos
 */
function CargarGraficos(){
    $('.contenedorGrafico > .grafico').each(function(i,elemento){
        if(echarts.getInstanceByDom(elemento)){
            echarts.getInstanceByDom(elemento).clear();
        }
    });
    CantidadContratosTipoContrato();
    MontoContratosTipoContrato();
    Top10InstitucionesMontos();
    MontoCatalogoElectronico();
    MontoCompraConjunta();
    ModalidadMontoCantidad();
}


/**
 * Obtiene un Json con los filtros aplicados según la url, recibe un json inicial por si se quiere setear el valor de algun grafico que no este todavía en la url
 */
function ObtenerJsonFiltrosAplicados(parametros){
    if(Validar(ObtenerValor('moneda'))){
        parametros['moneda']=decodeURIComponent(ObtenerValor('moneda'));
    }
    if(Validar(ObtenerValor('idinstitucion'))){
    parametros['idinstitucion']=decodeURIComponent(ObtenerValor('idinstitucion'));
    }
    if(Validar(ObtenerValor('año'))){
      parametros['año']=decodeURIComponent(ObtenerValor('año'));
    }
    if(Validar(ObtenerValor('proveedor'))){
        parametros['proveedor']=decodeURIComponent(ObtenerValor('proveedor'));
    }
    if(Validar(ObtenerValor('categoria'))){
      parametros['categoria']=decodeURIComponent(ObtenerValor('categoria'));
    }
    if(Validar(ObtenerValor('modalidad'))){
        parametros['modalidad']=decodeURIComponent(ObtenerValor('modalidad'));
    }
    if(Validar(ObtenerValor('sistema'))){
        parametros['sistema']=decodeURIComponent(ObtenerValor('sistema'));
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
 */
function ObtenerFiltros(){
    var parametros=ObtenerJsonFiltrosAplicados({});
    $.get(api+"/dashboardoncae/filtros/",parametros).done(function( datos ) {
  
    MostrarListaElastica(datos,'#elastic-list');
    MostrarEtiquetaListaElasticaAplicada();
    MostrarListaElasticaAplicados();
  
  
      
        
      }).fail(function() {
          
          
        });

}

/**
 * Obtiene los datos de los filtros a mostrar
 * @param {Object} opciones - objeto de filtros aplicados
 * @param {boolean} desUrl - descartar parametro de la url en caso de que este y no se haya recibido un json de opciones
 * @return {string}
 */
function AccederUrlPagina(opciones,desUrl){
    var direccion=('/indicadoresProcesosContratacion/?'+
    (ValidarCadena(opciones.moneda)? '&moneda='+encodeURIComponent(opciones.moneda): (ValidarCadena(ObtenerValor('moneda'))&&!desUrl?'&moneda='+ObtenerValor('moneda'):''))+
    (ValidarCadena(opciones.idinstitucion)? '&idinstitucion='+encodeURIComponent(opciones.idinstitucion): (ValidarCadena(ObtenerValor('idinstitucion'))&&!desUrl?'&idinstitucion='+ObtenerValor('idinstitucion'):''))+
   
    (ValidarCadena(opciones.año)? '&año='+encodeURIComponent(opciones.año): (ValidarCadena(ObtenerValor('año'))&&!desUrl?'&año='+ObtenerValor('año'):''))+
    (ValidarCadena(opciones.proveedor)? '&proveedor='+encodeURIComponent(opciones.proveedor): (ValidarCadena(ObtenerValor('proveedor'))&&!desUrl?'&proveedor='+ObtenerValor('proveedor'):''))+
    (ValidarCadena(opciones.categoria)? '&categoria='+encodeURIComponent(opciones.categoria): (ValidarCadena(ObtenerValor('categoria'))&&!desUrl?'&categoria='+ObtenerValor('categoria'):''))+
    (ValidarCadena(opciones.modalidad) ? '&modalidad='+encodeURIComponent(opciones.modalidad):(ValidarCadena(ObtenerValor('modalidad'))&&!desUrl?'&modalidad='+ObtenerValor('modalidad'):''))+
    (ValidarCadena(opciones.sistema) ? '&sistema='+encodeURIComponent(opciones.sistema):(ValidarCadena(ObtenerValor('sistema'))&&!desUrl?'&sistema='+ObtenerValor('sistema'):''))+
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
      )
    });

    $('.filtrosContenedoFiltrosBusqueda').attr('style','height:calc(100vh - '+($('#extencionFiltrosAplicados').height()?123:110)+'px - '+($('#extencionFiltrosAplicados').height() + ($('#extencionFiltrosAplicados').height()?4:0))+'px)')

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
      $('ul#ul'+ filtrosAplicablesR[llave].parametro ).find(
        'li[valor="'+(valor).toString()+'"]'
      ).addClass('active');
    });
  }

  /**
   * Muestra el contenido de los filtros aplicados, las opciones de selección
   */
  function MostrarListaElastica(datos,selector){
    $(selector).html('');
    $.each(datos.respuesta,function(llave,valor){
      $(selector).append(
        $('<div class="list-container col-md-12 2">').append(
          $('<div class="panel panel-default ">').append(
            $('<div class="panel-heading">').text(
              filtrosAplicables[llave]?filtrosAplicables[llave].titulo:llave
            ),
            $('<input>',{type:'text', class:'elastic-filter',placeholder:filtrosAplicables[llave]?filtrosAplicables[llave].titulo:llave ,filtro:llave,on:{
              keyup:function(e){
                var texto=$(e.currentTarget).val();
                if (texto.length > 0) {
                  texto = texto.toLocaleLowerCase();
                  var regla = " ul#" + 'ul'+llave + ' li[formato*="' + texto + '"]{display:block;} ';
                  regla += " ul#" + 'ul'+llave + ' li:not([formato*="' + texto + '"]){display:none;}';
                  $('#style'+llave).html(regla);
                } else {
                  $('#style'+llave).html('');
                }
              }
            }}),
            $('<style>',{id:'style'+llave}),
            $('<ul >',{class:'list-group',id:'ul'+llave}).append(
              AgregarPropiedadesListaElastica(valor,llave)
            ),
            ['instituciones','proveedores'].includes(llave)&&valor&&valor.length>=50?
              $('<a>',{
                class:'enlaceTablaGeneral ptextoColorPrimario pcursorMano',
                href:'javascript:void(0)',
                style:'width:150px;padding:5px 15px',
                text: valor.length==50? 'Mostrar Todos...':'Mostrar Menos...',
                toolTexto:valor.length==50?'Mostrar más resultados':'Mostrar menos resultados',
                toolCursor:'true',
                llave:llave,
                on:{
                  click:MostrarMasResultados
                }
              })
            :null
              
            
          )
        )
      )
      
      
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
    $.each(valor,function(i,propiedades){
      if(Validar(propiedades.contratos)&&(ObtenerNumero(propiedades.contratos) == 0)){
          return;
      }
      elementos.push(
        $('<li >',{
        class:'list-group-item',
        valor:propiedades[ValoresLlaves(llave).codigo]?propiedades[ValoresLlaves(llave).codigo]:propiedades.key, 
        formato: (ObtenerTexto(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ ValoresLlaves(llave).valor])).toString().toLowerCase(),'llave':llave,
        
        on:{
          click:function(e){
            var filtro=$(e.currentTarget);
            if(filtro.hasClass('active')){
              filtro.removeClass('active')
            }else{
              filtro.parent().find('.list-group-item.active').removeClass('active');
              filtro.addClass('active');
            }
            /*var filtros={
            };
            $('li.list-group-item.active').each(function(cla,val){
              filtros[filtrosAplicables[$(val).attr('llave')]?filtrosAplicables[$(val).attr('llave')].parametro:'' ]=$(val).attr('valor');
            });*/
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
          $('<div>',{
            class:'badge',
            toolTexto: (propiedades.procesos||propiedades.contratos)?('Procesos: '+ValorNumerico(propiedades.procesos)+'<br>Contratos: '+ValorNumerico(propiedades.contratos)):'OCID',
            text:(ValoresLlaves(llave).cantidad)=='procesos'?ValorNumerico(propiedades[ValoresLlaves(llave).cantidad]===0?propiedades['contratos']:propiedades[ValoresLlaves(llave).cantidad]):ValorNumerico(propiedades[ValoresLlaves(llave).cantidad]) 
          }),
          $('<div >',{
          class:'elastic-data',
          toolTexto:(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ValoresLlaves(llave).valor]),
          toolCursor:'true',
          
          text:(traducciones[propiedades[ValoresLlaves(llave).valor]]?traducciones[propiedades[ValoresLlaves(llave).valor]].titulo:propiedades[ValoresLlaves(llave).valor])}
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
            //$('<style>',{id:'style'+elemento.llave}),
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
 * Obtiene los datos e inicializa el gráfico de compra conjunta por montos de contrato
 */
function MontoCompraConjunta(){
    var parametros={}
    parametros=ObtenerJsonFiltrosAplicados(parametros);
    MostrarReloj('#montoCompraConjunta',true);
$.get(api+"/indicadoresoncae/comprasconjuntas/",parametros).done(function( datos ) {

    
OcultarReloj('#montoCompraConjunta',true);
if(datos&&datos.resultados&&Array.isArray(datos.resultados.cantidadProcesos)  && datos.resultados.cantidadProcesos.length==0){
    MostrarSinDatos('#montoCompraConjunta',true)
    return;
}
var grafico=echarts.init(document.getElementById('montoCompraConjunta'));
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
    
    
                    
                    var cadena=ObtenerParrafo(e[0].name,40).replace(/\n/g,'<br>')+'<br>';
    
                    e.forEach(function(valor,indice){
                        cadena=cadena+' '+valor.marker+' '+valor.seriesName+' '+(valor.seriesIndex==0?ValorMoneda(valor.value) :ValorNumerico(valor.value) ) +' '+(valor.seriesIndex==0?'HNL':'Contratos')+'<br>'
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
                                /*min: 0,
                                max: 810,*/
                                //interval: 100000,
                                axisLabel: {
                                    formatter: '{value} HNL',
                                    rotate:45,
                            showMinLabel:false
                                },
                                axisPointer: {
                                    label: {
                                        formatter: '{value} HNL'
                                    }
                                },
                    position:'bottom',
                    name:'Monto de Contratos'
                },
                {
                    type: 'value',
                    axisLabel: {
                        formatter: '{value}'
                    },
                    position:'top',
                    name:'Cantidad de Contratos'
                }
            ],
            yAxis: [
                {
                    
                    type: 'category',
                    data: datos.resultados.nombreCatalogos.reverse(),
                    axisPointer: {
                        type: 'shadow',
                        label:{
                            formatter:function(params){
                                return  ObtenerParrafo(params.value,40);
                            }
                        }
                    },axisLabel: {
                        //rotate:45
                        formatter:function(e){
                            return ObtenerParrafo(e,30);
                        }
                    },
                    name:'Compra Conjunta'
                    
                }
            ],
            series: [
                {
                    name:'Monto de Contrato',
                    type:'bar',
                    data:datos.resultados.montoContratado.reverse(),
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[4]
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
                            },
                            color:'gray'
                        }
                    },
                    barWidth:30,
                    barCategoryGap:'20%',
                    barGap:'50%'
                },
                {
                    name:'Cantidad de Contratos',
                    type:'line',
                    //yAxisIndex: 1,
                    data:datos.resultados.cantidadProcesos.reverse(),
                    symbol: 'circle',
                    symbolSize: 10,
                    lineStyle: {
                        normal: {
                            color: ObtenerColores('Pastel1')[9],
                            width: 4/*,
                            type: 'dashed'*/
                        }
                    },
                    itemStyle:{
                        color: ObtenerColores('Pastel1')[9]
                    },
                    xAxisIndex: 1
                }
            ],
            grid:{
                containLabel:true,
                right:'15%'
            }
        },
        media:[
            {
                query:{
                    maxWidth:600
                },
                option:{
                    xAxis: [
                        {
                            
                            type: 'category',
                            data: datos.resultados.nombreCatalogos.reverse(),
                            axisPointer: {
                                type: 'shadow',
                                label:{
                                    formatter:function(params){
                                        return  ObtenerParrafo(params.value,40);
                                    }
                                }
                            },
                            axisLabel: {
                                interval:0,
                                rotate:90,
                                showMinLabel:false,
                                padding:[0,0,0,0],
                                formatter:function(e){
                                    return ObtenerParrafo(e,40);
                                }
                            },
                            name:'Compra\nConjunta'
                            
                        },
                        {
                           
                            name:''
                        }
                    ],
                    yAxis: [
                        {
                            name:'Monto de\nContrato',
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
                            },
                            position:'bottom'
                        },
                        {
                            name:'Cantidad de\nContratos',
                            type: 'value',
                            axisLabel: {
                                formatter: '{value}',
                                rotate:295
                            },
                            position:'top'
                        }
                        
                    ],
                    series: [
                        {
                            name:'Monto de\nContrato',
                            type:'bar',
                            data:datos.resultados.montoContratado.reverse(),
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[2]
                            },
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
                            barWidth:20,
                            barCategoryGap:'10%',
                            barGap:'10%'
                        },
                        {
                            name:'Cantidad de\nContratos',
                            type:'line',
                            //yAxisIndex: 1,
                            data:datos.resultados.cantidadProcesos.reverse(),
                            symbol: 'circle',
                            symbolSize: 10,
                            lineStyle: {
                                normal: {
                                    color:ObtenerColores('Pastel1')[9],
                                    width: 4/*,
                                    type: 'dashed'*/
                                }
                            },
                            itemStyle:{
                                color: ObtenerColores('Pastel1')[9]
                            },
                            xAxisIndex: 0,
                            yAxisIndex: 1
                        }
                    ],
                    grid:{
                        right:'10%',
                        left:0,
                        top:100
                    },
                    tooltip:{
                        position:['0%','50%']
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