function MostrarContratosInstitucion(todos,inicio) {
    var filtros=ObtenerJsonFiltrosAplicados({});
    if(!filtros.institucion){
        return;
	}
	if($('#PesosContratosGrafico').length){
		$('#PesosContratosGrafico').empty();
	}
	$('#PesosContratos').html('');
	$('#PesosContratos').show();
	MostrarEspera('#PesosContratos',true);
	if(!inicio){
		$('html,body').animate({ scrollTop: $('#PesosContratos').position().top}, 'slow');
	}
	
    $.get(api + "/compradores/" + encodeURIComponent(filtros.institucion) + '/contratos',{tid:'id',year:filtros.año,dependencias:1,pagina:1,paginarPor: todos?todos:250,ordenarPor:'desc(montoCon)'}).done(function(datos) {
        console.dir('DATOS JSON COMRPADOR')
       
		console.dir(datos)
		
		OcultarEspera('#PesosContratos');
		var resultados=[];
		
        if(datos&&datos.resultados&&datos.resultados.length){
            datos.resultados.forEach(function(elemento){
                resultados.push(
                    {
                        id:elemento._source.extra.ocid,
                        codigo_contratacion:elemento._source.id,
                        convocante:elemento._source.buyer.name,
                        monto_adjudicado:elemento._source.extra&&elemento._source.extra.LocalCurrency?elemento._source.extra.LocalCurrency.amount:0,
                        razon_social:(elemento._source.suppliers&&elemento._source.suppliers.length)?elemento._source.suppliers.map(function(e){return e.name;}).join(', '):'',
                        fecha_firma_contrato:elemento._source.period?elemento._source.period.startDate:(elemento._source.dateSigned?elemento._source.dateSigned:''),
						nombre_licitacion:(elemento._source.title?elemento._source.title:'')+((elemento._source.title&&elemento._source.description)?', ':'')+elemento._source.description?elemento._source.description:'',
						institucion:elemento._source.extra&&elemento._source.extra.parentTop?elemento._source.extra.parentTop.name:'',
						codigo_institucion:filtros.institucion,
						tipo_procedimiento: elemento._source.extra&&elemento._source.extra.tenderProcurementMethodDetails?elemento._source.extra.tenderProcurementMethodDetails:'Sin Modalidad',
                        objeto_licitacion:elemento._source.localProcurementCategory?(traducciones[elemento._source.localProcurementCategory]?traducciones[elemento._source.localProcurementCategory].titulo:elemento._source.localProcurementCategory):'Sin Categoría',
                        
                        "ruc":"80013889-9",
                        "categoria_codigo":"24",
                        "categoria":"Equipos, accesorios y programas computacionales, de oficina, educativos, de imprenta, de comunicación y señalamiento",
                        "tipo_procedimiento_codigo":"ND",
                        "_objeto_licitacion":"BIEN",
                        "_moneda":"PYG",
						"id_llamado":"360669"
                    }
				);
				if(!elemento._source.period){
					//console.dir(elemento)
				}
				/*if(!(elemento._source.extra&&elemento._source.extra.LocalCurrency)){
					console.dir(elemento)
				}*/
            });
        }

        initForce({contratos:resultados,cantidad:datos.paginador['total.items']});
    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');

    });
}




function MostrarContratosGenerales(){
    $('#ContratosGeneralesGrafico').empty();
    MostrarEspera('#ContratosGenerales',true);
	var parametros=ObtenerJsonFiltrosAplicados({});
	

	if(!parametros.año){
			alerty.toasts('Selecciona un Año', {place: 'top',time:2000});
			OcultarEspera('#ContratosGenerales');
			return;
	}
    $.get(api+"/visualizacionesoncae/instituciones/",{anio:parametros.año}).done(function(datos) {

		if(datos&&datos.resultados&&datos.resultados.length){
			datos.resultados=datos.resultados.sort(function(a, b){return ((a.nombre > b.nombre) ? 1 : -1);});
		}
        OcultarEspera('#ContratosGenerales');
        DibujarContratosGenerales(datos.resultados,'Cantidad de Contratos por Institución');

    }).fail(function() {
        /*Error de Conexion al servidor */
        console.dir('error de api');

    });
    
}
var anioActual = ((new Date().getFullYear())), nivelActual = "poder-ejecutivo", entidadActual, chart = null, popactual = null, data, height;
var es_ES = {
	"decimal" : ".",
	"thousands" : "\xa0",
	"grouping" : [ 3 ],
	"currency" : [ "", " руб." ],
	"dateTime" : "%A, %e %B %Y г. %X",
	"date" : "%d.%m.%Y",
	"time" : "%H:%M:%S",
	"periods" : [ "AM", "PM" ],
	"days" : [ "Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes",
			"Sábado" ],
	"shortDays" : [ "Dom", "Lun", "Mar", "Mie", "Jue", "Vie", "Sab" ],
	"months" : [ "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
			"Julio", "Agosto", "Septiembre", "Octubre", "Noviembre",
			"Diciembre" ],
	"shortMonths" : [ "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago",
			"Sep", "Oct", "Nov", "Dic" ]
}
var ES = d3.locale(es_ES);
var width2 = 960, 
	height2 = 200,
	rectWidth = 20, 
	rectHeight = 10, 
	nodeStrokeW = 0.5, 
	padding = 1, 
	legendRectSize = 18;
	legendSpacing = 4;
    var maximoBurbujas = 250;
    var nodes, labels;
var chart = null, force = null, dataChart2, dataChart2Complete, tiposDeProcedimiento, categorias;
var padding = 5;
var maxRadius = d3.max(_.pluck(dataChart2, 'r'));

function MostrarVarianteContrato(e){

	$(e.currentTarget).parent().find('a').removeClass('activo');
	$(e.currentTarget).addClass('activo');
	return $(e.currentTarget).attr('mostrar');
}
var initForce = function(datos) {
	//console.dir(datos)
	if(!datos.contratos[0]){
		alerty.toasts('No hay Contratos de esta Institución', {place: 'top',time:2000});
		return;
	}
	var selector='PesosContratos';
	if(!$('#PesosContratosGrafico').length){
        $('#'+selector).append(
			$('<h4>',{class:'titularCajonSombreado textoAlineadoCentrado mt-2',text:datos.contratos[0].institucion}),
			$('<div>',{id:'SubTituloGrafico'}),
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
										$('<b>',{class:'textoColorGrisNormal',text:'Selecciona algún criterio como ser: Contratos, Dependencias, Modalidad de Compra.'})
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
												$('<b>',{class:'textoColorGrisNormal',text:'Posicionando el mouse sobre un contrato se pueden ver más detalles y haciendo click se puede ver el proceso de contratación en la sección de este contrato.'})
											)
											)
									)
								)
							)
						)
			
			),
			
			$('<div>',{class:'metodoBusquedaContenedor mt-3 mb-3 textoAlineadoCentrado visualizacion',style:'width: 100%;max-width: 400px;margin:0 auto',id:"SeleccionContratos"}).append(
				$('<div>',{class:'btn-group sombraBasica',role:'group','aria-label':'Mostrar Contratos'}).append(
					$('<a>',{href:'javascript:void(0)',class:'btn btn-primary fondoColorSecundario activo', mostrar:'contratos', on:{
						'click':function(e){ 
							MostrarVarianteContrato(e);
							initForce(datos);
						}
					}}).append(
						$('<span>',{text:'Contratos'})
					),
					$('<a>',{href:'javascript:void(0)',class:'btn btn-primary fondoColorSecundario', mostrar:'convocante', on:{
						'click':function(e){
							groupBy(MostrarVarianteContrato(e),datos);
						}
					}}).append(
						$('<span>',{text:'Dependencias'})
					),
					$('<a>',{href:'javascript:void(0)',class:'btn btn-primary fondoColorSecundario', mostrar:'tipo_procedimiento', on:{
						'click':function(e){
							groupBy(MostrarVarianteContrato(e),datos);
						}
					}}).append(
						$('<span>',{text:'Modalidad de Compra'})
					)
				)
			),
            $('<div>',{id:'PesosContratosGrafico'})
        );
    }else{
		$('#PesosContratosGrafico').empty();
	}
	if (force) {
       force.stop();
    }
	var totalHeight = height2 + 200;
	/*if(showAll){
	    datos.contratos = datos.contratosComplete;
	}else{
		datos.contratos = _.chain(datos.contratos).sortBy(function(d){ return d.monto_adjudicado; }).take(maximoBurbujas).value();
	}*/
	var totalContratos = datos.contratos.length;
	/*var maxElem = d3.max(datos.contratos, function(d) {
		return d.monto_adjudicado;
	})
	var area = d3.scale.sqrt().domain([ 0, maxElem ]).range([ 0, 10 ]);*/
	var montos = _.map(datos.contratos, function(elem){
			var now = elem.fecha_firma_contrato;
			var test = now.substring(0,10);
			var m = moment(test);
			var montoEscalable;
			//if (elem._moneda == "PYG") {
				montoEscalable = elem.monto_adjudicado;
			/*} else {
				var anio = m.year();
				montoEscalable = elem.monto_adjudicado * anioCambio[anio];
			}*/
			//return parseInt(elem.monto_adjudicado)
			return parseInt(montoEscalable)
		});
	var rScale = d3.scale.sqrt().domain([0, _.max(montos)]).range([3, 25]);

	for (var j = 0; j < datos.contratos.length; j++) {
		/* La x se genera en base a la fecha */
		var now = datos.contratos[j].fecha_firma_contrato;
		var test = now.substring(0,10);
		var m = moment(test);
		var day = m.dayOfYear() - 1;
		
		// escalar el monto segun la moneda
		var montoEscalable;
		//if (datos.contratos[j]._moneda == "PYG") {
			montoEscalable = datos.contratos[j].monto_adjudicado;
		/*} else {
			var anio = m.year();
			montoEscalable = datos.contratos[j].monto_adjudicado * anioCambio[anio];
		}*/
		
		//datos.contratos[j].r = rScale(datos.contratos[j].monto_adjudicado);
		datos.contratos[j].r = rScale(montoEscalable);
		datos.contratos[j].x = day;
		datos.contratos[j].y = Math.random() * height2;
		datos.contratos[j].dia = day; //seterle el dia segun la fecha
		datos.contratos[j].mes = m.month(); //setearle el mes segun la fecha
		//console.log(datos.contratos[j].monto_adjudicado);
		//console.log(datos.contratos[j]._moneda);
		//console.log(datos.contratos[j].monto_escalable);
    }
	
	chart = d3.select("#"+selector+'Grafico')
		.append("svg")
			.style("width", "1080px")
		  	.attr("height", totalHeight)
		  	.attr("pointer-events", "all")
	  	.append("g")
	    	.attr("transform", "translate(70,10)")
	  	.append('g');

	chart.append('rect')
	   	.style('width', width2)
	   	.attr('height', totalHeight)
	   	.attr('fill', 'white');
	
	var xScale = d3.scale.linear().domain([ 0, 366 ]).range([ 0, width2 ])

	var xScaleTime = d3.scale.ordinal()
			.domain(["Enero", "Abril", "Agosto", "Diciembre"])
			.rangePoints([0, width2]);
			//.range([ 0, width2 ]);
	   
	var axis = d3.svg.axis().scale(xScaleTime).orient("bottom")
	
	var xAxis = chart.append("g")
		.attr("class", "x axis")
		//.style({ 'stroke': '#000', 'fill': 'none', 'stroke-width': '0.2'})
		.call(axis)
		
	xAxis.selectAll('text')
		.style({ 'stroke-width': '0.1', 'fill': '#000'});

    drawLines(1, width2);
    drawLegend(chart, totalHeight);
    
	nodes = chart.selectAll("circle").data(datos.contratos);
	nodes.enter().append("circle")
		.attr("class", "node")
	    .attr("cx", function(d) { return xScale(d.x) })
	    .attr("cy", function(d) { return d.y })
	    .attr("r", function(d) { return d.r })
		.style("fill", function(d) {
			return  ObtenerColor(d.objeto_licitacion);})
		  .style("stroke", "black")
		.style("cursor", "pointer")
		.style("stroke-width", nodeStrokeW)
      	.style("display", function(){ return (datos.contratos.length > maximoBurbujas) ? "none" : "block"; })
      	//.on("mouseover", function (d) { showPopover.call(this, d); })
		//.on("mouseout",	function (d) { removePopovers(); })
		.on("mouseover", mouseover)
		.on("mouseout", mouseout)
		.on("mousemove", mousemove)
		.on("click", clickBubble)
	
   	force = d3.layout.force()
	    //.gravity(0)
	    //.charge(0)
	    .friction(0)
   		//.alpha(0.8);
	   	.nodes(datos.contratos)
	    .size([width2, totalHeight]);


    if(datos.contratos.length > maximoBurbujas){
        force.on("start", function(){
           // loadingSpinner.spin(document.getElementById(selector), {top: '85%'});
        });

        force.on("end", function(){
           // loadingSpinner.stop();
            nodes.style("display", "block");
        });
    }


    var msg = "Mostrando " + datos.contratos.length.toString() + " contratos de un total de " + datos.cantidad + ".";
    if(datos.contratos.length < datos.cantidad){
		console.dir(datos.contratos.length)
		console.dir(datos.cantidad)
        msg += " Ver todos los contratos haciendo click <a id='MostrarTodos' href='javascript:void(0)'>aquí</a>."
    }
    if(datos.contratos.length > maximoBurbujas){
        msg += " Ver los " + maximoBurbujas.toString() + " contratos con mayor monto adjudicado haciendo click <a id='MostrarAlgunos' href='javascript:void(0)'>aquí</a>."
    }
    $('.SubTituloGrafico').remove();
    $('#SubTituloGrafico').html('<div class="SubTituloGrafico tituloFiltrosAplicados textoColorGris" style="font-size:16px;color:#c4c4c4">'+ msg +'</div>');
    $('#MostrarTodos').unbind().click(function(e){
        MostrarContratosInstitucion(datos.cantidad,true);
    });
    $('#MostrarAlgunos').unbind().click(function(e){
		MostrarContratosInstitucion(false,true);
    });

	/* que se grafique en el tree layout con sus centros */
	var centers = getCenters("dia", [width2, height2],datos);
	force.on("tick", tick(centers, "dia",datos));
    force.start();
    
}

/* Línea de puntos en medio del gráfico y/o en cada agrupación */
function drawLines(number, newWidth, esAgrupacion){
    _.times(number, function(i){
    	var x1,x2,y;
    	if((number - 1) === 0){
    	    y = height2 * i + 200;
    	}else{
    	    y = height2 * i + 185 - (i * 17);
    	    if(y > 2500) y -= 12;
    	}

    	if (esAgrupacion) {
    		x1 = 50;
    		x2 = newWidth + 50;
    	} else {
    		x1 = 0;
    		x2 = width2;
    	}
    	chart.append("line")
    	    .attr("class", 'd3-dp-line')
    	    .attr("x1", x1)
    	    .attr("y1", y)
    	    .attr("x2", x2)
    	    .attr("y2", y)
    	    .style("stroke-dasharray", ("1, 1"))
    	    .style("stroke-opacity", 0.9)
    	    .style("stroke-width", 0.3)
    	    .style("stroke", "#000");
    });
}

/* Leyenda de colores */
function drawLegend(chart, chartHeight){
	var hor = -100;
	var ver = chartHeight - 50;
	var legend = chart.selectAll('.legend').data(function(){var arreglo=[];$.each(traducciones,function(llave,valor){arreglo.push(valor.titulo);}); return arreglo;}).enter()
			.append('g').attr('class', 'legend').attr('transform',
					function(d, i) {
						hor = hor + 150;
						return 'translate(' + hor + ',' + ver + ')';
					});

	legend.append('rect')
	  	.attr('width', legendRectSize)
	  	.attr('height', legendRectSize - 10)
	  	.style('fill', function(d){return  ObtenerColor(d);})
	  	.style('stroke', function(d){return  ObtenerColor(d);});

	legend.append('text')
	  	.attr('x', legendRectSize + legendSpacing)
	  	.attr('y', legendRectSize - legendSpacing - 6)
			.text(function(d) { return d; });
}
/*
var objetosDelLlamado = [ "Bienes", "Consultorías", "Locación inmuebles",
		"Locación muebles", "Obras", "Servicios", "Tierras" ]

var nombreToCodigoObjetoLlamado = {
	"Bienes" : "BIEN",
	"Consultorías" : "CON",
	"Locación inmuebles" : "LOCINM",
	"Locación muebles" : "LOCMUE",
	"Obras" : "OBRAS",
	"Servicios" : "SER",
	"Tierras" : "TIERRAS"
};

var objetosDelLlamadoMap = {
	"BIEN" : "#542788",
	"CON" : "#998EC3",
	"LOCINM" : "#D8DAEB",
	"LOCMUE" : "#FEE0B6",
	"OBRAS" : "#F1A340",
	"SER" : "#B35806",
	"TIERRAS" : "#ddd"
};*/
var tooltip = d3.select("body").append("div").style("position", "absolute")
		.style("z-index", "10").style("visibility", "hidden").style("display", "none").style("color",
				"white").style("padding", "8px").style("background-color",
				"rgba(0, 0, 0, 0.75)").style("border-radius", "6px").style(
				"font", "12px sans-serif").text("tooltip");
var alternatingColorScale = function(d,codigo) {
	
    if (codigo){
        return objetosDelLlamadoMap[d];
    } else {
        return objetosDelLlamadoMap[nombreToCodigoObjetoLlamado[d]];
    }

}
function ObtenerColor(d){
	return colores[d]?colores[d]:ObtenerColores('Pastel1')[5];
}

function mouseover(d) {
	tooltip.html("<strong>Proveedor </strong> " + d.razon_social + "<br>" +
			"<strong>Nombre </strong> " + d.nombre_licitacion + "<br>" +
			"<strong>Tipo de Contrato </strong> " + d.objeto_licitacion + "<br>" +
			//"<strong>Categoria </strong> " + d.categoria + "<br>" +
			"<strong>Modalidad de Compra </strong> " + d.tipo_procedimiento + "<br>" +
			"<strong>Código de contrato</strong> " + d.codigo_contratacion + "<br>" +
			"<strong>Fecha firma contrato </strong> " + moment(d.fecha_firma_contrato.split(" ")[0]).format('YYYY-MM-DD') + 
			"<br><strong>Monto</strong> " + ValorMoneda(d.monto_adjudicado)+ " HNL ")
			.style("visibility", "visible").style("display", "block");
			//.style("display", "static");
	//d3.select(this).style("fill-opacity", function(d) { return 0.8; });
	d3.select(this).style("stroke-width", "2px")
}

function mousemove(d) {
	tooltip.style("top",
		    (d3.event.pageY-10)+"px").style("left",(d3.event.pageX+10)+"px");
}

function mouseout(d) {
	tooltip.style("visibility", "hidden").style("display", "none");
	//d3.select(this).style("fill-opacity", function(d) { return 1; });
	d3.select(this).style("stroke-width", "0.5px");
}

function clickBubble(d) {

	window.open(url+'/proceso/'+d.id+'?contrato='+d.codigo_contratacion);

	    
}

/* Funcion para ubicar los centros - Variable de agrupacion: dia */
function getCenters (vname, size,datos) {
    var centersByDay, groups, centers;
    if(vname === 'dia'){
		centersByDay = getCentersByDay(size);
        //centers = centersByDay;
        centers = _.map(centersByDay, function(c){
         return {name: c.name, value: 1, dia: c.dia, x: c.x, y: c.y + 100, dx: c.dx, dy: c.dy }
        });
    }else{
        groups = _.uniq(_.pluck(datos.contratos, vname));
        labels = _.map(groups, _.clone);
        size[1] = size[1]/groups.length;
        centersByDay = getCentersByDay(size);
        centers = _.chain(centersByDay).map(function(c){
            return _.map(groups, function(g, i){
                var y = (c.y + i * height2 + 100);
                return {name: c.name.toString() + g, value: 1, dia: c.dia, x: c.x, y: y, dx: c.dx, dy: c.dy };
            });
        }).flatten().value();
    }
    return centers;  //D
};

function getCentersByDay (size){
    var centers, map;
	var circulitos;
	var filtros=ObtenerJsonFiltrosAplicados({});
    centers = _.range(EsBiciesto(filtros.año)?366:365).map(function (d) {
            return {name: d, value: 1, dia:d};
    });

    map = d3.layout.treemap().size(size).ratio(1/1).sort(function(a,b){return b.dia - a.dia}).mode("slice"); //B
    map.nodes({children: centers}); //C
    return centers; //D
};


/* Funcion para agrupar las burbujas hacia su centro */
function tick (centers, varname,datos) {
	//console.log(centers);
	
	//console.dir(datos.contratos)dia
    var foci = {}; // Making an object here for quick look-up
    for (var i = 0; i < centers.length; i++) {
      foci[centers[i].name] = centers[i];
    }
    return function (e) { //A
      for (var i = 0; i < datos.contratos.length; i++) {
		var o = datos.contratos[i];
        var key = (varname === 'dia') ?( o[varname]) : o.dia.toString() + o[varname];
        var f = foci[key];
        if(!f){
            console.log(o);
            console.log(f);
            console.log(foci);
            console.log(key);
        }
        o.y += ((f.y + (f.dy / 2)) - o.y) * e.alpha;
        o.x += ((f.x + (f.dx / 2)) - o.x) * e.alpha;
      }
      nodes.each(collide(.11,datos)) //B
        .attr("cx", function (d) { return d.x; })
        .attr("cy", function (d) { return d.y; });
    }
  }


  function collide(alpha,datos) {
    var quadtree = d3.geom.quadtree(datos.contratos);
    return function (d) {
      var r = d.r + maxRadius + padding,
          nx1 = d.x - r,
          nx2 = d.x + r,
          ny1 = d.y - r,
          ny2 = d.y + r;
      quadtree.visit(function(quad, x1, y1, x2, y2) {
        if (quad.point && (quad.point !== d) ) {
          var x = d.x - quad.point.x,
              y = d.y - quad.point.y,
              l = Math.sqrt(x * x + y * y),
              r = d.r + quad.point.r + padding;

            if (l < r) {
              l = (l - r) / l * alpha;
              d.x -= x *= l;
              d.y -= y *= l;
              quad.point.x += x;
              quad.point.y += y;
            }
        }

        return x1 > nx2 || x2 < nx1 || y1 > ny2 || y2 < ny1;
      });
    };
  }


  function DibujarContratosGenerales(datos, nivel, rScale) {
    var nestData = d3.nest().key(function(d) {
            var entAbr = getEntidadAbreviada(d.nombre.toUpperCase());
            return entAbr;
        
        }).rollup(function(rows) {
            return {
                total : d3.sum(rows, function(g) {
                    return g.cantidad;
                }),
                contratos : rows
            };
        }).entries(datos);

        initChart(nivel, rScale, nestData, nestData);
}


var initChart = function(nivel, rScale,datos, datosCompletos) {
    // console.log(data.length);
	var selector='ContratosGenerales';
    if(!$('#ContratosGeneralesGrafico').length){
        $('#'+selector).append(
			$('<h4>',{class:'titularCajonSombreado textoAlineadoCentrado',text:nivel}),
			
			$('<div>',{class:'campoAzulBusquedaPadre normal',style:'margin:0 auto; max-width:500px;width:100%;transform:scale(0.8)',id:"contenedorCampoBusqueda"}).append(
				$('<input>',{class:'form-control form-control-lg campoAzulBusqueda',type:'text',placeholder:'Busca una Institución..',id:'campoBusquedaInstitucion'
			}),
				$('<i>',{class:'fas fa-search cursorMano'})
			),
            $('<div>',{id:'ContratosGeneralesGrafico',style:'overflow-y: auto; height:500px;overflow-x: auto'})
        )
	}
	
	$('#campoBusquedaInstitucion').unbind().change(function(e){
		$('#ContratosGeneralesGrafico').empty();
					if(ValidarCadena($('#campoBusquedaInstitucion').val().trim())){
						var filtrados=datosCompletos.filter(function(e){
							var regular=new RegExp($('#campoBusquedaInstitucion').val().trim().toLowerCase(), "g");
							return regular.test(e.key.toString().toLowerCase());
						});
						if(filtrados.length==0){alerty.toasts('Sin Resultados', {place: 'top',time:2000});}
						initChart(nivel, rScale,filtrados,datosCompletos);
					}else{
						initChart(nivel, rScale,datosCompletos,datosCompletos);
					}
	});
	$('#contenedorCampoBusqueda i').unbind().click(function(e){
		$('#ContratosGeneralesGrafico').empty();
					if(ValidarCadena($('#campoBusquedaInstitucion').val().trim())){
						var filtrados=datosCompletos.filter(function(e){
							var regular=new RegExp($('#campoBusquedaInstitucion').val().trim().toLowerCase(), "g");
							return regular.test(e.key.toString().toLowerCase());
						});
						if(filtrados.length==0){alerty.toasts('Sin Resultados', {place: 'top',time:2000});}
						initChart(nivel, rScale,filtrados,datosCompletos);
					}else{
						initChart(nivel, rScale,datosCompletos,datosCompletos);
					}
	});
	var cant = datos.length;
	if (cant == 1 || cant == 2)
		height = 50 + cant * 55 + 50;
	else
		height = 50 + cant * 55;

	function truncate(str, maxLength, suffix) {
		if (str.length > maxLength) {
			str = str.substring(0, maxLength + 1);
			str = str.substring(0, Math.min(str.length, str.lastIndexOf(" ")));
			str = str + suffix;
		}
		return str;
	}

	var margin = {
		top : 20,
		right : 200,
		bottom : 0,
		left : 0
	}, width = 800;

	var start_year = 1, end_year = 12;

	var c = d3.scale.category20c();

	var x = d3.scale.linear().range([ 0, width ]);

	var xAxis = d3.svg.axis().scale(x).orient("top");
    var filtro=ObtenerJsonFiltrosAplicados({});
	var x2 = d3.time.scale().domain(
			[ new Date(filtro.año, 0, 1), new Date(filtro.año, 10, 31) ]).range(
			[ 0, width ]);

	var xAxis2 = d3.svg.axis().scale(x2).orient("top").ticks(d3.time.months)
			.tickSize(5, 0).tickFormat(ES.timeFormat("%B"));

	var formatYears = d3.format("0000");
	xAxis.tickFormat(formatYears);

	var marginleft = margin.left + 520;
	var svg = d3.select("#"+selector+'Grafico').append("svg").style("width",
			(1250 + margin.left + margin.right)+'px').attr("height",
			height + margin.top + margin.bottom).style("margin-left",
			margin.left + "px").append("g").attr("transform",
			"translate(" + marginleft + "," + margin.top + ")");

	x.domain([ start_year, end_year ]);
	var xScale = d3.scale.linear().domain([ start_year, end_year ]).range(
			[ 0, width ]);

	svg.append("g").attr("class", "x axis").attr("transform",
			"translate(0," + 40 + ")").call(xAxis2);

    if(!rScale){
    	var contratos = _.map(datos, function(elem) {
    		return elem.values.contratos
    	});
    	var cantidades = _.chain(contratos).flatten().map(function(contrato) {
    		return parseInt(contrato.cantidad)
    	}).value();
    	rScale = d3.scale.sqrt().domain([ 0, _.max(cantidades) ]).range(
    			[ 0, 25 ]);
    }

	// console.log(_.max(cantidades));
    // console.log(data);
	for (var j = 0; j < datos.length; j++) {
		var g = svg.append("g").attr("class", "journal");

		var circles = g.selectAll("circle")
				.data(datos[j]['values']['contratos']).enter().append("circle");

		var text = g.selectAll("text").data(datos[j]['values']['contratos'])
				.enter().append("text");

		circles.attr("cx", function(d, i) {
			return xScale(d['mes']);
		}).attr("cy", j * 50 + 80).attr("r", function(d) {
			return rScale(d['cantidad']);
		}).style("fill", function(d) {
			return c(j);
		}).on("click", click).on("mouseover", mouseover).on("mouseout", mouseout);
		
		text.attr("y", j * 50 + 85).attr("x", function(d, i) {
			return xScale(d['mes']) - 5;
		}).attr("class", "value").text(function(d) {
			return d['cantidad'];
		}).style("fill", function(d) {
			return c(j);
		}).style("display", "none")
		.on("click", click).on("mouseover", mouseover).on("mouseout", mouseout);

		g.append("text").attr("y", j * 50 + 85).attr("x", -450).attr(
				"class", "label")
		// .text(truncate(data[j]['key'],40,"..."))
		.text(/*datos[j]['key']*/ truncate(datos[j]['key'],70,"...")).style("fill", function(d) {
			return c(j);
		}).on("click",click).on("mouseover", mouseover).on("mouseout", mouseout);
	};

    // chart title
	//svg.append("text").text(nivel).attr("x", width / 3).attr("y", 0).attr("class", "titleChartx titularCajonSombreado textoAlineadoCentrado").attr('style','fill:gray;margin-top:10px');
            
           /* 
            {   
        text:nivel,
        x:width / 3,
        y:0,
        class:"titleChart titularCajonSombreado textoAlineadoCentrado"
    }*/
            
            

	function mouseover(p) {
		var g = d3.select(this).node().parentNode;
		d3.select(g).selectAll("circle").attr("opacity", 0);
		d3.select(g).selectAll("text.value").style("display", "block").classed("active", true); 
	}

	function mouseout(p) {
		var g = d3.select(this).node().parentNode;
		d3.select(g).selectAll("circle").attr("opacity", 1);
		d3.select(g).selectAll("text.value").style("display", "none").classed("active", true);
	}

	function click(p) {
		/*
		 * Obtener el nombre de la entidad en la que hizo click para pasarle al
		 * grafico 2
		 */
        console.dir('HICE CLICK EN UN MINISTERIO');
        var datos =d3.select(d3.select(this).node().parentNode).select('circle').data()[0];
        var parametros=ObtenerJsonFiltrosAplicados({});
            parametros['institucion']=datos.codigo;
			PushDireccion(AccederUrlPagina(parametros));
			MostrarContratosInstitucion();
	}

}

function groupBy(vname,datos){
    var chart, groups, newWidth, totalHeight, centers, linesHeight;
    groups = _.uniq(_.pluck(datos.contratos, vname));
    newWidth = width2 - 150;
    totalHeight = (height2 - 17) * (groups.length - 1) + (2 * height2);
    linesHeight = totalHeight - (height2 + 100);
    //totalHeight = groups.length * height2;
    chart = d3.selectAll("#PesosContratosGrafico svg")
    			.attr("width", newWidth)
    		  	.attr("height", totalHeight);
    
    chart.select('#PesosContratosGrafico svg g')
    	.attr("transform", "translate(100,10)")
    
    chart.select('#PesosContratosGrafico svg rect')
    	.attr('width', newWidth)
        .attr('height', totalHeight);
	
    chart.select(".x.axis").remove();
    
    xScale = d3.scale.linear().domain([ 0, 366 ]).range([width2 - newWidth, width2])

    xScaleTime = d3.scale.ordinal()
    		.domain(["Enero", "Abril", "Agosto", "Diciembre"])
    		.rangePoints([width2 - newWidth, width2]);
    
	var axis = d3.svg.axis().scale(xScaleTime).orient("bottom")
	
	var xAxis = chart.append("g")
		.attr("class", "x axis")
		//.style({ 'stroke': '#000', 'fill': 'none', 'stroke-width': '0.2'})
		.call(axis)
		
	xAxis.selectAll('text')
		.style({ 'stroke-width': '0.1', 'fill': '#000'});
    
    chart.selectAll('line').remove();
    drawLines(groups.length, newWidth, true);
    
    chart.selectAll('.legend').remove();
    drawLegend(chart, totalHeight);
    
    centers = getCenters(vname, [newWidth + 105, linesHeight],datos);
    
    chart.selectAll('.label').remove();
    drawLabels(groups.length, vname);
    chart.selectAll('.label')
    	.call(wrap, 100);
    
    var foci = {}; // Making an object here for quick look-up
    for (var i = 0; i < centers.length; i++) {
        foci[centers[i].name] = centers[i];
    }
    force.on("tick", tick(centers, vname,datos));
    force.start();
//    chart.selectAll('circle').attr('cy', function(c){
//        var key = c.dia.toString() + c[vname];
//        console.log(c);
//        return c.y + foci[key].y;
//    });

}

function drawLabels(number, vname) {
	_.times(number, function(i){
        /* Labels para cada agrupación */
    	//var y = height2 * i + 200 - (i * 40);
    	if((number - 1) === 0){
            y = height2 * i + 215;
        }else{
            y = height2 * i + 205 - (i * 17);
            if(y > 2500) y -= 12;
        }
    	var strippedLabels = _.map(labels, function(l){
    		return l.split("/")[0];
    	});
    	
    	var g = chart.append("g").attr("class", "journal");
		g.append("text")
			.attr("y", y)
			.attr("x", -50)
			.attr("dy", 0)
    	    .attr("dx", 0)
			.attr("class", "label")
			//.call(wrap, labels[i]);
			.text(labels[i]);
    });
}

function wrap(text, width) {
	
	text.each(function() {
	  var text = d3.select(this),
		  words = text.text().split(/\s+/).reverse(),
		  diff;
	  //console.log(words);
	  diff = -40;
	  var word,
		  line = [],
		  lineNumber = 0,
		  lineHeight = 1.1, // ems
		  y = parseFloat(text.attr("y")),
		  dy = parseFloat(text.attr("dy")),
		  //dx = parseFloat(text.attr("dx")),
	  y = y + diff;
	  var tspan = text.text(null).append("tspan").attr("x", -50).attr("y", y).attr("dy", dy + "em");
	 
	  while (word = words.pop()) {
		line.push(word);
		tspan.text(line.join(" "));
		if (tspan.node().getComputedTextLength() > width) {
		  line.pop();
		  tspan.text(line.join(" "));
		  line = [word];
		  tspan = text.append("tspan").attr("x", -50).attr("y", y).attr("dy", ++lineNumber * lineHeight + dy + "em").text(word);
		}
	  }
	});
  }