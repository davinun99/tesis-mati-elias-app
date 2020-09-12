/**
 * @file general.js Este archivo de incluye en todas la páginas del sitio y contiene funciones que pueden ser utilizadas en todas las secciones
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */
/**
 * variable url global
 * @type {string} url
 */
var url=window.location.origin;

/**
 * variable url de la api global
 * @type {string} api
 */
var api=url+"/api";

/**
 * Objeto para traducir los estados de un contrato
 * @type {Object} estadosContrato
 */
var estadosContrato={
    'pending':{titulo:'Pendiente',descripcion:'Este contrato se propuso pero aún no entra en vigor. Puede estar esperando ser firmado.'},
    'active':{titulo:'Activo',descripcion:'Este contrato se ha firmado por todas las partes y ahora está legalmente en proceso.'},
    'cancelled':{titulo:'Cancelado',descripcion:'Este contrato se canceló antes de ser firmado.'},
    'unsuccessful':{titulo:'Sin Éxito',descripcion:'Este contrato se firmo y entro en vigor, ahora esta cerca de cerrarse. Esto puede ser debido a la terminación exitosa del contrato, o puede ser una terminación temprana debido a que no fue finalizado.'},
    'terminated':{
        titulo:'Terminado',
        descripcion:'Este contrato se firmo y entro en vigor, ahora esta cerca de cerrarse. Esto puede ser debido a la terminación exitosa del contrato, o puede ser una terminación temprana debido a que no fue finalizado.'
    }
   };
/**
 * Moneda por defecto de a mostrar
 * @type {string} defaultMoneda
 */   
var defaultMoneda='PYG';

/**
 * Nombres de meses en el transcurso de un año
 * @type {string[]} meses
 */   
var meses=['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];

/**
 * Obtiene el nombre de un mes, recibiendo el numero del mes 
 * @param {string} numero - El numero del mes empezando por 01 hasta 12.
 * @return {string} El nombre de un mes
 * @example 
 *      ObtenerMesNombre('01')
 */
function ObtenerMesNombre(numero){
    numero=ObtenerNumero(numero);
    if(numero > 0 && numero < 13){
        return meses[numero - 1];
    }else{
        return '';
    }

}
/**
 * Inicializa el tour con INTRO JS 
 * @example 
 *      MostrarIntroduccion()
 */
function MostrarIntroduccion(){
    introJs().setOption("nextLabel", "Siguiente").setOption("prevLabel", "Atras").setOption("skipLabel", "SALTAR").setOption("doneLabel", "LISTO").start();
}

/**
 * Imprime un momento de la fecha, util para verificar tiempos a la hora de hacer debug
 * @example 
 *       DebugFecha()
 */
function DebugFecha(){
    let d =new Date();
    let fecha=d.getFullYear()+'-'+(d.getMonth()+1)+'-'+d.getDate()+' '+d.getHours()+':'+d.getMinutes()+':'+d.getSeconds()+':'+d.getMilliseconds();
    console.dir(fecha);
}

/**
 * Obtiene un arreglo de las paletas de colores creadas en base a un nombre 
 * @param {string} paleta - El nombre de la plateta que se desea obtener. 
 * @return {string[]}  ['#57C5CB','#DA517A','#FECB7E','#F79A6A','#ADA7FC','#B2F068','#6AECF4','#45B4E7','#AD61ED','#6569CC']
 * @example 
 *      ObtenerColores('Basica')
 */
function ObtenerColores(paleta){
    var Paletas={
        'Basica':['#57C5CB','#DA517A','#FECB7E','#F79A6A','#ADA7FC','#B2F068','#6AECF4','#45B4E7','#AD61ED','#6569CC'],
        'Pastel1':['#285189','#04869C','#4F3B78','#9E0B28','#DA517A','#45B4E7','#0B3C77','#F79A6A','#AD61ED','#6569CC'],
        'Pastel2':['#82CCB5','#DD86B9','#FFF68F','#F9B48A','#F497AA','#B6D884','#6BCADE','#71ABDD','#FDCD7B','#9977B4'],
        'Pastel3':['#9DDAEC','#F29AC0','#FEDDB4','#FFAAA5','#C1ACD3','#B9DB9F','#B0DDD6','#DCEDC1','#EDEEA2','#FF8B94']
    }
    return Paletas[paleta?paleta:'Basica'];
}

/**
 * verifica si se puede mostrar la instroducción guía 
 * @param {string} variable - nombre de la cookie para controlar las veces que se puede ejecutar una guía.
 * @param {number} veces - veces permitidas que se puede repetir la guía.
 * @example
 *      VerificarIntroduccion('PAGINA_INICIO',3)
 *      
 */
function VerificarIntroduccion(variable,veces){
    var introduccion=ObtenerCookie(variable);
    if(introduccion===null){
        DefinirCookie(variable,(veces?veces:1) - 1,365);
        MostrarIntroduccion();
    }else{
        var restantes=ObtenerNumero(introduccion);
        if(restantes>0){
            DefinirCookie(variable,(restantes - 1),365);
            MostrarIntroduccion();
        }
    }
}

/**
 * Parte una cadena de texto por limite de caracteres, buscando el  ultimo espacio o el ultimo salto de linea 
 * @param {string} cadena - La cadena que se desea obtener.
 * @param {string} limite - Limite de carácteres por linea.
 * @return {string[]}  'hola \ncomo \nestas \nyo \nestoy\n bien \ny tu '
 * @example 
 *      ObtenerParrafo('hola como estas yo estoy bien y tu ',5)
 */
function ObtenerParrafo(cadena,limite){
    var parrafo = ''; 
    var subCadena = cadena;
    while (subCadena.length > limite) {
        var c = subCadena.substring(0,limite);
        limiteLinea = c.lastIndexOf(' ');
        var limiteLinea2 =c.lastIndexOf('\n');
        if (limiteLinea2 != -1) {limiteLinea = limiteLinea2;}
        if (limiteLinea == -1) {limiteLinea = limite;}
        parrafo += c.substring(0,limiteLinea) + '\n';
        subCadena = subCadena.substring(limiteLinea+1);
    }
    return parrafo + subCadena;
}

/**
 * Elimina una cookie
 * @param {string} nombre - nombre de la cookie a eliminar.
 * @example
 *      EliminarCookie('MI_COOKIE')
 */
function EliminarCookie(nombre){
    document.cookie=nombre+'=;path=/;Max-Age=0';
}
/**
 * Obtiene el valor de una cookie, si no se encuentra se devuelve null
 * @param {string} nombre - nombre de la cookie a obtener.
 * @return {string}  'hola \ncomo \nestas \nyo \nestoy\n bien \ny tu '
 * @example
 *      ObtenerCookie('MI_COOKIE')
 */
function ObtenerCookie(nombre){
    var vs = document.cookie.split(';');
    if(vs.length>0){
      for(let i=0;i<vs.length;i++){
        var index=Number(vs[i].indexOf('='));
        if(vs[i].substring(0, index).trim()==nombre){
          return vs[i].substring(index+1);
        }
      }
    }
    return null;
}


/**
 * Obtiene el valor de una cookie, si no se encuentra se devuelve null
 * @param {string} nombre - nombre de la cookie a obtener.
 * @example
 *      DefinirCookie('MI_COOKIE','LISTO',2)
 */
function DefinirCookie(nombre,valor,dias){

    document.cookie = nombre+'='+valor+'; path=/; expires='+ObtenerDuracionCookie(dias ? dias : 40);
}

/**
 * Obtiene el valor de tiempo que debe setearse en una cookie
 * @param {number} dia - cantidad de dias de duración.
 * 
 */
function ObtenerDuracionCookie(dia) {
    var d = new Date();
    d.setTime(d.getTime() + (dia * 24 * 60 * 60 * 1000));
    return d.toUTCString();
}
/**
 * Obtiene el valor de tiempo que debe setearse en una cookie
 * @param {number} dia - cantidad de dias de duración.
 */
function ObtenerFecha(texto,tipo){
    if(texto){
        try {
            fecha=new Date(texto);
            switch(tipo){
                case 'fecha':
                    return fecha.getFullYear()+'-'+('0' + (fecha.getMonth()+1)).slice(-2)+'-'+('0' +fecha.getDate()).slice(-2);
                    break;
                default:
                    return fecha.getFullYear()+'-'+('0' + (fecha.getMonth()+1)).slice(-2)+'-'+('0' +fecha.getDate()).slice(-2)+' '+('0' + fecha.getHours()).slice(-2)+':'+('0' +fecha.getMinutes()).slice(-2)+':'+('0' +fecha.getSeconds()).slice(-2);
                    break;
            }
            }
          catch(error) {
            return ''
          }
    }else{
        return '';
    }
}
/**
 * Verifica si un año es biciesto
 * @param {number} ano - Año.
 * @return {boolean}
 * @example
 *      EsBiciesto('2020')
 */
function EsBiciesto(ano){
    if(ano){
        ano=Number(ano);
        return (((ano%4===0)&&(ano%100!==0))||(ano%400===0));
    }else{
        return false;
    }
    
}

/**
 * Obtener nombre de un mes
 * @param {string} texto - Numero del Mes en Base al Objeto del meses.
 * @return {string}
 */
function ObtenerMes(texto){
    if(texto){
        try {
            fecha=new Date(texto);
            return meses[fecha.getMonth()];
            }
          catch(error) {
            return ''
          }
    }else{
        return '';
    }
}


/**
 * Obtiene un numero en base a un texto recibido
 * @param {string} texto - texto a obtener el numero.
 * @return {number}
 * @example
 *      ObtenerNumero('2000')
 */
function ObtenerNumero(texto){
    if(Number(texto)){
        return Number(texto);
    }else{
        return 0;
    }
}

/**
 * Obtiene un texto en base a una cadena recibida
 * @param {string} texto - texto a obtener.
 * @return {string}
 *      ObtenerTexto(null)
 */
function ObtenerTexto(texto){
    if(Validar(texto)){
        return texto.toString();
    }else{
        return '';
    }
}

/**
 * Verifica si un texto contiene una cadena determinada
 * @param {string} texto - texto a obtener.
 * @param {string} palabra - palabra a buscar.
 * @return {boolean}
 * @example
 *      ContenerCadena('hola como estas','como')
 */
function ContenerCadena(texto,palabra){
    texto=ObtenerTexto(texto);
    palabra=ObtenerTexto(palabra);
    if(texto.trim().toLowerCase().indexOf(palabra.trim().toLowerCase())!=-1){
    return true;
    }
    else{
    return false;
    }
}


/**
 * Reemplaza caracteres no validos para usar como identificiadores de documentos html
 * @param {string} texto - texto a reemplazar.
 * @return {string}
 */
function SanitizarId(texto){
    texto=ObtenerTexto(texto);
    return reemplazarValor(reemplazarValor(reemplazarValor(texto,'\\\\',''),'/',''),' ','');
}

/**
 * Reemplaza una cadena dentro de un texto
 * @param {string} texto - texto a buscar .
 * @param {string} nombre - texto a reemplazar.
 * @param {string} reemplazo - texto de reemplazo.
 * @return {string}
 */
function reemplazarValor(texto,nombre,reemplazo)
{   
    var regular=new RegExp(nombre, "g");
    while(regular.test(texto)){
      texto=texto.replace(nombre,reemplazo);
    }
    return texto;
  }
/**
 * Verifica si en un identificador de un proveedor se encuentra un rtn válido.
 * @param {string} texto - texto a buscar.
 * @return {boolean}
 */
function VerificarCadenaRTN(texto){
    return /^HN-RTN-\d{14}$/.test(ObtenerTexto(texto));
}

/**
 * Corta un texto a un máximo de carácteres.
 * @param {string} texto - texto a buscar.
 * @param {number} maximo - maximo de carácteres para buscar.
 * @return {boolean}
 */
function ReducirTexto(texto,maximo){
    texto=ObtenerTexto(texto);
    if(texto.length>maximo){
       return texto.substring(0, maximo)+'...';
    }else{
        return texto;
    }
}

/**
 * Obtiene la Extensión de un archivo si cuenta con ella
 * @param {string} texto - direccion a buscar.
 * @return {string}
 */
function ObtenerExtension(direccion){
    var texto=ObtenerTexto(direccion);
    var extension=texto.match(/\.[^\.]+$/);
    if(extension&&extension[0]){
        return extension[0].toUpperCase();
    }else{
        return 'ARCHIVO'
    }
    
}

/**
 * Obtiene el formato en moneda de un numero a partir de un texto
 * @param {string} texto - texto donde esta el numero 
 * @return {string} 1,000.00
 * @example
 *      ValorMoneda('1000')
 */
function ValorMoneda(texto){
    var numero=ObtenerNumero(texto);
    numero=numero.toFixed(0);
    numero=numero.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return numero;
}

/**
 * Obtiene el formato en numero a partir de un texto
 * @param {string} texto - texto donde esta el numero 
 * @return {number}
 */
function ValorNumerico(texto){
    var numero=ObtenerNumero(texto);
    numero=numero.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return numero;
}

/**
 * Obtiene el texto traducido de un estado de contrato
 * @param {string} texto - texto del codigo
 * @return {string}
 */
function TraduceTexto(texto){
    switch(texto){
        case 'planned':
            return 'Planeado';
            break;
        case 'buyer':
            return 'Comprador';
            break;
        case 'complete':
            return 'Completado';
            break;
        case 'contractSigned':
            return 'Contrato Firmado'
            break;
        default:
            return texto;
            break;
    }
}


/**
 * Obtiene un arreglo de obligaciones financieras que pertenecen a una transaccion
 * @param {Object} transaccion - objeto de transaccion
 * @param {Object[]} obligaciones - arreglo de obligaciones financieras
 * @return {Object[]}
 */
function ObtenerObligacionesTransaccion(transaccion,obligaciones){
    var arregloObligaciones=[];
    if(transaccion.financialObligationIds && transaccion.financialObligationIds.length && obligaciones && obligaciones.length){
      for(i=0;i<obligaciones.length;i++){
        if(transaccion.financialObligationIds.includes(obligaciones[i].id)){
          arregloObligaciones.push(
            obligaciones[i]
          )
        }
      }
    }
    return arregloObligaciones;
  }

function DescargarElemento(direccion){
    var enlace=document.createElement('a');
    document.body.appendChild(enlace);
    enlace.href=direccion;
    enlace.click();
}


/**
 * Muestra el elemento de cargando en  un determinado elemento html 
 * @param {string} selector - selector de document query
 * @param {boolean} elemento - si es en un elemento determinado o en general
 */
function MostrarEspera(selector,elemento){
    elemento=true;
    if($(selector+' .espera'+(elemento?'.elemento':'')).length){
        $(selector+' .espera'+(elemento?'.elemento':'')).remove();
    }
    
    $(selector).append(
        $('<div>',{class:'espera'+(elemento?' elemento':'')}).append(
            $('<img>',{class:'imagen',src:'/static/img/otros/loader.svg'}),
            $('<div>',{text:'Cargando',class:'textoCargando'})
        )
    );
    
}

/**
 * Muestra el elemento de que no se pudo mostrar con datos el grafico en  un determinado elemento html 
 * @param {string} selector - selector de document query
 * @param {boolean} elemento - si es en un elemento determinado o en general
 */
function MostrarSinDatos(selector,elemento){
    elemento=true;
    if($(selector+' .espera'+(elemento?'.elemento':'')).length){
        $(selector+' .espera'+(elemento?'.elemento':'')).remove();
    }
    $(selector).append(
        $('<div>',{class:'espera'+(elemento?' elemento':'')}).append(
            $('<div>',{class:'imagen textoCargando ',style:'top:40%'}).append(
                $('<i>',{class:'far fa-frown animated rotateIn',style:'font-size:60px'})
            ),
            $('<div>',{text:'No hay datos para mostrar el gráfico con este filtrado.',class:'textoCargando',style:'font-size:20px;padding-left:10%;padding-right:10%'})
        )
    );
    
}

/**
 * Ocultar el elemento de cargando en  un determinado elemento html 
 * @param {string} selector - selector de document query
 */
function OcultarEspera(selector){
    if($(selector+' .espera').length){
        $(selector+' .espera').hide();
    }
}

/**
 * Muestra el elemento de un reloj mientras carga algo en  un determinado elemento html 
 * @param {string} selector - selector de document query
 * @param {boolean} elemento - si es en un elemento determinado o en general
 */
function MostrarReloj(selector,elemento){
    if($(selector+' .espera'+(elemento?'.elemento':'')).length){
        $(selector+' .espera'+(elemento?'.elemento':'')).remove();
    }
    if($(selector+' .esperaReloj').length){
        $(selector+' .esperaReloj').show();
    }else{
        $(selector).append(
            $('<div>',{class:'esperaReloj'}).append(
                $('<div>',{class:'reloj'}).append(
                    $('<div>',{class:'flecha hora'}),
                    $('<div>',{class:'flecha minuto'})
                ),
                $('<div>',{text:'Cargando',class:'textoCargando'})
            )
        )
    }
}

/**
 * Oculta el elemento de un reloj mientras carga algo en  un determinado elemento html 
 * @param {string} selector - selector de document query
 */
function OcultarReloj(selector){
    if($(selector+' .esperaReloj').length){
        $(selector+' .esperaReloj').hide();
    }
}


/**
 * Cambia el la seleccion de muestra de un tipo de filtrado > < =
 * @param {Object} elemento - elemento html
 */
function cambiarFiltroNumerico(elemento){
if($(elemento).is(":checked")){
    $(elemento).parent().parent().parent().parent().parent().find('.btnFiltroNumerico').html(
        $(elemento).attr('opcion').replace('==','=')
    );
    $(elemento).parent().parent().parent().parent().parent().find('.campoBlancoTextoSeleccion').attr(
        'opcion',$(elemento).attr('opcion')
    );
    $(elemento).parent().parent().parent().parent().parent().find('.campoBlancoTextoSeleccion').trigger('change');
}

}

/**
 * Valida si una valor es nulo o indefinido
 * @param {Object} valor - valor a validad
 * @return {boolean}
 */
function Validar(valor){
    if(valor!=null&&valor!=undefined){
        return true;
    }else{
        return false;
    }
}


/**
 * Valida si una valor nos es vacío o nulo
 * @param {Object} valor - valor a validad
 * @return {boolean}
 */
function ValidarCadena(valor){
    if(valor!=null&&valor!=undefined&&valor!==''){
        return true;
    }else{
        return false;
    }
}

/**
 * Muestra el  orden seleccionado de un campo
 * @param {Object} evento - evento de cambio de un elemento html
 */
function cambiarOrden(evento){
    var elemento=$(evento.currentTarget);
    switch($(elemento).attr('opcion')){
        case 'neutro':
           $(elemento).find('.flechaArriba').hide();
           $(elemento).find('.flechaAbajo').show();
           $(elemento).attr('opcion','descendente');  
        break;
        case 'ascendente':
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').show();
            $(elemento).attr('opcion','neutro'); 
        break;
        case 'descendente':
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').hide();
            $(elemento).attr('opcion','ascendente'); 
        break;
        default:
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').show();
            $(elemento).attr('opcion','neutro'); 
        break;
    }  
}

/**
 * Muestra el  orden seleccionado de un campo y ejecuta una funcion
 * @param {Object} evento - evento de cambio de un elemento html
 * @param {Object} funcion - una funcion opcional a ser ejecutada
 */
function cambiarOrdenFiltro(evento,funcion){
    var elemento=$(evento.currentTarget);
    $('.ordenEncabezado').not($(elemento)).attr('opcion','neutro'); 
    $('.ordenEncabezado').not($(elemento)).find('.flechaAbajo').show();
    $('.ordenEncabezado').not($(elemento)).find('.flechaArriba').show();
    var filtro=elemento.closest('.campoFiltrado');
    switch($(elemento).attr('opcion')){
        case 'neutro':
           $(elemento).find('.flechaArriba').hide();
           $(elemento).find('.flechaAbajo').show();
           $(elemento).attr('opcion','descendente');
           if(funcion){
             funcion(filtro.attr('filtro'),'descendente')
           }
        break;
        case 'ascendente':
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').show();
            $(elemento).attr('opcion','neutro');
            if(funcion){
                funcion(filtro.attr('filtro'),'neutro')
              } 
        break;
        case 'descendente':
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').hide();
            $(elemento).attr('opcion','ascendente'); 
            if(funcion){
                funcion(filtro.attr('filtro'),'ascendente')
              } 
        break;
        default:
            $(elemento).find('.flechaArriba').show();
            $(elemento).find('.flechaAbajo').show();
            $(elemento).attr('opcion','neutro'); 
            if(funcion){
                funcion(filtro.attr('filtro'),'neutro')
              }
        break;
    }  
}

/**
 * Agrega al evento click al hacer click a los botones de orden de una columna
 */
function AsignarOrdenTabla(){
    $('.ordenEncabezado').each(function(llave,elemento){
            $(elemento).on('click',
            function(evento){
                cambiarOrden(evento);
            })
            
        }
    )
}

/**
 * Agrega al evento click al hacer click a los botones de orden de una columna
 * @param {Object} funcion - una funcion opcional a ser ejecutada
 * @param {Object} evento - evento de cambio de un elemento html
 * 
 */
function AsignarOrdenTablaFiltros(funcion,selector){
    $(selector?selector:'.ordenEncabezado').each(function(llave,elemento){
            $(elemento).on('click',
            function(evento){
                cambiarOrdenFiltro(evento,funcion);
            })
            
        }
    )
}


/**
 * Agrega el tooltip de la librería de tippy a todos los elementos que tengan el atributo toolTexto
 * 
 */
function AgregarToolTips(){
    $('[toolTexto]').each(
        function(llave,elemento){
            $(elemento).css('outline','0')
            var parametros={
                arrow: true,
                arrowType: 'round',
                content:$(elemento).attr('toolTexto'),
                allowHTML: true
              };
            if($(elemento).attr('toolPosicion')){
                parametros['placement']=$(elemento).attr('toolPosicion');
            }
            if($(elemento).attr('toolFlecha')&&$(elemento).attr('toolFlecha')=="false"){
                parametros['arrow']=false;
            }
            if($(elemento).attr('toolCursor')&&$(elemento).attr('toolCursor')=="true"){
                parametros['followCursor']=true;
            }else if ($(elemento).attr('toolCursor')){
                parametros['followCursor']=$(elemento).attr('followCursor');
            }
            tippy($(elemento).get(),parametros);
        }
    )
}

/**
 * Obtiene el valor de un parametro get
 * @param {string} nombre - nombre del parametro get a buscar
 * @param {string} url - dirección en la que se desea buscar el parámetro get
 * @return {string}
 */
function ObtenerValor( nombre, url ) {
    if (!url) {url = location.href};
    nombre=encodeURIComponent(nombre);
    nombre = nombre.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
    var regexS = "[\\?&]"+nombre+"=([^&#]*)";
    var regex = new RegExp( regexS, 'g' );

    let matches =[]
    for(const match of url.matchAll(regex)) {
        matches.push(match[1]);
    }

    if(matches.length > 1) {
        return matches.join(",");
    }
    return matches.length == 0 ? null : matches[0];
}


/**
 * Obtiene el enlace de una parte ya sea un proveedor o un comprador
 * @param {string} id - identificador de la parte
 * @param {Object[]} arreglo - arreglo de partes
 * @param {Object} fuente -fuente de datos de las partes
 * @return {Object[]}
 */
function ObtenerEnlaceParte(id,arreglo,fuente){
    var elementos=[];
    if(arreglo){
      elementos=arreglo;
    }
    var partes=fuente&&fuente.parties?fuente.parties:((typeof(procesoRecord)!='undefined')&&procesoRecord.compiledRelease&&procesoRecord.compiledRelease.parties?procesoRecord.compiledRelease.parties:[]);
    for(var i = 0; i < partes.length;i++){
        if(partes[i].id == id){
          elementos.push(partes[i]);
          if(partes[i].memberOf){
            for(var j = 0; j < partes[i].memberOf.length;j++){
              ObtenerEnlaceParte(partes[i].memberOf[j].id,elementos,fuente);
            }
  
          }
        }
    }
    return elementos;
  }

/**
 * Obtiene los elementos html de un enlace de una parte
 * @param {string} id - identificador de la parte
 * @param {Object} fuente -fuente de datos de las partes
 * @return {Object[]}
 */
  function ObtenerElementosParte(id,fuente){
    var parte=ObtenerEnlaceParte(id,false,fuente);
    var elementos=[];
    for(var i=0;i<parte.length;i++){
      elementos.push(
        parte[i].roles.includes('buyer')?($('<a>',{text:parte[i].name,class:'enlaceTablaGeneral',href:'/comprador/'+encodeURIComponent( parte[i].id)})):(parte[i].roles.includes('supplier')?(
          $('<a>',{text:parte[i].name,class:'enlaceTablaGeneral',href:'/proveedor/'+parte[i].id})
        ):(
          $('<span>',{text:parte[i].name})
          ) 
        ) 
      )
      if(elementos.length>=1&&i+1<parte.length){
        elementos.push($('<span>',{text:' de '}));
      }
    }
    return elementos;
  }


/**
 * Obtiene los elementos html de los proveedores enviados
 * @param {Object[]} proveedores - arreglo de objetos proveedores
 * @param {Object} fuente -fuente de datos de las partes
 * @return {Object[]}
 */
  function ObtenerProveedores(proveedores,fuente){
    var elementos=[];
    if(proveedores){
      for(var i=0;i<proveedores.length;i++){
        elementos=elementos.concat(ObtenerElementosParte(proveedores[i].id,fuente));
      }
    }
    
    return elementos;
  }

/**
 * Obtiene los elementos a mostrar en una paginacion
 * @param {Object[]} paginaActual - pagina que se quiere mostrar
 * @param {Object} ultimaPagina - ultima pagina disponible
 * @return {string[]}
 */
  function ObtenerPaginacion(paginaActual, ultimaPagina) {
    var paginas=[];
    var paginasPuntos=[];
    var espaciado=2;
    var izquierda=paginaActual - espaciado;
    var derecha=paginaActual + espaciado;
    var contador=0;
    for(var i=1;i<=ultimaPagina;i++){
        if (i==1||i==ultimaPagina||i>=izquierda&&i<=derecha){
            paginas.push(i);
        }
    }
    for(var i=0;i<paginas.length;i++){
        if(contador){
            if (paginas[i]-contador!=1) {
                paginasPuntos.push('...');
            }
        }
        paginasPuntos.push(paginas[i]);
        contador=paginas[i];
    }
    return paginasPuntos;
}

/**
 * Obtiene un arreglo de objetos cuyas prpiedades estan definidos por un delimitador
 * @param {Object} json - objeto json
 * @param {Object} resultado - objeto de resultados
 * @param {string} delimitador - delimitador de las propiedades de un json
 * @param {string} path - path anterior
 */
function ArregloJSON(json,resultado,delimitador,path){
    path=ValidarCadena(path)?path:'';
    $.each(json,function(indice,valor){
        if(typeof(valor)==='object'&&typeof(valor.getMonth)!=='function'){
            ArregloJSON(valor,resultado,delimitador,path+(ValidarCadena(path)?delimitador:'')+indice);
        }else{
            
            //var objeto={};
            resultado[path+(ValidarCadena(path)?delimitador:'')+indice]=valor;
            /*resultado.push(
                objeto
            );*/
        }
    });
    return resultado;
}
/**
 * Obtiene un arreglo de arreglos cuyos encabezados estan definidos por un delimitador
 * @param {Object[]} arreglo - arreglo de objetos
 * @return {string[][]}
 */
function ObtenerMatrizObjeto(arreglo){
    var encabezados=[];
    var cuerpo=[];
    var resultados=[];
    arreglo.forEach(function(valor,indice){
        var fila=ArregloJSON(valor,{},'/');
        resultados.push(fila);
        encabezados=encabezados.concat(Object.keys(fila));
        encabezados=encabezados.filter(function(valorFila, indiceFila){ return (encabezados.indexOf(valorFila) === indiceFila);});
    });
    cuerpo.push(encabezados);
    resultados.forEach(function(valorFila,indiceFila){
        var fila=[]
        encabezados.forEach(function(valorEncabezado,indiceEncabezado){
            if(Validar(valorFila[valorEncabezado])){
                fila.push(valorFila[valorEncabezado]);
            }else{
                fila.push('');
            }
        });
        cuerpo.push(fila);
    });
    
    return cuerpo;
}

/**
 * Guarda un archivo generado csv
 * @param {string[]} arreglo - arreglo de cadenas
 * @param {string} nombre - nombre opcional de archivo
 */
function DescargarCSV(arreglo,nombre){
if(arreglo.length>1){
    var cadenaCSV='';
    arreglo.forEach(function(fila,indice){
        fila=fila.map(function(e){return '"'+reemplazarValor(/*reemplazarValor(reemplazarValor(e,'\n',' '),',','.')*/e,'"',"'")+'"';});
        cadenaCSV+=fila.join(',')+((indice<arreglo.length-1)?'\n':'');
    });

    var blob = new Blob(["\uFEFF"+cadenaCSV], { type: "text/csv;charset=utf-8" });
    saveAs(blob, nombre?('Portal de Contrataciones Abiertas de Honduras - '+nombre+'.csv'):'Portal de Contrataciones Abiertas de Honduras.csv');

}
}
/**
 * Guarda un archivo generado
 * @param {string[]} arreglo - arreglo de cadenas
 */
function DescargarJSON(objeto,nombre){
    if(objeto){
        var blob = new Blob([JSON.stringify(objeto)], { type: "application/json" });
        saveAs(blob, nombre?('Portal de Contrataciones Abiertas de Honduras - '+nombre+'.json'):'Portal de Contrataciones Abiertas de Honduras.json');
    }
}

/**
 * Guarda un archivo generado xlsx
 * @param {string[]} arreglo - arreglo de cadenas
 * @param {string} nombre - nombre opcional de archivo
 */
function DescargarXLSX(arreglo,nombre){
    if(arreglo.length>1){
        var wb = XLSX.utils.book_new();
    wb.Props = {
        Title: nombre?nombre:'',
        Subject: "Portal de Contrataciones Abiertas de Honduras",
        Author: "Portal de Contrataciones Abiertas de Honduras",
        CreatedDate: new Date()
    };
    wb.SheetNames.push(nombre?nombre:'');
    var ws_data = arreglo;
    var ws = XLSX.utils.aoa_to_sheet(ws_data);
    wb.Sheets[nombre?nombre:''] = ws;
    var wbout = XLSX.write(wb, {
        bookType: 'xlsx',
        type: 'binary'
    });
    saveAs(new Blob([s2ab(wbout)], {
        type: "application/octet-stream"
    }),nombre?('Portal de Contrataciones Abiertas de Honduras - '+nombre+'.xlsx'):'Portal de Contrataciones Abiertas de Honduras.xlsx');
    }
}

/**
 * Obtiene un buffer para guardar un archivo
 * @param {string[]} s - arreglo de cadenas
 */
function s2ab(s) { 
    var buf = new ArrayBuffer(s.length); //convert s to arrayBuffer
    var view = new Uint8Array(buf);  //create uint8array as viewer
    for (var i=0; i<s.length; i++) view[i] = s.charCodeAt(i) & 0xFF; //convert to octet
    return buf;    
}

/**
 * Abre una modal para descargar los archivos
 * @param {string} selector - selector que hace referencia al modal
 * @param {string} titulo - titulo que se desea que tenga el modal
 * @param {boolean} creacion - booleano para identificar si se esta creando el modal
 */
function AbrirModalDescarga(selector,titulo,creacion){
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
                        $('<div>',{class:'modal-body textoAlineadoCentrado'}).append(
                            $('<div>',{style:'min-height:200px;position:relative',class:'contenedorEspera'}).append(
                                $('<div>',{class:'espera elemento'}).append(
                                    $('<img>',{class:'imagen',src:'/static/img/otros/loader.svg'}),
                                    $('<div>',{text:'Obteniendo Datos',class:'textoCargando'})
                                ),
                                $('<i>',{class:'fas fa-file-download textoColorPrimario descargaIcono fadeIn animated',style:'display:none;font-size:100px;line-height:200px'})
                            )
                            
                        ),
                        $('<div>',{class:'modal-footer'}).append(
                            $('<button>',{class:'botonGeneral fondoColorSecundario','data-dismiss':'modal',type:'button'}).text(
                                'Cancelar'
                            ),$('<button>',{class:'botonGeneral fondoColorGris botonDescarga',type:'button'}).text(
                                'Descargar'
                            )
                        )
                    )
                )
            )
        )
    }
    if(!creacion){
        $('body #'+selector).modal();
    }
    

}


/**
 * Remove el evento del boton descargar de una modal
 * @param {string} selector - selector que hace referencia al modal
 */
function EliminarEventoModalDescarga(selector){
    if($('body #'+selector).length){
        $('body #'+selector+' .descargaIcono').hide();
        $('body #'+selector+' .botonDescarga').removeClass('fondoColorPrimario').addClass('fondoColorGris');
        $('body #'+selector+' .botonDescarga').off();
        $('body #'+selector+' .contenedorEspera .espera').show();
    }
    
}
/**
 * Agrega el evento al boton descargar de una modal
 * @param {string} selector - selector que hace referencia al modal
 */
function AgregarEventoModalDescarga(selector,funcion){
    if($('body #'+selector).length){
        $('body #'+selector+' .botonDescarga').removeClass('fondoColorGris').addClass('fondoColorPrimario');
        $('body #'+selector+' .botonDescarga').on({
            click:function(e){
                funcion();
            }
        });
        OcultarEspera('body #'+selector+' .contenedorEspera');
        $('body #'+selector+' .descargaIcono').show();
    }
    
    
}