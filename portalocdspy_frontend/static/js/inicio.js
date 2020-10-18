
DebugFecha()
CargarEstadisticas();

$(function(){
    /*Anadir eventos a los elementos*/
    $('#campoBlancoBusquedaProcesosBoton').on('click',function(e){
        window.location.href="/busqueda?term="+ encodeURIComponent($('#campoBlancoBusquedaProcesos').val()) +'&metodo='+ObtenerMetodo();
    });
    $('#campoBlancoBusquedaProcesos').on('keydown',function(e){
        teclaCodigo=e.keyCode ? e.keyCode : e.which;
        if(teclaCodigo=='13'){
            window.location.href="/busqueda?term="+encodeURIComponent($('#campoBlancoBusquedaProcesos').val()) +'&metodo='+ObtenerMetodo();
        }
    });
    /*Anadir Titles a los campos de busqueda*/
    AgregarToolTips();
    VerificarIntroduccion('INTROJS_INICIO',1);
});




function CargarEstadisticas(){
    $.get(api+"/inicio",function(datos){
        DebugFecha();
        if(datos){
            $('#compradores').attr('data-to', ObtenerNumero(datos.compradores));
            $('#uniqueRedFlags').attr('data-to', ObtenerNumero(datos.uniqueRedFlags));
            $('#redFlags').attr('data-to', ObtenerNumero(datos.redFlags));
            $('#procesos').attr('data-to', ObtenerNumero(datos.procesos));
            $('#proveedores').attr('data-to', ObtenerNumero(datos.proveedores));
        }
        $('.contenedorEstadisticaInicial').show();
        EfectoEstadisticas();
        AgregarToolTips();
    }).fail(function(a,b,c) {
        /*Error de Conexion al servidor */
        $('.contenedorEstadisticaInicial').show();
        EfectoEstadisticas();
        console.dir(a);
        console.dir(b);
        console.dir(c);
      });
}

function EfectoEstadisticas(){
    $('.tiempo').countTo({
        formatter: function (value, options) {
          value = value.toFixed(options.decimals);
          value = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
          return value;
      }
      });
}
function ObtenerMetodo(){
    return $('#metodoProceso')[0].checked?'proceso':$('#metodoContrato')[0].checked?'contrato':$('#metodoPago')[0].checked?'pago':'proceso';

}