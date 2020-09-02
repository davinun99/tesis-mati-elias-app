/**
 * @file visualizaciones.js Este archivo se incluye en la sección de Visualizaciónes del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

 /**
  * Accede al enlace para comartir la red Social
  * @param {string} tipo - Tipo de red social
  * @param {string} direccion - Dirección de la visualización
  */
function ObtenerEnlaceRed(tipo,direccion){
    switch(tipo){
        case 'whatsapp':
        location.href='https://api.whatsapp.com/send?text='+encodeURIComponent(url+ direccion+'?año='+ObtenerAnoVisualizacionesDefecto());
        break;
        case 'facebook':
        location.href='https://www.facebook.com/sharer.php?u='+encodeURIComponent(url+direccion+'?año='+ObtenerAnoVisualizacionesDefecto());
        break;
        case 'twitter':
        location.href='https://twitter.com/intent/tweet?text='+encodeURIComponent(url+direccion+'?año='+ObtenerAnoVisualizacionesDefecto());
        break;
        default:
        break;
    }
    
}

/**
 * Dirige el sitio a la página de la visualización
 * @param {string} direccion - Dirección de la sección de la página de la visualización
 */
function AccederVisualizacion(direccion){
    location.href=direccion+'?año='+ObtenerAnoVisualizacionesDefecto();
}

/**
 * Obtiene el año actual por defecto para filtrar las visualizaciones en caso de que sea el mes de enero retornará el año anterior.
 * @return {number} - Año
 */
function ObtenerAnoVisualizacionesDefecto(){
    if((new Date()).getMonth()==0){
        return (new Date()).getFullYear() - 1;
    }else{
        return (new Date()).getFullYear();
    }
}