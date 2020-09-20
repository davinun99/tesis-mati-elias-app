from django.conf.urls.static import static
from django.urls import path, include

from portalocdspy import settings
from portalocdspy_frontend import views as frontend_views

urlpatterns = [
	path('', frontend_views.Inicio),
    path('proceso/<str:ocid>/', frontend_views.Proceso),
    path('proceso/', frontend_views.Proceso),
    path('acerca/', frontend_views.Acerca),
    path('comprador/<str:id>/', frontend_views.Comprador),
    path('comprador/', frontend_views.Comprador),
    path('compradores/', frontend_views.Compradores),
    path('busqueda/', frontend_views.Busqueda),
    path('preguntas/', frontend_views.Preguntas),
    path('administracion-banderas/', frontend_views.Admin),

]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root= settings.MEDIA_ROOT)