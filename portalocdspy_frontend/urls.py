from django.conf.urls.static import static
from django.urls import path, include

from portalocdspy import settings
from portalocdspy_frontend import views as frontend_views
from django.contrib.auth import views as auth_views

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
    path('accounts/login/', auth_views.LoginView.as_view(template_name='auth/login.html')),
    path('accounts/change-password/', auth_views.PasswordChangeView.as_view(template_name='auth/change-password.html')),
    path('accounts/', include('django.contrib.auth.urls')),

]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
