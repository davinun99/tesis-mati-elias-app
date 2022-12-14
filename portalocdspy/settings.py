"""
Django settings for portalocdspy project.

Generated by 'django-admin startproject' using Django 2.2.2.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.2/ref/settings/
"""

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'uo251c%zw60+efuwf$7yn7dl=6@0)p12%q(-87*p4r^dy-zbhp'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DEBUG', False) == 'True'

ALLOWED_HOSTS = [
    'localhost',
    '127.0.0.1',
    '*'
]

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'portalocdspy_backend',
    'rest_framework',
    'portalocdspy_frontend',
    'django_elasticsearch_dsl',
    'django_elasticsearch_dsl_drf',
    'django.contrib.humanize',
    'ocds_bulk_download',
    'channels'
]

MIDDLEWARE = [
    'django.middleware.gzip.GZipMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.cache.UpdateCacheMiddleware',  # NEW
    'django.middleware.common.CommonMiddleware',
    'django.middleware.cache.FetchFromCacheMiddleware',  # NEW
]

CACHE_MIDDLEWARE_ALIAS = 'default'  # which cache alias to use
CACHE_MIDDLEWARE_SECONDS = 600    # number of seconds to cache a page for (TTL)
CACHE_MIDDLEWARE_KEY_PREFIX = ''    # should be used if the cache is shared across multiple sites that use the same Django instance
WHITENOISE_MAX_AGE = 31557600

ROOT_URLCONF = 'portalocdspy.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'portalocdspy_frontend/templates')
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'portalocdspy.wsgi.application'

ASGI_APPLICATION = "portalocdspy.routing.application"

# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'mydatabase',
    },
    'bdkingfisher': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'pezeypvv',
        'USER': 'pezeypvv',
        'PASSWORD': 'V_2fdl4tJJblu8DxBj5VtZ8q5WRL4MhP',
        'HOST': 'suleiman.db.elephantsql.com',
        'PORT': '5432',
    },
    'portaledcahn_admin': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'sjbwpcih',
        'USER': 'sjbwpcih',
        'PASSWORD': 'I5yY3PzBy_sfzAIwpMI_GU0qTOxPriHc',
        'HOST': 'jelani.db.elephantsql.com',
        'PORT': '5432',
    }
}

DATABASE_ROUTERS = ['portalocdspy.dbrouters.dbRouter']

# Password validation
# https://docs.djangoproject.com/en/2.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/2.2/topics/i18n/

LANGUAGE_CODE = 'es-ar'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

MEDIA_URL = '/media/'

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.2/howto/static-files/

STATIC_URL = '/static/'

STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')

STATICFILES_DIRS = (
    os.path.join(BASE_DIR, 'portalocdspy_frontend/static'),
)

LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'


# Configuracion de parametros adicionales

REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 10
}

ELASTICSEARCH_DSL_HOST = 'https://localhost:9200'

ELASTICSEARCH_DSL = {
    'default': {
        'hosts': ELASTICSEARCH_DSL_HOST,
        'timeout': 60
    },
}

ELASTICSEARCH_USER = 'elastic'
ELASTICSEARCH_PASS = 'LZWHe+2DQKvgW7RqUjwU'
ELASTICSEARCH_TIMEOUT = 120

PAGINATE_BY = 10

SOURCE_SEFIN_ID = 'HN.SIAFI2'

"""
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'debug.log',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['file'],
            'level': 'DEBUG',
            'propagate': True,
        },
    },
}
"""
