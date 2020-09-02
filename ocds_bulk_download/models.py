from django.db import models
from django.contrib.postgres.fields import JSONField

class Descarga(models.Model):
    id = models.TextField(primary_key=True, unique=True)
    file = JSONField()
    createdby = models.TextField(blank=True, null=True)
    active = models.BooleanField(blank=True, null=True)