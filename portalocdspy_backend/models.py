from django.db import models
from django.contrib.postgres.fields import JSONField


class AlembicVersion(models.Model):
    version_num = models.CharField(primary_key=True, max_length=32)

    class Meta:
        managed = False
        db_table = 'alembic_version'
        app_label = 'kingfisher'


class Collection(models.Model):
    source_id = models.TextField()
    data_version = models.DateTimeField()
    store_start_at = models.DateTimeField()
    store_end_at = models.DateTimeField(blank=True, null=True)
    sample = models.BooleanField()
    check_data = models.BooleanField()
    check_older_data_with_schema_version_1_1 = models.BooleanField()
    transform_type = models.TextField(blank=True, null=True)
    transform_from_collection = models.ForeignKey('self', models.DO_NOTHING, blank=True, null=True)
    deleted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'collection'
        unique_together = (('source_id', 'data_version', 'sample', 'transform_from_collection', 'transform_type'),)
        app_label = 'kingfisher'

class CollectionFile(models.Model):
    collection = models.ForeignKey(Collection, models.DO_NOTHING)
    filename = models.TextField(blank=True, null=True)
    store_start_at = models.DateTimeField(blank=True, null=True)
    store_end_at = models.DateTimeField(blank=True, null=True)
    warnings = models.TextField(blank=True, null=True)  # This field type is a guess.
    url = models.TextField(blank=True, null=True)
    errors = models.TextField(blank=True, null=True)  # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'collection_file'
        unique_together = (('collection', 'filename'),)
        app_label = 'kingfisher'

class CollectionFileItem(models.Model):
    collection_file = models.ForeignKey(CollectionFile, models.DO_NOTHING)
    store_start_at = models.DateTimeField(blank=True, null=True)
    store_end_at = models.DateTimeField(blank=True, null=True)
    number = models.IntegerField(blank=True, null=True)
    warnings = models.TextField(blank=True, null=True)  # This field type is a guess.
    errors = models.TextField(blank=True, null=True)  # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'collection_file_item'
        unique_together = (('collection_file', 'number'),)
        app_label = 'kingfisher'

class CollectionNote(models.Model):
    collection = models.ForeignKey(Collection, models.DO_NOTHING)
    note = models.TextField()
    stored_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'collection_note'
        app_label = 'kingfisher'

class CompiledRelease(models.Model):
    collection_file_item = models.ForeignKey(CollectionFileItem, models.DO_NOTHING)
    ocid = models.TextField(blank=True, null=True)
    data = models.ForeignKey('Data', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'compiled_release'
        app_label = 'kingfisher'

class Data(models.Model):
    hash_md5 = models.TextField(unique=True)
    data = JSONField()  # This field type is a guess.
    # data = models.TextField()

    class Meta:
        managed = False
        db_table = 'data'
        app_label = 'kingfisher'

class PackageData(models.Model):
    hash_md5 = models.TextField(unique=True)
    data = JSONField() # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'package_data'
        app_label = 'kingfisher'

class Record(models.Model):
    collection_file_item = models.ForeignKey(CollectionFileItem, models.DO_NOTHING)
    ocid = models.TextField(blank=True, null=True)
    data = models.ForeignKey(Data, models.DO_NOTHING)
    package_data = models.ForeignKey(PackageData, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'record'
        app_label = 'kingfisher'
        ordering = ['-id']

class RecordCheck(models.Model):
    record = models.ForeignKey(Record, models.DO_NOTHING)
    override_schema_version = models.TextField(blank=True, null=True)
    cove_output = models.TextField()  # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'record_check'
        unique_together = (('record', 'override_schema_version'),)
        app_label = 'kingfisher'

class RecordCheckError(models.Model):
    record = models.ForeignKey(Record, models.DO_NOTHING)
    override_schema_version = models.TextField(blank=True, null=True)
    error = models.TextField()

    class Meta:
        managed = False
        db_table = 'record_check_error'
        unique_together = (('record', 'override_schema_version'),)
        app_label = 'kingfisher'

class Release(models.Model):
    collection_file_item = models.ForeignKey(CollectionFileItem, models.DO_NOTHING)
    release_id = models.TextField(blank=True, null=True)
    ocid = models.TextField(blank=True, null=True)
    data = models.ForeignKey(Data, models.DO_NOTHING)
    package_data = models.ForeignKey(PackageData, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'release'
        app_label = 'kingfisher'
        ordering = ['-id']

class ReleaseCheck(models.Model):
    release = models.ForeignKey(Release, models.DO_NOTHING)
    override_schema_version = models.TextField(blank=True, null=True)
    cove_output = models.TextField()  # This field type is a guess.

    class Meta:
        managed = False
        db_table = 'release_check'
        unique_together = (('release', 'override_schema_version'),)
        app_label = 'kingfisher'

class ReleaseCheckError(models.Model):
    release = models.ForeignKey(Release, models.DO_NOTHING)
    override_schema_version = models.TextField(blank=True, null=True)
    error = models.TextField()

    class Meta:
        managed = False
        db_table = 'release_check_error'
        unique_together = (('release', 'override_schema_version'),)
        app_label = 'kingfisher'

class TransformUpgrade10To11StatusRecord(models.Model):
    source_record = models.ForeignKey(Record, models.DO_NOTHING, primary_key=True)

    class Meta:
        managed = False
        db_table = 'transform_upgrade_1_0_to_1_1_status_record'
        app_label = 'kingfisher'

class TransformUpgrade10To11StatusRelease(models.Model):
    source_release = models.ForeignKey(Release, models.DO_NOTHING, primary_key=True)

    class Meta:
        managed = False
        db_table = 'transform_upgrade_1_0_to_1_1_status_release'
        app_label = 'kingfisher'

####Otros###

class Contract(models.Model):
    id = models.TextField(blank=True, null=True)
    amount = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    currency = models.TextField(blank=True, null=True)
    date = models.DateField(auto_now=False, auto_now_add=False, null=True)
    buyerId = models.TextField(blank=True, null=True)
    buyerName = models.TextField(blank=True, null=True)
    buyerMemberOf = JSONField()

    class Meta:
        managed = False
        db_table = 'contracts'
        app_label = 'kingfisher'

class TasasDeCambio(models.Model):
    id = models.TextField(blank=True, null=True)
    year_er = models.DecimalField(null=True, max_digits=10, decimal_places=4)
    currency = models.TextField(blank=True, null=True)
    exchange_rate = models.DecimalField(null=True, max_digits=16, decimal_places=4)

    class Meta:
        managed = False
        db_table = '"edcahn"."tasas_de_cambio"'
        app_label = 'kingfisher'

class Buyer(models.Model):
    id = models.TextField(blank=True, null=True)
    buyerId = models.TextField(blank=True, null=True)
    buyerName = models.TextField(blank=True, null=True)
    buyerMemberOf = JSONField()
    numberOfContracts = models.IntegerField(null=True)
    sum_amount_hnl = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    sum_amount_usd = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    avg_amount_hnl = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    avg_amount_usd = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    max_amount_hnl = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    max_amount_usd = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    min_amount_hnl = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    min_amount_usd = models.DecimalField(null=True, max_digits=16, decimal_places=4)
    last_date_contract = models.DateField(auto_now=False, auto_now_add=False, null=True)

    class Meta:
        managed = False
        db_table = 'buyers'
        app_label = 'kingfisher'


class Contrato(models.Model):
    id = models.TextField()
    ocid = models.TextField(blank=True, null=True)
    data = JSONField()

    class Meta:
        managed = False
        db_table = 'contratos'
        app_label = 'kingfisher'