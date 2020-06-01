from elasticsearch_dsl import analyzer
from elasticsearch_dsl.connections import connections
from django_elasticsearch_dsl import DocType, Index, fields 
from portalocdspy_backend import models as articles_models

data_index = Index('edca')
data_index.settings(
	number_of_shards=1,
	number_of_replicas=0
)

html_strip = analyzer(
	'html_strip',
	tokenizer="standard",
	filter=["standard", "lowercase", "stop", "snowball"],
	char_filter=["html_strip"]
)

@data_index.doc_type
class DataDocument(DocType):
	data = fields.ObjectField()

	class Meta:
		model = articles_models.Data
		fields = [
			'id', 
			'hash_md5', 
		]

	def prepare_data(self, instance):
		return instance.data

record_index = Index('record')
record_index.settings(
	number_of_shards=1,
	number_of_replicas=0
)

@record_index.doc_type
class RecordDocument(DocType):

	class Meta:
		model = articles_models.Record
		fields = [
			'id', 
			'ocid', 
		]