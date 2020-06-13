class dbRouter:

	def db_for_read(self, model, **hints):

		if model._meta.app_label == 'kingfisher':
			return 'bdkingfisher'
		else:
			return 'portaledcahn_admin'

	def db_for_write(self, model, **hints):

		if model._meta.app_label == 'kingfisher':
			return False
		else:
			return 'portaledcahn_admin'

	def allow_relation(self, obj1, obj2, **hints):

		if obj1._meta.app_label == 'kingfisher' or \
			obj2._meta.app_label == 'kingfisher':
			return False
		return None

	def allow_migrate(self, db, app_label, model_name=None, **hints):

		if app_label == 'kingfisher':
			return False
		else:
			return 'portaledcahn_admin'