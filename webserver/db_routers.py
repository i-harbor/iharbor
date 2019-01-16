
class MetadataRouter(object):
    """
    A router to control all database operations on models that app_label == 'metadata'
    """
    def db_for_read(self, model, **hints):
        """
        Attempts to read meatadata models go to metadata.
        """
        if model._meta.app_label == 'metadata':
            return 'metadata'
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write metadata models go to metadata.
        """
        if model._meta.app_label == 'metadata':
            return 'metadata'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model is involved.
        """
        if obj1._meta.app_label == 'metadata' or \
           obj2._meta.app_label == 'metadata':
           return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the metadata Model class only appears in the 'metadata'
        database.
        """
        if app_label == 'metadata':
            return db == 'metadata'
        return None


