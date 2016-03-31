from django.db import DEFAULT_DB_ALIAS


class AutoRouter:
    def __init__(self):
        self.db_alias = DEFAULT_DB_ALIAS

    def db_for_read(self, model, **hints):
        return self.db_alias

    def db_for_write(self, model, **hints):
        return self.db_alias

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return True
