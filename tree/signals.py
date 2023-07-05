from typing import Type

from django.db.models import Model
from django.db.models.signals import post_save
from django.dispatch import receiver

from tree.fields import PathField


@receiver(post_save)
def defer_paths(sender: Type[Model], **kwargs):
    path_fields = [
        field for field in sender._meta.concrete_fields
        if isinstance(field, PathField)
    ]
    if path_fields:
        instance = kwargs['instance']
        for path_field in path_fields:
            if path_field.attname in instance.__dict__:
                # Removes the cached value for the field, making it deferred.
                # That way, Django will run a new query to know what is
                # the new path, only if it is used.
                # I wish we could make Django receive paths from SQL
                # through `RETURNING`, but unfortunately the ORM
                # only uses `RETURNING pk`.
                del instance.__dict__[path_field.attname]
