from django.db.models import Func, IntegerField, TextField

from .fields import PathField


class SubPath(Func):
    function = 'subpath'


class Level(Func):
    function = 'nlevel'

    @property
    def output_field(self):
        return IntegerField()


class Index(Func):
    function = 'index'

    @property
    def output_field(self):
        return IntegerField()


class TextToPath(Func):
    function = 'text2ltree'

    @property
    def output_field(self):
        return PathField()


class PathToText(Func):
    function = 'ltree2text'

    @property
    def output_field(self):
        return TextField()


class CommonAncestor(Func):
    function = 'lca'
