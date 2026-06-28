from importlib.util import find_spec
from django.db.models import QuerySet
from django.utils.functional import cached_property


class Path:
    def __init__(self, field, value):
        # Kept minimal: `from_db_value` builds a `Path` for every fetched row, so
        # anything derivable from the field (`attname`, `field_bound`, `qs`) is
        # computed lazily below instead of on this per-row hot path.
        self.field = field
        self.value = value

    @cached_property
    def attname(self):
        return getattr(self.field, 'attname', None)

    @cached_property
    def field_bound(self):
        return self.attname is not None

    @cached_property
    def qs(self):
        # Cloning a queryset here would otherwise cost one clone per loaded row,
        # even when the path is only read (never used to navigate the tree).
        if self.field_bound:
            return self.field.model._default_manager.all()
        return QuerySet()

    def __repr__(self):
        if self.field_bound:
            return '<Path %s %s>' % (self.field, self.value)
        return '<Path %s>' % self.value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        if isinstance(other, Path):
            other = other.value
        return self.value == other

    def __ne__(self, other):
        if isinstance(other, Path):
            other = other.value
        return self.value != other

    def __lt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return True
        return self.value < other

    def __le__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return True
        return self.value <= other

    def __gt__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return True
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        return self.value > other

    def __ge__(self, other):
        # We simulate the effects of a NULLS LAST.
        if not self.value:
            return True
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        return self.value >= other

    def __iter__(self):
        return iter(self.value)

    def get_children(self):
        if not self.value:
            return self.qs.none()
        return self.qs.filter(
            **{
                f'{self.attname}__child_of': self.value,
            }
        )

    def get_ancestors(self, include_self=False):
        if not self.value or (self.is_root() and not include_self):
            return self.qs.none()
        path = self.value
        if not include_self:
            path = path[:-1]
        # Using the lookup `ancestor_of` here is slower,
        # so we explicitly specify the ancestors’ paths.
        return self.qs.filter(
            **{self.attname + '__in': [path[:i] for i in range(1, len(path) + 1)]}
        )

    def get_descendants(self, include_self=False):
        if not self.value:
            return self.qs.none()
        # Both lookups are range comparisons on the whole path, so they use the
        # btree index backing the path instead of a dedicated slice index. The
        # strict variant excludes self via `> P` alone, avoiding a second
        # `array_length(...)` predicate.
        lookup = 'descendant_of' if include_self else 'strict_descendant_of'
        return self.qs.filter(**{f'{self.attname}__{lookup}': self.value})

    def get_siblings(self, include_self=False, queryset=None):
        if not self.value:
            return self.qs.none()

        qs = self.qs if queryset is None else queryset
        qs = qs.filter(
            **{
                self.attname + '__sibling_of': self.value,
            }
        )
        if include_self:
            return qs
        return qs.exclude(**{self.attname: self.value})

    def get_prev_siblings(self, include_self=False, queryset=None):
        if not self.value:
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self, queryset=queryset)
        lookup = '__lte' if include_self else '__lt'
        return siblings.filter(**{self.attname + lookup: self.value}).order_by(
            '-' + self.attname
        )

    def get_next_siblings(self, include_self=False, queryset=None):
        if not self.value:
            return self.qs.none()
        siblings = self.get_siblings(include_self=include_self, queryset=queryset)
        lookup = '__gte' if include_self else '__gt'
        return siblings.filter(**{self.attname + lookup: self.value}).order_by(
            self.attname
        )

    def get_prev_sibling(self, queryset=None):
        if not self.value:
            return
        # Single query: `sibling_of` enforces same parent and depth, while
        # `__lt` against our own path both keeps only earlier siblings and
        # excludes self (a path is never `<` itself).
        qs = self.qs if queryset is None else queryset
        return (
            qs.filter(
                **{
                    self.attname + '__sibling_of': self.value,
                    self.attname + '__lt': self.value,
                }
            )
            .order_by('-' + self.attname)
            .first()
        )

    def get_next_sibling(self, queryset=None):
        if not self.value:
            return None
        qs = self.qs if queryset is None else queryset
        return (
            qs.filter(
                **{
                    self.attname + '__sibling_of': self.value,
                    self.attname + '__gt': self.value,
                }
            )
            .order_by(self.attname)
            .first()
        )

    def get_level(self):
        if self.value:
            return len(self.value)

    def is_root(self):
        if self.value:
            return len(self.value) == 1

    def is_leaf(self):
        if self.value:
            return not self.get_children().exists()

    def is_ancestor_of(self, other, include_self=False):
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        if not isinstance(other, list):
            raise TypeError('`other` must be a `Path` instance or a list of decimals.')
        if not include_self and self.value == other:
            return False
        return other[: len(self.value)] == self.value

    def is_descendant_of(self, other, include_self=False):
        if not self.value:
            return False
        if isinstance(other, Path):
            other = other.value
        if not other:
            return False
        if not isinstance(other, list):
            raise TypeError('`other` must be a `Path` instance or a list of decimals.')
        if not include_self and self.value == other:
            return False
        return self.value[: len(other)] == other

    @staticmethod
    def register_psycopg2():
        from psycopg2.extensions import register_adapter, adapt

        def adapt_path(path):
            return adapt(path.value)

        register_adapter(Path, adapt_path)

    @staticmethod
    def register_psycopg3():
        import psycopg
        from psycopg.types.string import StrDumper

        class PathDumper(StrDumper):
            def quote(self, obj: Path):
                return psycopg.sql.quote(obj.value).encode()

        psycopg.adapters.register_dumper(Path, PathDumper)

    @classmethod
    def register_psycopg(cls):
        # Tells psycopg how to prepare a Path object for the database,
        # in case it doesn't go through the ORM.
        if find_spec('psycopg') is not None:
            return cls.register_psycopg3()
        if find_spec('psycopg2') is not None:
            return cls.register_psycopg2()
