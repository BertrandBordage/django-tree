from importlib.util import find_spec
from django.db.models import QuerySet
from django.utils.functional import cached_property


# The level delimiter separating path segments (see `tree.sql.postgresql`).
DELIMITER = b'\x00'


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

    @cached_property
    def _segments(self):
        # The per-level segments, without their `0x00` terminators. A stored path
        # always ends with a delimiter, so the trailing empty split is dropped.
        if not self.value:
            return []
        return self.value.split(DELIMITER)[:-1]

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
        return iter(self._segments)

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
        segments = self._segments
        if not include_self:
            segments = segments[:-1]
        # Using the lookup `ancestor_of` here is slower, so we explicitly specify
        # the ancestors’ paths (each prefix ending on a `0x00` delimiter).
        paths = [
            DELIMITER.join(segments[:i]) + DELIMITER
            for i in range(1, len(segments) + 1)
        ]
        return self.qs.filter(**{self.attname + '__in': paths})

    def get_descendants(self, include_self=False):
        if not self.value:
            return self.qs.none()
        # Both lookups are range comparisons on the whole path, so they use the
        # btree index backing the path instead of a dedicated slice index. The
        # strict variant excludes self via `> P` alone.
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
            return len(self._segments)

    def is_root(self):
        if self.value:
            return len(self._segments) == 1

    def is_leaf(self):
        if self.value:
            return not self.get_children().exists()

    @staticmethod
    def _as_bytes(other):
        if isinstance(other, Path):
            other = other.value
        if not other:
            return None
        if not isinstance(other, (bytes, bytearray, memoryview)):
            raise TypeError('`other` must be a `Path` instance or a bytes path.')
        return bytes(other)

    def is_ancestor_of(self, other, include_self=False):
        if not self.value:
            return False
        other = self._as_bytes(other)
        if other is None:
            return False
        if not include_self and self.value == other:
            return False
        # The `0x00` terminator of `self.value` makes this a level-aligned prefix
        # test, so a sibling that merely shares leading bytes cannot match.
        return other.startswith(self.value)

    def is_descendant_of(self, other, include_self=False):
        if not self.value:
            return False
        other = self._as_bytes(other)
        if other is None:
            return False
        if not include_self and self.value == other:
            return False
        return self.value.startswith(other)

    @staticmethod
    def register_psycopg2():
        from psycopg2 import Binary
        from psycopg2.extensions import register_adapter, AsIs

        def adapt_path(path):
            if path.value is None:
                return AsIs('NULL')
            return Binary(path.value)

        register_adapter(Path, adapt_path)

    @staticmethod
    def _psycopg3_dumper(fmt):
        # psycopg3's built-in bytea dumpers are C types that cannot be subclassed,
        # so emit the wire format directly: raw bytes for the binary format, the
        # `\x<hex>` escape for the text one.
        import psycopg
        from psycopg.adapt import Dumper
        from psycopg import pq

        bytea_oid = psycopg.adapters.types['bytea'].oid

        class PathDumper(Dumper):
            format = fmt
            oid = bytea_oid

            def dump(self, obj):
                value = b'' if obj.value is None else obj.value
                if self.format == pq.Format.BINARY:
                    return value
                return b'\\x' + value.hex().encode()

        return PathDumper

    @classmethod
    def register_psycopg3(cls):
        import psycopg
        from psycopg import pq

        for fmt in (pq.Format.TEXT, pq.Format.BINARY):
            psycopg.adapters.register_dumper(Path, cls._psycopg3_dumper(fmt))

    @classmethod
    def register_psycopg(cls):
        # Tells psycopg how to prepare a Path object for the database,
        # in case it doesn't go through the ORM.
        if find_spec('psycopg') is not None:
            return cls.register_psycopg3()
        if find_spec('psycopg2') is not None:
            return cls.register_psycopg2()
