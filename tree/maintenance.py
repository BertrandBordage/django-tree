"""Python path maintenance for backends without a database trigger.

PostgreSQL maintains the ``bytea`` path with a PL/pgSQL ``BEFORE`` trigger (see
:mod:`tree.sql.postgresql`). No portable SQL trigger can do the same -- MySQL
forbids a row trigger from modifying its own table and SQLite cannot loop over
bytes -- so on every other backend :class:`PathMaintainer` reproduces the trigger
body through the ORM, on Django's save cycle (see :mod:`tree.signals`). It is a
faithful port of ``get_update_paths_function_creation``: same ``tree_mid``
placement on insert/move, same base-254 ranks on rebuild, so the resulting tree
structure is identical to PostgreSQL's.

The one thing it cannot cover is a write that bypasses the ORM (raw SQL): with no
trigger, nothing observes it. Call :meth:`TreeModelMixin.rebuild_paths` afterwards.
"""

from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, cast

from django.db import DEFAULT_DB_ALIAS, ProgrammingError
from django.db.models import F, Field, Q

from .sql.helpers import DELIMITER, seg_width, tree_int_to_seg, tree_mid

if TYPE_CHECKING:
    from django.db.models import Model

    from .fields import PathField


# (model label, path attname, db alias) of fields whose Python maintenance is
# currently suspended (the off-PostgreSQL equivalent of `DISABLE TRIGGER`).
_disabled: set[tuple[str, str, str]] = set()


def _key(field: 'PathField', db_alias: str) -> tuple[str, str, str]:
    return field.model._meta.label, field.attname, db_alias


def set_trigger_disabled(field: 'PathField', db_alias: str, disabled: bool) -> None:
    if disabled:
        _disabled.add(_key(field, db_alias))
    else:
        _disabled.discard(_key(field, db_alias))


def is_trigger_disabled(field: 'PathField', db_alias: str) -> bool:
    return _key(field, db_alias) in _disabled


def _to_bytes(value: Any) -> bytes | None:
    # `values()` runs `PathField.from_db_value`, so a stored path comes back as a
    # `Path`; raw drivers may hand back `bytearray`/`memoryview`.
    from .types import Path

    if value is None:
        return None
    if isinstance(value, Path):
        return value.value
    return bytes(value)


def _compare_q(column: str, value: Any, greater: bool | None, strict: bool) -> Q:
    """ORM equivalent of ``tree.sql.base.compare_columns`` (NULLS LAST)."""
    if greater is None:  # equality
        if value is None:
            return Q(**{f'{column}__isnull': True})
        return Q(**{column: value})
    if greater:  # column >= / > value, NULLs sort last (greatest)
        if value is None:
            return Q(**{f'{column}__isnull': True}) if not strict else Q(pk__in=[])
        op = 'gt' if strict else 'gte'
        return Q(**{f'{column}__isnull': True}) | Q(**{f'{column}__{op}': value})
    # column <= / < value, NULLs sort last
    if value is None:
        return Q(**{f'{column}__isnull': False}) if strict else Q()
    op = 'lt' if strict else 'lte'
    return Q(**{f'{column}__{op}': value})


class PathMaintainer:
    def __init__(self, field: 'PathField', db_alias: str = DEFAULT_DB_ALIAS) -> None:
        self.field = field
        self.db_alias = db_alias
        self.model = field.model
        meta = self.model._meta
        self.pk_attname = meta.pk.attname
        self.path_attname = field.attname
        self.parent_attname = field.parent_field.attname

        # Resolve the ordering columns exactly like the PL/pgSQL trigger: each
        # `order_by` entry to its column attname + a descending flag, then append
        # the primary key as a final tie-break unless it is already referenced.
        self.columns: list[str] = []
        self.descending: list[bool] = []
        for field_name in field.order_by:
            descending = field_name.startswith('-')
            if descending:
                field_name = field_name[1:]
            attname = (
                self.pk_attname
                if field_name == 'pk'
                else cast(Field, meta.get_field(field_name)).attname
            )
            self.columns.append(attname)
            self.descending.append(descending)
        if self.pk_attname not in self.columns:
            self.columns.append(self.pk_attname)
            self.descending.append(False)

    @property
    def _base(self):  # type: ignore[no-untyped-def]
        return self.model._base_manager.using(self.db_alias)

    def _order_values(self, instance: 'Model') -> tuple[Any, ...]:
        return tuple(getattr(instance, column) for column in self.columns)

    def capture_old(self, instance: 'Model') -> None:
        """Stash the row's pre-save path/parent/order values on the instance.

        Reads the current DB row so :meth:`on_save` has the trigger's ``OLD.*``.
        For an insert (no row yet, even when the pk is client-generated) nothing
        is stashed.
        """
        if instance.pk is None:
            return
        columns = self._select_columns()
        row = self._base.filter(pk=instance.pk).values(*columns).first()
        if row is None:
            return
        store = instance.__dict__.setdefault('_tree_old', {})
        store[self.path_attname] = (
            _to_bytes(row[self.path_attname]),
            row[self.parent_attname],
            tuple(row[column] for column in self.columns),
        )

    def capture_old_many(
        self, pks: list[Any]
    ) -> dict[Any, tuple[bytes | None, Any, tuple[Any, ...]]]:
        """Pre-write path/parent/order values for a set of rows, in one query.

        Lets a bulk ``update()`` replay :meth:`on_save` per affected row with the
        right ``OLD.*`` -- a targeted move that leaves every other path untouched,
        like the PostgreSQL trigger (a full :meth:`rebuild` would renumber the
        whole tree and invalidate paths the caller still holds).
        """
        rows = self._base.filter(pk__in=pks).values(*self._select_columns())
        return {
            row[self.pk_attname]: (
                _to_bytes(row[self.path_attname]),
                row[self.parent_attname],
                tuple(row[column] for column in self.columns),
            )
            for row in rows
        }

    def _select_columns(self) -> list[str]:
        columns = [self.path_attname, self.parent_attname]
        for column in self.columns:
            if column not in columns:
                columns.append(column)
        return columns

    def _old_state(
        self, instance: 'Model'
    ) -> tuple[bytes | None, Any, tuple[Any, ...]] | None:
        return instance.__dict__.get('_tree_old', {}).get(self.path_attname)

    def _parent_path(self, parent_id: Any) -> bytes:
        if parent_id is None:
            return b''
        path = (
            self._base.filter(pk=parent_id)
            .values_list(self.path_attname, flat=True)
            .first()
        )
        return _to_bytes(path) or b''

    def _nearest_sibling_segment(
        self, parent_id: Any, instance: 'Model', parent_len: int, greater: bool
    ) -> bytes | None:
        qs = self._base
        if parent_id is None:
            qs = qs.filter(**{f'{self.parent_attname}__isnull': True})
        else:
            qs = qs.filter(**{self.parent_attname: parent_id})
        qs = qs.exclude(pk=instance.pk).filter(
            **{f'{self.path_attname}__isnull': False}
        )
        qs = qs.filter(self._sibling_q(self._order_values(instance), greater))
        order = self.path_attname if greater else f'-{self.path_attname}'
        path = qs.order_by(order).values_list(self.path_attname, flat=True).first()
        path = _to_bytes(path)
        if path is None:
            return None
        # Siblings share the parent prefix; their segment is the rest, minus the
        # trailing delimiter (PL/pgSQL: substr(path, parent_len+1, len-parent_len-1)).
        return path[parent_len:-1]

    def _sibling_q(self, values: tuple[Any, ...], greater: bool) -> Q:
        # Lexicographic "nearby sibling" predicate, the ORM twin of
        # `tree.sql.base.get_nearby_sibling_where_clause`.
        n = len(self.columns)
        result = Q()
        matched_any = False
        for pivot in range(n):
            clause = Q()
            for i in range(pivot + 1):
                if i == pivot:
                    clause &= _compare_q(
                        self.columns[i],
                        values[i],
                        greater=greater != self.descending[i],
                        strict=pivot < n - 1,
                    )
                else:
                    clause &= _compare_q(self.columns[i], values[i], None, False)
            result |= clause
            matched_any = True
        return result if matched_any else Q()

    def on_save(self, instance: 'Model', created: bool) -> None:
        if is_trigger_disabled(self.field, self.db_alias):
            return
        parent_id = getattr(instance, self.parent_attname)
        new_parent_path = self._parent_path(parent_id)
        parent_len = len(new_parent_path)
        old = self._old_state(instance)

        if not created and old is not None:
            old_path, old_parent_id, old_values = old
            if (
                old_path is not None
                and old_parent_id == parent_id
                and old_values == self._order_values(instance)
            ):
                return  # nothing the path depends on changed

        prev_seg = self._nearest_sibling_segment(
            parent_id, instance, parent_len, greater=False
        )
        next_seg = self._nearest_sibling_segment(
            parent_id, instance, parent_len, greater=True
        )

        if not created and old is not None:
            old_path, old_parent_id, _ = old
            if old_path is not None and old_parent_id == parent_id:
                old_seg = old_path[parent_len:-1]
                # Keep the current path when it is still between the neighbours,
                # even if not exactly in the middle (avoids needless rewrites).
                if (prev_seg is None or old_seg > prev_seg) and (
                    next_seg is None or old_seg < next_seg
                ):
                    return

        old_path = old[0] if old is not None else None
        if old_path and new_parent_path.startswith(old_path):
            # Same error the PL/pgSQL trigger raises, for cross-backend parity.
            raise ProgrammingError('Cannot set itself or a descendant as parent.')

        new_path = new_parent_path + tree_mid(prev_seg, next_seg) + DELIMITER
        self._write(instance.pk, new_path)
        instance.__dict__[self.path_attname] = new_path

        if old_path is not None and old_path != new_path:
            self._rewrite_descendants(instance.pk, old_path, new_path)

    def _write(self, pk: Any, path: bytes) -> None:
        self._base.filter(pk=pk).update(**{self.path_attname: path})

    def _rewrite_descendants(self, pk: Any, old_path: bytes, new_path: bytes) -> None:
        rows = (
            self._base.filter(**{f'{self.path_attname}__descendant_of': old_path})
            .exclude(pk=pk)
            .values_list(self.pk_attname, self.path_attname)
        )
        to_update = []
        prefix_len = len(old_path)
        for child_pk, path in rows:
            path = _to_bytes(path)
            if path is None or not path.startswith(old_path):
                continue
            obj = self.model(**{self.pk_attname: child_pk})
            setattr(obj, self.path_attname, new_path + path[prefix_len:])
            to_update.append(obj)
        if to_update:
            self._base.bulk_update(to_update, [self.path_attname])

    def rebuild(self) -> None:
        """Recompute every path from the roots down, matching the PL/pgSQL
        recursive-CTE rebuild (base-254 ranks via ``tree_int_to_seg``).

        Children are ordered by the database (not in Python) so the rank order is
        the same collation the insert-time placement uses -- a rebuild never
        reorders siblings relative to how they were inserted.
        """
        # Order children by the database, with PostgreSQL's default NULL placement
        # (ASC => NULLS LAST, DESC => NULLS FIRST) made explicit so SQLite/MySQL
        # rank identically, and a `pk` tie-break to match the trigger.
        order_by = []
        for field_name in self.field.order_by:
            descending = field_name.startswith('-')
            expression = F(field_name[1:] if descending else field_name)
            order_by.append(
                expression.desc(nulls_first=True)
                if descending
                else expression.asc(nulls_last=True)
            )
        order_by.append(F('pk').asc())
        rows = list(
            self._base.order_by(*order_by).values(
                self.pk_attname, self.parent_attname, self.path_attname
            )
        )

        children: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            children[row[self.parent_attname]].append(row)

        paths: dict[Any, bytes] = {}

        def assign(group: list[dict[str, Any]], parent_path: bytes) -> None:
            width = seg_width(len(group))
            for rank, row in enumerate(group):
                paths[row[self.pk_attname]] = (
                    parent_path + tree_int_to_seg(rank, width) + DELIMITER
                )

        assign(children.get(None, []), b'')
        queue = deque(row[self.pk_attname] for row in children.get(None, []))
        while queue:
            pk = queue.popleft()
            group = children.get(pk, [])
            assign(group, paths[pk])
            queue.extend(row[self.pk_attname] for row in group)

        to_update = []
        for row in rows:
            pk = row[self.pk_attname]
            path = paths.get(pk)
            if path is None or _to_bytes(row[self.path_attname]) == path:
                continue
            obj = self.model(**{self.pk_attname: pk})
            setattr(obj, self.path_attname, path)
            to_update.append(obj)
        if to_update:
            self._base.bulk_update(to_update, [self.path_attname])
