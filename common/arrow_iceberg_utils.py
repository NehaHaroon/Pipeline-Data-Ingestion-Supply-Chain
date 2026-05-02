"""
PyArrow → PyIceberg (0.6.x) compatibility for Silver/Gold writes.

PyIceberg 0.6 schema conversion rejects timezone-aware and nanosecond timestamps,
dictionary-encoded columns, and (on newer PyArrow) string_view.
We: decode dictionaries / string_view, combine chunks, strip TZ, then coerce timestamps
to naive microseconds with ``safe=False``, milliseconds fallback if needed.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

_MAX_TIMESTAMP_NORMALIZE_ITERS = 8


def _timestamp_field_needs_fix(typ: pa.DataType) -> bool:
    if not pa.types.is_timestamp(typ):
        return False
    if getattr(typ, "tz", None):
        return True
    if typ.unit == "ns":
        return True
    return False


def _schema_has_bad_timestamps(schema: pa.Schema) -> bool:
    return any(_timestamp_field_needs_fix(f.type) for f in schema)


def _decode_dictionary_and_string_view_columns(table: pa.Table) -> pa.Table:
    """
    PyIceberg 0.6 ``visit_pyarrow`` does not accept dictionary-encoded columns or
    ``string_view``; Iceberg scans often return dict-encoded strings for
    low-cardinality columns after pandas round-trips.
    """
    arrays = []
    fields = []
    for field in table.schema:
        col = table[field.name]
        col = _combine_column_chunks(col)
        if pa.types.is_dictionary(col.type):
            vt = col.type.value_type
            try:
                col = pc.cast(col, vt, safe=False)
            except Exception:
                try:
                    col = pc.cast(col, vt)
                except Exception:
                    pass
            col = _combine_column_chunks(col)
        is_sv = getattr(pa.types, "is_string_view", None)
        if callable(is_sv) and is_sv(col.type):
            try:
                col = pc.cast(col, pa.large_string(), safe=False)
            except Exception:
                pass
        fields.append(pa.field(field.name, col.type, nullable=field.nullable))
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _combine_column_chunks(col: pa.ChunkedArray | pa.Array) -> pa.ChunkedArray | pa.Array:
    if isinstance(col, pa.ChunkedArray):
        try:
            return col.combine_chunks()
        except Exception:
            return col
    return col


def _arrow_timestamp_to_iceberg_safe(col: pa.ChunkedArray | pa.Array) -> pa.ChunkedArray | pa.Array:
    """
    Normalize Arrow timestamps to naive microseconds (PyIceberg-friendly), or
    naive milliseconds if conversion to microseconds fails.
    """
    col = _combine_column_chunks(col)
    typ = col.type
    if not pa.types.is_timestamp(typ):
        return col

    unit = typ.unit
    tz = getattr(typ, "tz", None)

    # 1) tz-aware → naive (same unit). Never tz+ns → us in a single cast.
    if tz is not None:
        try:
            col = pc.cast(col, pa.timestamp(unit), safe=False)
        except Exception:
            col = pc.cast(col, pa.timestamp(unit))

    # 2) Narrow unit to sub-second precision Iceberg accepts (microseconds preferred).
    col = _combine_column_chunks(col)
    unit_now = col.type.unit
    if unit_now == "us":
        return col

    try:
        return pc.cast(col, pa.timestamp("us"), safe=False)
    except Exception:
        return pc.cast(col, pa.timestamp("ms"), safe=False)


def arrow_table_timestamps_naive_for_pyiceberg(table: pa.Table) -> pa.Table:
    """Cast timestamp columns for Iceberg (prefer naive ``timestamp[us]``)."""
    arrays = []
    fields = []
    for field in table.schema:
        col = table[field.name]
        col = _arrow_timestamp_to_iceberg_safe(col)
        fields.append(pa.field(field.name, col.type, nullable=field.nullable))
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def prepare_arrow_table_for_iceberg(table: pa.Table) -> pa.Table:
    """
    Single entry point before ``catalog.create_table`` / ``overwrite`` / ``append``:
    combine chunks, decode dictionary/string_view, then normalize timestamps for PyIceberg 0.6.
    Repeats until no nanosecond or timezone-aware timestamp fields remain (bounded).
    """
    try:
        table = table.combine_chunks()
    except Exception:
        pass
    table = _decode_dictionary_and_string_view_columns(table)

    for _ in range(_MAX_TIMESTAMP_NORMALIZE_ITERS):
        try:
            table = table.combine_chunks()
        except Exception:
            pass
        table = arrow_table_timestamps_naive_for_pyiceberg(table)
        if not _schema_has_bad_timestamps(table.schema):
            break
    return table
