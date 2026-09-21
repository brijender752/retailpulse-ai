"""Timestamp normalization for ML source tables."""

from pyspark.sql import functions as F


def source_timestamp(column: str):
    """Normalize CDC epoch milliseconds and ordinary date/timestamp values."""
    value = F.trim(F.col(column).cast("string"))
    return F.when(
        value.rlike(r"^[+-]?[0-9]+$"),
        F.timestamp_millis(value.cast("long")),
    ).otherwise(F.to_timestamp(value))
