-- Check CDC milliseconds, formatted batch timestamps, nulls, and pre-epoch values.
with fixtures as (
    select * from values
        ('1785248638116', cast(1785248638116 as bigint)),
        ('1970-01-01T00:00:01.234Z', cast(1234 as bigint)),
        (' 1234 ', cast(1234 as bigint)),
        ('-1', cast(-1 as bigint)),
        ('0', cast(0 as bigint)),
        (cast(null as string), cast(null as bigint))
    as fixtures(raw_timestamp, expected_millis)
), converted as (
    select *, unix_millis({{ cdc_timestamp('raw_timestamp') }}) as actual_millis
    from fixtures
)
select * from converted
where not (actual_millis <=> expected_millis)
