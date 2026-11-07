# Day 46 - Cross-city Compatibility Test

## Objective

Validate that Auckland Transport (AT) and Metlink GTFS Static
feeds can both be projected into the same canonical Silver schema.

The goal is provider compatibility, not provider-specific schema
expansion.

## 1. Providers and Source Data

Validated GTFS Static dimensions:

| Provider | Routes | Stops | Trips | Total |
|---|---:|---:|---:|---:|
| Auckland Transport (AT) | 215 | 7,010 | 37,894 | 45,119 |
| Metlink | 245 | 3,148 | 19,157 | 22,550 |

Both providers were successfully projected through
`build_silver_dimensions`.

## 2. Canonical Schema Validation

Canonical dimensions are defined in:

`src/transport_lakehouse/contracts/canonical.py`

The compatibility contract covers:

- Routes
- Stops
- Trips

Each projected record must contain exactly the canonical fields
defined in `STATIC_DIMENSION_FIELDS`.

Each non-null value must match the expected Python type defined
in `STATIC_DIMENSION_FIELD_TYPES`.

Shared entity identifiers remain strings:

- route_id
- stop_id
- trip_id

## 3. Provider-specific Differences

The following source differences were validated:

| Difference | AT | Metlink | Canonical behavior |
|---|---|---|---|
| routes provider-only fields | contract_id and other source fields | route_color and other source fields | Excluded from canonical output |
| trips provider-only fields | trip_short_name | etm_id | Excluded from canonical output |
| stops wheelchair_boarding | Present | Absent | Nullable canonical field |
| route_type | Standard values | Includes 712 | Preserved as integer |

Additional observations from the real data validation:

- Both providers had zero trips referencing missing routes.
- Metlink had 714 trips with missing trip_headsign values.
- Metlink had 3 stops with missing zone_id values.
- Metlink omitted wheelchair_boarding from all 3,148 stops.
- AT had no missing trip_headsign or wheelchair_boarding values
  in the validated dataset.

Missing optional values are represented as `None` in the canonical
Silver records.

## 4. Compatibility Tests

Test file:

`tests/unit/test_cross_city_compatibility.py`

Coverage includes:

1. Both providers project to exactly the canonical field sets.
2. Both providers produce canonical field types.
3. Provider-specific fields do not leak into canonical output.
4. Missing Metlink wheelchair_boarding remains nullable.
5. Extended Metlink route_type value 712 remains an integer.
6. Shared entity identifiers remain strings.
7. Missing optional GTFS fields remain nullable.

The provider fixtures are synthetic and deterministic so the tests
can run in CI without depending on local GTFS files.

## 5. Test Results

Cross-city compatibility tests:

- 11 passed
- 0 skipped

Full project regression:

- 470 passed
- 14 skipped

The skipped tests require optional external integration services:
real S3 credentials/configuration or a PostgreSQL integration service.

Lint:

- `ruff check .` passed.

A Prefect logging error was emitted during temporary server shutdown
after pytest had reported its successful result. It did not change
the pytest outcome.

## 6. Acceptance

- [x] AT Static data validated against the canonical schema.
- [x] Metlink Static data validated against the canonical schema.
- [x] Provider-specific fields excluded from canonical output.
- [x] Optional fields handled as nullable values.
- [x] Extended Metlink route_type preserved as an integer.
- [x] Shared entity identifiers remain strings.
- [x] Cross-city compatibility tests passed.
- [x] Full pytest suite passed.
- [x] Ruff checks passed.

## Outcome

Day 46 Cross-city Compatibility Test completed.

AT and Metlink Static feeds share the canonical Silver dimension
contract without requiring provider-specific output schemas.
