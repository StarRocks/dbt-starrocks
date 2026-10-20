# dbt-starrocks

![PyPI](https://img.shields.io/pypi/v/dbt-starrocks)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt-starrocks)
![PyPI - Downloads](https://img.shields.io/pypi/dw/dbt-starrocks)

This project is **under development**.


The `dbt-starrocks` package contains all the code to enable [dbt](https://getdbt.com) to work with [StarRocks](https://www.starrocks.io).

- Requires StarRocks version 2.5.0 or higher
  - version 3.4.x is recommended
  - StarRocks versions 2.4 and below are no longer supported


## Installation

This plugin can be installed via pip:

```shell
$ pip install dbt-starrocks
```

## Supported features
| StarRocks <= 2.5 | StarRocks 2.5 ~ 3.1 | StarRocks >= 3.1 | StarRocks >= 3.4 |              Feature              |
|:----------------:|:-------------------:|:----------------:|:----------------:|:---------------------------------:|
|        ✅         |          ✅          |        ✅         |        ✅         |       Table materialization       |
|        ✅         |          ✅          |        ✅         |        ✅         |       View materialization        |
|        ❌         |          ❌          |        ✅         |        ✅         | Materialized View materialization |
|        ❌         |          ✅          |        ✅         |        ✅         |    Incremental materialization    |
|        ❌         |          ✅          |        ✅         |        ✅         |         Primary Key Model         |
|        ✅         |          ✅          |        ✅         |        ✅         |              Sources              |
|        ✅         |          ✅          |        ✅         |        ✅         |         Custom data tests         |
|        ✅         |          ✅          |        ✅         |        ✅         |           Docs generate           |
|        ❌         |          ❌          |        ✅         |        ✅         |       Expression Partition        |
|        ❌         |          ❌          |        ❌         |        ❌         |               Kafka               |
|        ❌         |          ❌          |        ❌         |        ✅         |         Dynamic Overwrite         |
|        ❌         |         *4          |        *4        |        ✅         |            Submit task            |
|        ❌         |          ✅          |        ✅         |        ✅         |  Microbatch (Insert Overwrite)   |
|        ❌         |          ❌          |        ❌         |        ✅         | Microbatch (Dynamic Overwrite)   |

### Notice
1. When StarRocks Version < 2.5, `Create table as` can only set engine='OLAP' and table_type='DUPLICATE'
2. When StarRocks Version >= 2.5, `Create table as` supports table_type='PRIMARY'
3. When StarRocks Version < 3.1 distributed_by is required
4. Verify the specific `submit task` support for your version, see [SUBMIT TASK](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK/).
5. **Views:** when a view's SQL is unchanged, `dbt run` issues no DDL on the view, leaving it in place (the run log notes `skip <view>`, and the model still completes as a successful no-op). This avoids deactivating dependent materialized views, which StarRocks does whenever a base view is recreated, even with identical SQL.

## Profile Configuration

**Example entry for profiles.yml:**

```
starrocks:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: localhost
      port: 9030
      catalog: default_catalog
      schema: analytics
      username: your_starrocks_username
      password: your_starrocks_password
```

| Option              | Description                                                        | Required? | Example                        |
|---------------------|--------------------------------------------------------------------|-----------|--------------------------------|
| type                | The specific adapter to use                                        | Required  | `starrocks`                    |
| host                | The hostname to connect to                                         | Required  | `192.168.100.28`               |
| port                | The port to use                                                    | Required  | `9030`                         |
| catalog             | Specify the catalog to build models into                           | Optional  | `default_catalog`
| schema              | Specify the schema (database in StarRocks) to build models into    | Required  | `analytics`                    |
| username            | The username to use to connect to the server                       | Required  | `dbt_admin`                    |
| password            | The password to use for authenticating to the server               | Required  | `correct-horse-battery-staple` |
| version             | Let Plugin try to go to a compatible starrocks version             | Optional  | `3.1.0`                        |
| use_pure            | set to "true" to use C extensions                                  | Optional  | `true`                         |
| is_async            | "true" to submit suitable tasks as etl tasks.                      | Optional  | `true`                         |
| async_query_timeout | Sets the `query_timeout` value when submitting a task to StarRocks | Optional  | `300`                          |
| poll_interval       | Base delay in seconds between task status polls                    | Optional  | `1`                            |
| poll_max_delay      | Maximum delay cap in seconds for task polling                      | Optional  | `600`                          |
| poll_factor         | Growth multiplier for exponential backoff between polls            | Optional  | `2.0`                          |

More details about setting `use_pure` and other connection arguments [here](https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html)


## Example

### dbt seed properties(yml):
#### Complete configuration:
```
models:
  materialized: table                   // table, view, materialized_view or incremental
  engine: 'OLAP'
  keys: ['id', 'name', 'some_date']
  table_type: 'PRIMARY'                 // PRIMARY or DUPLICATE or UNIQUE
  distributed_by: ['id']
  buckets: 3                            // leave empty for auto bucketing
  indexs=[{ 'columns': 'idx_column' }]
  partition_by: ['some_date']
  partition_by_init: ["PARTITION p1 VALUES [('1971-01-01 00:00:00'), ('1991-01-01 00:00:00')),PARTITION p1972 VALUES [('1991-01-01 00:00:00'), ('1999-01-01 00:00:00'))"]
  // RANGE, LIST, or Expr partition types should be used in conjunction with partition_by configuration
  // Expr partition type requires an expression (e.g., date_trunc) specified in partition_by
  order_by: ['some_column']             // only for PRIMARY table_type
  partition_type: 'RANGE'               // RANGE or LIST or Expr Need to be used in combination with partition_by configuration
  properties: {"replication_num":"1", "in_memory": "true"}
  refresh_method: 'async'               // only for materialized view default manual
  on_view_exists: 'replace'             // only for view: use CREATE OR REPLACE VIEW instead of DROP + CREATE

  // For 'materialized=incremental'
  incremental_strategy: 'merge'           // Supported values: ['default', 'append', 'merge', 'insert_overwrite', 'dynamic_overwrite', 'microbatch'] ('default' will be deprecated)
  unique_key: ['id']                      // Makes the table PRIMARY KEY, as 'merge' requires; not allowed with 'append'
  // 'insert_overwrite' and 'dynamic_overwrite' require version >= 3.4

  // For 'materialized=incremental' and 'incremental_strategy=microbatch'
  event_time: 'some_timestamp_column'     // The column name of the event time
  begin: '2025-01-01'                     // The start time of the incremental data
  lookback: 1                             // The lookback time of the each incremental run
  batch_size: 'day'                       // The batch size. Supported values ['year', 'month', 'day', 'hour']
  microbatch_use_dynamic_overwrite: true  // Whether to use dynamic_overwrite in version >= 3.4
```

### dbt run config:
#### Example configuration:
```
{{ config(materialized='view') }}
{{ config(materialized='view', on_view_exists='replace') }}
{{ config(materialized='table', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='table', indexs=[{ 'columns': 'idx_column' }]) }}
{{ config(materialized='table', partition_by=['date_trunc("day", first_order)'], partition_type='Expr') }}
{{ config(materialized='table', table_type='PRIMARY', keys=['customer_id'], order_by=['first_name', 'last_name'] }}
{{ config(materialized='incremental', table_type='PRIMARY', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['id']) }}
{{ config(materialized='incremental', incremental_strategy='append') }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='dynamic_overwrite') }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='microbatch', event_time='report_day', begin='2025-01-01', lookback=1, batch_size='day') }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='microbatch', event_time='report_day', begin='2025-01-01', lookback=1, batch_size='day', microbatch_use_dynamic_overwrite=true) }}
{{ config(materialized='materialized_view') }}
{{ config(materialized='materialized_view', properties={"storage_medium":"SSD"}) }}
{{ config(materialized='materialized_view', refresh_method="ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)") }}
```
For materialized view only support partition_by、buckets、distributed_by、properties、refresh_method configuration.

## Read From Catalog
First you need to add this catalog to starrocks. The following is an example of hive.
```mysql
CREATE EXTERNAL CATALOG `hive_catalog`
PROPERTIES (
    "hive.metastore.uris"  =  "thrift://127.0.0.1:8087",
    "type"="hive"
);
```
How to add other types of catalogs can be found in the documentation.
https://docs.starrocks.io/en-us/latest/data_source/catalog/catalog_overview
Then write the sources.yaml file.
```yaml
sources:
  - name: external_example
    schema: hive_catalog.hive_db
    tables:
      - name: hive_table_name
```
Finally, you might use below marco quote
```
{{ source('external_example', 'hive_table_name') }}
```

## Write To Catalog
Set `catalog` in a model's config, or in the profile, to build it inside an external catalog.
`database` names the database in that catalog. Without it, `table` and `incremental` models
use the model's resolved schema, as models in `default_catalog` do: the profile's `schema`, or
`<schema>_<custom>` when the model sets a custom schema with `schema` or `+schema`.
```
{{ config(materialized='table', catalog='iceberg_catalog', database='iceberg_db') }}
{{ config(materialized='incremental', incremental_strategy='dynamic_overwrite',
          catalog='iceberg_catalog', database='iceberg_db',
          partition_by=['day(`modified_at`)']) }}
{{ config(materialized='table', catalog='iceberg_catalog', schema='marts') }}
```
The table name comes from the model's file name (or its `alias`), not from `database`.

Only `partition_by`, `partition_type` (defaulting to `Expr`), `partition_by_init` and
`properties` are applied to external tables. `table_type`, `keys`, `distributed_by`,
`buckets` and `order_by` describe OLAP tables and are ignored here — Iceberg rejects key
clauses outright.

Because there are no keys, `unique_key` cannot deduplicate an external-catalog incremental
model: `INSERT INTO` appends. Replace partitions instead, with `insert_overwrite` or
`dynamic_overwrite`. On Iceberg both replace only the partitions the query produced and
leave the others in place, so they behave alike there; the difference between them applies
to OLAP tables. Row-level `unique_key` semantics would need `DELETE` on Iceberg, which
StarRocks only writes position delete files for from 4.1 onwards.

## Incremental strategies
The `incremental_strategy` config supports the following values:

- `append`: Plain `INSERT INTO`. Rows are added; nothing is deduplicated or replaced.
  Requires a DUPLICATE KEY table, so `unique_key` cannot be set.
- `merge`: Upsert on the table's PRIMARY KEY. Requires a PRIMARY KEY table: set `unique_key`,
  or `table_type='PRIMARY'` with `keys`.
- `insert_overwrite`: Applies `overwrite` with `dynamic_overwrite = false` (StarRocks >= 3.4).
- `dynamic_overwrite`: Applies `overwrite` with `dynamic_overwrite = true` (StarRocks >= 3.4).
- `microbatch`: Processes the model in time-based batches; see the microbatch configs above.
- `default` (or omitted): Behaviour depends on the table type. **Will be deprecated** in a
  future release; see below.

`append` and `merge` check the existing table's key type on every incremental run. If the
model's config changed since the table was built (for example, `unique_key` was added),
the run fails until you rebuild the table with `dbt run --full-refresh`.

### How `merge` works
StarRocks has no `MERGE` statement. Instead, setting `unique_key` on an incremental model
builds the target as a [PRIMARY KEY table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/)
keyed on those columns, where an ordinary `INSERT INTO` *is* an upsert: matching keys are
replaced, new keys are inserted.

Three consequences worth knowing:

- The whole row is always replaced, so `merge_update_columns` and `merge_exclude_columns`
  are not supported and will raise an error rather than be silently ignored.
- On a partitioned table, StarRocks requires the partition column to be part of the primary
  key, so `unique_key` must include your `partition_by` column.
- The PRIMARY KEY is set when the table is created, and incremental runs upsert on that key.
  Changing `unique_key` (or `keys`) on an existing model has no effect until the table is
  rebuilt with `dbt run --full-refresh`, or its key is migrated outside dbt.

`merge` is not supported for models in an external catalog, whose tables have no PRIMARY KEY.

### `default` will be deprecated
`default`, which is also what you get when `incremental_strategy` is not set, runs a plain
`INSERT INTO` whose effect depends on the table type: it upserts into a PRIMARY KEY table and
appends to a DUPLICATE KEY table. It is still fully supported, and each run logs a notice
that it will be deprecated in a future release. To prepare, switch to the explicit strategy
for your table, which runs the same SQL:

| Table built by the model | Use |
|---|---|
| PRIMARY KEY (`unique_key` set, or `table_type='PRIMARY'`) | `merge` |
| DUPLICATE KEY (the default table type) | `append` |
| External catalog | `append` |

For more details on the different behaviors, see [StarRocks' documentation for INSERT](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT).

## Submittable ETL tasks

> The implementation of the submittable etl is located in the `impl.py` file.

Setting `is_async: true` in your `profiles.yml` will enable submitting suitable ETL tasks using the `submit task` feature of StarRocks.

This will be automatically wrapped around any statement that supports submission. Setting this manually is currently not supported by the adapter.

The following statements will be submitted automatically:

- `CREATE AS ... SELECT`
- `INSERT INTO|OVERWRITE`
- `CACHE SELECT ...`

> See [StarRocks' documentation on SUBMIT TASK](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK/)

### Task Polling

Once the task has been submitted, the adapter will periodically poll StarRocks' `information_schema.task_runs` to retrieve the task status.

The polling is implemented using a configurable exponential backoff. The adapter's connection to the StarRocks' cluster will not be maintained during the waiting period. It will be re-opened right before the next status polling phase.

The polling delay is calculated as: `min(poll_max_delay, poll_interval * (poll_factor ^ attempt))`

| Option         | Description                              | Default |
|----------------|------------------------------------------|---------|
| poll_interval  | Base delay in seconds between polls      | `1`     |
| poll_max_delay | Maximum delay cap in seconds             | `600`   |
| poll_factor    | Growth multiplier for exponential backoff | `2.0`   |

For example, with `poll_interval: 5`, `poll_factor: 1.5`, `poll_max_delay: 60`:

| Attempt | Delay |
|---------|-------|
| 1       | 7.5s  |
| 2       | 11.25s |
| 3       | 16.9s |
| 4       | 25.3s |
| 5       | 38s   |
| 6       | 57s   |
| 7+      | 60s (capped) |

Compared to defaults (`poll_interval: 1`, `poll_factor: 2.0`, `poll_max_delay: 600`):

| Attempt | Delay |
|---------|-------|
| 1       | 2s    |
| 2       | 4s    |
| 3       | 8s    |
| 4       | 16s   |
| 5       | 32s   |
| 6       | 64s   |
| 7       | 128s  |
| 8       | 256s  |
| 9       | 512s  |
| 10+     | 600s (capped) |

### Controlling the task timeout

Using the `async_query_timeout` property in the `profiles.yml` will control the value of the `query_timeout` when submitting task.

It's going to be injected in the SQL query submitted to StarRocks:

```sql
submit /*+set_var(query_timeout={async_query_timeout})*/ task ...
```

### Example `profiles.yml` configuration

```yml
my_profile:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: host
      port: 9030
      schema: schema
      username: username
      password: password
      is_async: true
      async_query_timeout: 3600 # 1 hour
      poll_interval: 5
      poll_max_delay: 60
      poll_factor: 1.5
```

## Test Adapter
Prepare env: install dev requirements and run StarRocks docker image
```
pip install -r dev_requirements.txt
docker run -p 9030:9030 starrocks/allin1-ubuntu:3.5-latest
```
Run the following
```
python3 -m pytest tests/functional
```
consult [the project](https://github.com/dbt-labs/dbt-adapter-tests)

## Contributing
We welcome you to contribute to dbt-starrocks. Please see the [Contributing Guide](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for more information.
