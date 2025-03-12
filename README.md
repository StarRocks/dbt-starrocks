# dbt-starrocks

![PyPI](https://img.shields.io/pypi/v/dbt-starrocks)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt-starrocks)
![PyPI - Downloads](https://img.shields.io/pypi/dw/dbt-starrocks)

This project is **under development**.


The `dbt-starrocks` package contains all the code to enable [dbt](https://getdbt.com) to work with [StarRocks](https://www.starrocks.io).

This is an experimental plugin:
- We have not tested it extensively
- Requires StarRocks version 2.5.0 or higher  
  - version 3.1.x is recommended
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
|        ❌         |          ✅          |        ✅         |        ✅         |  Microbatch (Insert Overwrite)   |
|        ❌         |          ❌          |        ❌         |        ✅         | Microbatch (Dynamic Overwrite)   |

### Notice
1. When StarRocks Version < 2.5, `Create table as` can only set engine='OLAP' and table_type='DUPLICATE'
2. When StarRocks Version >= 2.5, `Create table as` supports table_type='PRIMARY'
3. When StarRocks Version < 3.1 distributed_by is required

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
      schema: analytics
      username: your_starrocks_username
      password: your_starrocks_password
```

| Option   | Description                                             | Required? | Example                        |
|----------|---------------------------------------------------------|-----------|--------------------------------|
| type     | The specific adapter to use                             | Required  | `starrocks`                    |
| host     | The hostname to connect to                              | Required  | `192.168.100.28`               |
| port     | The port to use                                         | Required  | `9030`                         |
| schema   | Specify the schema (database) to build models into      | Required  | `analytics`                    |
| username | The username to use to connect to the server            | Required  | `dbt_admin`                    |
| password | The password to use for authenticating to the server    | Required  | `correct-horse-battery-staple` |
| version  | Let Plugin try to go to a compatible starrocks version  | Optional  | `3.1.0`                        |
| use_pure | set to "true" to use C extensions                       | Optional  | `true`                         |

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
  properties: [{"replication_num":"1", "in_memory": "true"}]
  refresh_method: 'async'               // only for materialized view default manual
  
  // For 'materialized=incremental' in version >= 3.4
  incremental_strategy: 'dynamic_overwrite' // Supported values: ['default', 'insert_overwrite', 'dynamic_overwrite']
```
  
### dbt run config:
#### Example configuration:
```
{{ config(materialized='view') }}
{{ config(materialized='table', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='table', indexs=[{ 'columns': 'idx_column' }]) }}
{{ config(materialized='table', partition_by=['date_trunc("day", first_order)'], partition_type='Expr') }}
{{ config(materialized='table', table_type='PRIMARY', keys=['customer_id'], order_by=['first_name', 'last_name'] }}
{{ config(materialized='incremental', table_type='PRIMARY', engine='OLAP', buckets=32, distributed_by=['id']) }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='dynamic_overwrite') }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='microbatch', event_time='day', begin='2025-01-01', lookback=1, batch_size='day') }}
{{ config(materialized='incremental', partition_by=['my_partition_key'], partition_type='Expr', incremental_strategy='microbatch', event_time='day', begin='2025-01-01', lookback=1, batch_size='day', microbatch_use_dynamic_overwrite=true) }}
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

## Dynamic Overwrite (StarRocks >= 3.4)
Add a new `incremental_strategy` property that supports the following values:
- `default` (or omitted): Standard inserts without `overwrite`.
- `insert_overwrite`: Will apply `overwrite` with `dynamic_overwrite = false` to the inserts.
- `dynamic_overwrite`: Will apply `overwrite` with `dynamic_overwrite = true` to the inserts.

For more details on the different behaviors, see [StarRocks' documentation for INSERT](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT).

## Test Adapter
Run the following
```
python3 -m pytest tests/functional
```
consult [the project](https://github.com/dbt-labs/dbt-adapter-tests)

## Contributing
We welcome you to contribute to dbt-starrocks. Please see the [Contributing Guide](https://github.com/StarRocks/starrocks/blob/main/CONTRIBUTING.md) for more information.
