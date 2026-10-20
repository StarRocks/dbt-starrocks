import pytest
from dbt.adapters.starrocks.impl import StarRocksAdapter


class TestDBTLikeIsSubmittableETL:
    @pytest.mark.parametrize(
        "sql, expected",
        [
            # DBT-like statements
            (
                """ create table `my_db`.`my_table`
                PRIMARY KEY (key1, key2, key3) PARTITION BY (`key1`)
                DISTRIBUTED BY HASH (another_key) BUCKETS 5 as 
                PROPERTIES (
                  "replication_num" = "1"
                )
                as with source as (
                    select * from `my_db`.`my_table`
                ),
                renamed as (
                    select
                        *
                    from source
                )
                select * from renamed
                """, True
            ),
            (
                """
                insert into `my_table`.`my_db` (`id`, `value`) values
                (%s,%s),(%s,%s),(%s,%s),(%s,%s)
                """,
                True
            ),
            (
                """
                cache select   *
                from `my_table`.`my_db`
                """,
                True
            ),
        ]
    )
    def test_is_submittable_etl_suitable(self, sql, expected):
        assert StarRocksAdapter._is_submittable_etl(sql) == expected