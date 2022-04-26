from dbt.adapters.base import AdapterPlugin

from dbt.adapters.starrocks.connections import StarRocksAdapterCredentials
from dbt.adapters.starrocks.impl import StarRocksAdapter
from dbt.include import starrocks

Plugin = AdapterPlugin(
    adapter=StarRocksAdapter,
    credentials=StarRocksAdapterCredentials,
    include_path=starrocks.PACKAGE_PATH,
)
