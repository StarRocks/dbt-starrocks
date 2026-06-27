import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

@pytest.fixture(scope="class")
def ensure_iceberg_catalog(project):
    catalogs = project.run_sql("SHOW CATALOGS", fetch="all")
    if any(row[0] == "iceberg_catalog" for row in catalogs):
        yield
        return

    candidates = []
    env_uri = os.getenv("STARROCKS_ICEBERG_REST_URI")
    env_s3 = os.getenv("STARROCKS_ICEBERG_S3_ENDPOINT")
    if env_uri and env_s3:
        candidates.append((env_uri, env_s3))

    candidates.extend([
        ("http://host.docker.internal:8181", "http://host.docker.internal:9000"),
        ("http://iceberg-rest:8181", "http://minio:9000"),
        ("http://127.0.0.1:8181", "http://127.0.0.1:9000"),
    ])

    last_error = None
    for rest_uri, s3_endpoint in candidates:
        try:
            project.run_sql(f"""
                CREATE EXTERNAL CATALOG iceberg_catalog
                PROPERTIES (
                    'type'='iceberg',
                    'iceberg.catalog.type'='rest',
                    'iceberg.catalog.uri'='{rest_uri}',
                    'iceberg.catalog.warehouse'='warehouse',
                    'aws.s3.access_key'='admin',
                    'aws.s3.secret_key'='password',
                    'aws.s3.endpoint'='{s3_endpoint}',
                    'aws.s3.enable_path_style_access'='true'
                )
            """)
            project.run_sql(
                "CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_starrocks_catalog_check "
                'PROPERTIES ("location" = "s3://warehouse/dbt_starrocks_catalog_check")'
            )
            project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_starrocks_catalog_check FORCE")
            break
        except Exception as e:
            last_error = e
            project.run_sql("DROP CATALOG IF EXISTS iceberg_catalog")
    else:
        raise RuntimeError(f"Failed to set up iceberg_catalog. Last error: {last_error}")

    try:
        yield
    finally:
        project.run_sql("DROP CATALOG IF EXISTS iceberg_catalog")


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'starrocks',
#        'threads': 1,
#        'server': 'localhost',
        'username': 'root',
        'password': '',
        'port': 9030,
        'host': 'localhost',
#        'version': '3.0',
#        'schema': 'dbt',
#        'ssl_disabled': True
    }
