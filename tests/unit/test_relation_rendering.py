import pytest
from unittest.mock import Mock, patch
from dbt.adapters.starrocks.relation import StarRocksRelation


class TestCatalogRelationRendering:
    """Test relation name rendering with external catalogs"""
    
    def test_relation_with_external_catalog(self):
        """
        Test that relations render as catalog.database.table
        when catalog is explicitly set
        """
        relation = StarRocksRelation.create(
            database='iceberg_catalog',
            schema='analytics_db',
            identifier='my_table',
            type='table'
        )
        
        # Should render as: iceberg_catalog.analytics_db.my_table
        rendered = relation.render()
        assert 'iceberg_catalog' in rendered
        assert 'analytics_db' in rendered
        assert 'my_table' in rendered
        assert rendered.index('iceberg_catalog') < rendered.index('analytics_db')
        assert rendered.index('analytics_db') < rendered.index('my_table')
    
    def test_relation_without_catalog_uses_default(self):
        """
        Test that relations use default_catalog when no catalog specified
        """
        relation = StarRocksRelation.create(
            database=None,
            schema='analytics',
            identifier='my_table',
            type='table'
        )
        
        # Should fall back to default behavior
        rendered = relation.render()
        assert 'analytics' in rendered
        assert 'my_table' in rendered
        assert rendered.index('analytics') < rendered.index('my_table')


class TestNestedNamespaceRendering:
    """Dotted (nested) Iceberg namespaces must be quoted as a single name"""

    def test_nested_namespace_with_catalog(self):
        relation = StarRocksRelation.create(
            database='iceberg__main__dev',
            schema='bronze.nz_stats.census',
            identifier='cln_dwellings_sa1_2023',
            type='table'
        )

        assert relation.render() == (
            '`iceberg__main__dev`.`bronze.nz_stats.census`.`cln_dwellings_sa1_2023`'
        )

    def test_two_level_namespace_with_catalog(self):
        relation = StarRocksRelation.create(
            database='iceberg__main__dev',
            schema='bronze.spatialytics',
            identifier='t',
            type='table'
        )

        assert relation.render() == '`iceberg__main__dev`.`bronze.spatialytics`.`t`'

    def test_plain_schema_with_catalog_unchanged(self):
        relation = StarRocksRelation.create(
            database='iceberg_catalog',
            schema='analytics_db',
            identifier='my_table',
            type='table'
        )

        assert relation.render() == '`iceberg_catalog`.`analytics_db`.`my_table`'

    def test_catalog_dot_db_in_schema_without_catalog(self):
        relation = StarRocksRelation.create(
            schema='mycat.mydb',
            identifier='t',
            type='table'
        )

        assert relation.render() == '`mycat`.`mydb`.`t`'

    def test_nested_namespace_in_schema_without_catalog_keeps_every_part(self):
        # Splits on the first dot only, so no part of the name is dropped
        relation = StarRocksRelation.create(
            schema='a.b.c',
            identifier='t',
            type='table'
        )

        assert relation.render() == '`a`.`b.c`.`t`'

    def test_without_identifier_with_catalog(self):
        # Used by drop_schema; previously raised TypeError on the None identifier
        relation = StarRocksRelation.create(
            database='iceberg__main__dev',
            schema='bronze.nz_stats.census',
            identifier='t',
            type='table'
        )

        assert relation.without_identifier().render() == (
            '`iceberg__main__dev`.`bronze.nz_stats.census`'
        )
