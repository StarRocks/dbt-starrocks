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