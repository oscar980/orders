"""
Tests para el módulo de transformaciones.
Incluye mocks de API y validaciones de transformaciones.
"""
import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from src.transforms import DataTransformer
from src.api_client import APIClient


class TestDataTransformer:
    """Tests para DataTransformer."""
    
    @pytest.fixture
    def transformer(self):
        """Fixture para crear un transformer."""
        return DataTransformer()
    
    @pytest.fixture
    def sample_orders(self):
        """Órdenes de ejemplo."""
        return [
            {
                "order_id": "o_1001",
                "user_id": "u_1",
                "amount": 125.50,
                "currency": "USD",
                "created_at": "2025-08-20T15:23:10Z",
                "items": [
                    {"sku": "p_1", "qty": 2, "price": 60.0}
                ],
                "metadata": {"source": "api", "promo": None}
            },
            {
                "order_id": "o_1002",
                "user_id": "u_2",
                "amount": 89.99,
                "currency": "USD",
                "created_at": "2025-08-21T10:15:00Z",
                "items": [
                    {"sku": "p_2", "qty": 1, "price": 89.99}
                ],
                "metadata": {"source": "api", "promo": "SUMMER2025"}
            }
        ]
    
    @pytest.fixture
    def sample_users(self):
        """Usuarios de ejemplo."""
        return pd.DataFrame({
            'user_id': ['u_1', 'u_2'],
            'email': ['user1@example.com', 'user2@example.com'],
            'created_at': ['2025-01-15', '2025-02-20'],
            'country': ['US', 'CA']
        })
    
    @pytest.fixture
    def sample_products(self):
        """Productos de ejemplo."""
        return pd.DataFrame({
            'sku': ['p_1', 'p_2'],
            'name': ['Product A', 'Product B'],
            'category': ['Electronics', 'Clothing'],
            'price': [60.0, 89.99]
        })
    
    def test_normalize_orders_valid(self, transformer, sample_orders):
        """Test normalización de órdenes válidas."""
        result = transformer._normalize_orders(sample_orders)
        
        assert len(result) == 2
        assert 'order_id' in result.columns
        assert 'user_id' in result.columns
        assert 'created_at' in result.columns
        assert result['order_id'].iloc[0] == 'o_1001'
    
    def test_normalize_orders_invalid(self, transformer):
        """Test normalización de órdenes inválidas."""
        invalid_orders = [
            {"order_id": None, "user_id": "u_1"},  # Sin order_id
            {"order_id": "o_1003", "user_id": None},  # Sin user_id
            {"order_id": "o_1004", "user_id": "u_1", "created_at": None},  # Sin fecha
            {"order_id": "o_1005", "user_id": "u_1", "created_at": "2025-08-20T15:23:10Z", "items": []},  # Sin items
        ]
        
        result = transformer._normalize_orders(invalid_orders)
        assert len(result) == 0  # Todas deben ser rechazadas
    
    def test_deduplicate_orders(self, transformer):
        """Test deduplicación de órdenes."""
        orders_df = pd.DataFrame({
            'order_id': ['o_1001', 'o_1001', 'o_1002'],
            'user_id': ['u_1', 'u_1', 'u_2'],
            'created_at': [
                pd.to_datetime('2025-08-20T15:23:10Z', utc=True),
                pd.to_datetime('2025-08-21T15:23:10Z', utc=True),  # Más reciente
                pd.to_datetime('2025-08-22T15:23:10Z', utc=True)
            ],
            'amount': [125.50, 125.50, 89.99],
            'currency': ['USD', 'USD', 'USD'],
            'items': [[], [], []],
            'metadata': [{}, {}, {}]
        })
        
        result = transformer._deduplicate_orders(orders_df)
        
        assert len(result) == 2
        assert 'o_1001' in result['order_id'].values
        assert 'o_1002' in result['order_id'].values
        # Verificar que se mantiene la más reciente
        o_1001_row = result[result['order_id'] == 'o_1001']
        assert o_1001_row['created_at'].iloc[0] == pd.to_datetime('2025-08-21T15:23:10Z', utc=True)
    
    def test_expand_order_items(self, transformer):
        """Test expansión de items de órdenes."""
        orders_df = pd.DataFrame({
            'order_id': ['o_1001', 'o_1002'],
            'user_id': ['u_1', 'u_2'],
            'created_at': [
                pd.to_datetime('2025-08-20T15:23:10Z', utc=True),
                pd.to_datetime('2025-08-21T10:15:00Z', utc=True)
            ],
            'items': [
                [{"sku": "p_1", "qty": 2, "price": 60.0}, {"sku": "p_2", "qty": 1, "price": 30.0}],
                [{"sku": "p_3", "qty": 1, "price": 89.99}]
            ]
        })
        
        result = transformer._expand_order_items(orders_df)
        
        assert len(result) == 3  # 2 items + 1 item
        assert result['order_id'].value_counts()['o_1001'] == 2
        assert result['order_id'].value_counts()['o_1002'] == 1
    
    def test_create_dim_user(self, transformer, sample_orders, sample_users):
        """Test creación de dimensión de usuarios."""
        orders_df = transformer._normalize_orders(sample_orders)
        result = transformer._create_dim_user(orders_df, sample_users)
        
        assert len(result) == 2
        assert 'user_id' in result.columns
        assert 'email' in result.columns
        assert 'country' in result.columns
        assert 'first_order_date' in result.columns
    
    def test_create_dim_product(self, transformer, sample_orders, sample_products):
        """Test creación de dimensión de productos."""
        orders_df = transformer._normalize_orders(sample_orders)
        fact_order_items = transformer._expand_order_items(orders_df)
        result = transformer._create_dim_product(fact_order_items, sample_products)
        
        assert len(result) >= 1
        assert 'sku' in result.columns
        assert 'name' in result.columns
        assert 'category' in result.columns
    
    def test_create_fact_order(self, transformer, sample_orders):
        """Test creación de tabla de hechos."""
        orders_df = transformer._normalize_orders(sample_orders)
        fact_order_items = transformer._expand_order_items(orders_df)
        result = transformer._create_fact_order(orders_df, fact_order_items)
        
        assert len(result) >= 1
        assert 'order_id' in result.columns
        assert 'user_id' in result.columns
        assert 'sku' in result.columns
        assert 'line_total' in result.columns
        assert 'order_date' in result.columns
    
    def test_transform_orders_complete(self, transformer, sample_orders, sample_users, sample_products):
        """Test transformación completa."""
        result = transformer.transform_orders(
            orders=sample_orders,
            users_df=sample_users,
            products_df=sample_products
        )
        
        assert 'dim_user' in result
        assert 'dim_product' in result
        assert 'fact_order' in result
        assert not result['dim_user'].empty
        assert not result['dim_product'].empty
        assert not result['fact_order'].empty
    
    def test_transform_orders_with_since_date(self, transformer, sample_orders, sample_users, sample_products):
        """Test transformación con filtro de fecha."""
        result = transformer.transform_orders(
            orders=sample_orders,
            users_df=sample_users,
            products_df=sample_products,
            since_date='2025-08-21'
        )
        
        # Solo debería incluir órdenes del 2025-08-21 en adelante
        if not result['fact_order'].empty:
            min_date = pd.to_datetime(result['fact_order']['created_at']).min()
            assert min_date >= pd.to_datetime('2025-08-21', utc=True)


class TestAPIClient:
    """Tests para APIClient con mocks."""
    
    @pytest.fixture
    def api_client(self):
        """Fixture para crear un cliente API."""
        return APIClient(max_retries=3, retry_delay=0.1)
    
    def test_fetch_from_file(self, api_client, tmp_path):
        """Test lectura desde archivo."""
        test_file = tmp_path / 'test_orders.json'
        test_data = [
            {"order_id": "o_1", "user_id": "u_1", "amount": 100.0}
        ]
        
        import json
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        result = api_client._fetch_from_file(str(test_file))
        assert len(result) == 1
        assert result[0]['order_id'] == 'o_1'
    
    def test_fetch_from_file_not_found(self, api_client):
        """Test error cuando archivo no existe."""
        with pytest.raises(FileNotFoundError):
            api_client._fetch_from_file('nonexistent.json')
    
    @patch('src.api_client.requests.get')
    def test_fetch_from_api_success(self, mock_get, api_client):
        """Test llamada exitosa a API."""
        mock_response = Mock()
        mock_response.json.return_value = [
            {"order_id": "o_1", "user_id": "u_1"}
        ]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        api_client.base_url = 'http://api.example.com'
        result = api_client._fetch_from_api('/api/v1/orders')
        
        assert len(result) == 1
        mock_get.assert_called_once()
    
    @patch('src.api_client.requests.get')
    def test_fetch_from_api_retry(self, mock_get, api_client):
        """Test reintentos en caso de error."""
        # Primero falla, luego tiene éxito
        import requests
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("Connection error"),
            Mock(json=lambda: [{"order_id": "o_1"}], raise_for_status=Mock())
        ]
        
        api_client.base_url = 'http://api.example.com'
        result = api_client._fetch_from_api('/api/v1/orders')
        
        assert len(result) == 1
        assert mock_get.call_count == 2
    
    @patch('src.api_client.requests.get')
    def test_fetch_from_api_max_retries(self, mock_get, api_client):
        """Test que se lanza excepción después de max_retries."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection error")
        
        api_client.base_url = 'http://api.example.com'
        
        with pytest.raises(requests.exceptions.ConnectionError):
            api_client._fetch_from_api('/api/v1/orders')
        
        assert mock_get.call_count == 3  # max_retries

