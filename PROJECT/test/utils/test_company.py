import pytest
from unittest.mock import patch, MagicMock
from datetime import timedelta
from io import StringIO
import json

# Assuming the provided code is in a module named `company_module`
from src.utils.company import Product, Company, TrackingTier, deserialize_companies_from_json, serialize_companies_to_json, convert_to_underscore_nomenclature


# Test for Product Class
@pytest.fixture
def product():
    return Product("Product One", ["keyword1", "keyword2"], "basic")

def test_product_initialization(product):
    assert product.name == "product_one"  # Assert that name is in the expected format
    assert product.keywords == ["keyword1", "keyword2"]
    assert product.tracking_tier == "basic"

def test_convert_to_underscore_nomenclature(product):
    assert convert_to_underscore_nomenclature("Product One") == "product_one"

def test_product_to_dict():
    product = Product("Product One", ["keyword1", "keyword2"], TrackingTier.BASIC, {"start_date": "today"})
    expected_dict = {
        "product_name": "product_one",
        "product_keywords": ["keyword1", "keyword2"],
        "tracking_tier": "basic",
        "usecase_airflow_args": {"start_date": "today"}
    }
    assert product.to_dict() == expected_dict


# Test for Company Class
@pytest.fixture
def company():
    return Company("company123")

def test_add_product(company):
    company.add_product("Product One", ["keyword1", "keyword2"], TrackingTier.BASIC)
    
    assert len(company.products) == 1
    assert company.product_indexes["product_one"] == 0  # Ensure key is in lowercase and no spaces

def test_add_duplicate_product(company):
    company.add_product("Product One", ["keyword1", "keyword2"], TrackingTier.BASIC)
    
    with pytest.raises(ValueError):
        company.add_product("Product One", ["keyword3"], TrackingTier.PREMIUM)

def test_get_product_by_index(company):
    company.add_product("Product One", ["keyword1"], TrackingTier.BASIC)
    
    product = company.get_product(0)
    assert product.name == "product_one"
    
    with pytest.raises(IndexError):
        company.get_product(1)

def test_generate_usecase_dag_id(company):
    company.add_product("Product One", ["keyword1"], TrackingTier.BASIC)
    
    dag_id = company.generate_usecase_dag_id(0)
    assert dag_id == "company123_product_one"  # Ensure company_id and product_name are in the right format
    
    with pytest.raises(IndexError):
        company.generate_usecase_dag_id(1)

def test_get_product_by_name(company):
    company.add_product("Product One", ["keyword1"], TrackingTier.BASIC)
    
    product = company.get_product_by_name("Product One")
    assert product.name == "product_one"  # Ensure name is in lowercase + space removal
    
    with pytest.raises(KeyError):
        company.get_product_by_name("Nonexistent Product")


# Test for Serialization functions
@patch("builtins.open", new_callable=MagicMock)
@patch("json.load")
def test_deserialize_companies_from_json(mock_json_load, mock_open):
    mock_json_load.return_value = [
        {
            "company_id": "company123",
            "products": [
                {
                    "product_name": "Product One",
                    "product_keywords": ["keyword1", "keyword2"],
                    "tracking_tier": "basic",
                    "usecase_airflow_args": {"start_date": "today"}
                }
            ]
        }
    ]
    
    companies = deserialize_companies_from_json("fake_path")
    assert len(companies) == 1
    assert companies[0].company_id == "company123"  # Company ID should be in lowercase and no spaces
    assert len(companies[0].products) == 1
    assert companies[0].products[0].name == "product_one"  # Ensure product name is in the right format

@patch("builtins.open", new_callable=MagicMock)
@patch("json.dump")
def test_serialize_companies_to_json(mock_json_dump, mock_open):
    company = Company("company123")
    company.add_product("Product One", ["keyword1"], TrackingTier.BASIC)
    
    companies = [company]
    serialize_companies_to_json(companies, "fake_output_path")
    mock_json_dump.assert_called_once()
    assert mock_open.called
