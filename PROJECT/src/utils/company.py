import json
import pprint
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List

from loguru import logger

# ===----------------------------------------------------------------------===#
# Company Related Information                                                 #
#                                                                             #
# These classes provide a structured and extensible way of capturing the      #
# details of a specific client company that wants to make use of VibeRadar to #
# track products. Despite its implementation, only one product will be tracked#
# per company reduce complexity. It also generates Airflow-like arguments for #
# ease of orchestration and DAG creation                                      #
#                                                                             #
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


# Utility function to convert text to underscore nomenclature
def convert_to_underscore_nomenclature(text: str) -> str:
    """Convert a string to lowercase and replace spaces with underscores."""
    return re.sub(r"\s+", "_", text.strip()).lower()


class Product:
    def __init__(self, product_name: str, product_keywords: List[str], tracking_tier: str):
        # The lowercase name uniquely identifies a product
        self.name = convert_to_underscore_nomenclature(product_name)
        # each product has a set of associated keywords
        self.keywords = [keyword.lower() for keyword in product_keywords]
        # each product has a certain degree of tracking (tier)
        self.tracking_tier = tracking_tier

    def to_dict(self):
        return {"product_name": self.name, "product_keywords": self.keywords, "tracking_tier": self.tracking_tier.value}


class Company:
    def __init__(self, company_id: str):
        # Ensure company_id is also lowercase and no spaces
        self.company_id = convert_to_underscore_nomenclature(company_id)
        self.products: List[Product] = []
        self.product_indexes = {}

    def add_product(self, prod_name: str, prod_keywords: List[str], track_tier: str):
        # Convert product name to lowercase and replace spaces with underscores
        prod_name = convert_to_underscore_nomenclature(prod_name)

        # Check for already existing product
        if prod_name in self.product_indexes:
            raise ValueError(f"Product '{prod_name}' already exists for company {self.company_id}.")
        product = Product(prod_name, prod_keywords, track_tier)
        self.products.append(product)
        self.product_indexes[prod_name] = len(self.products) - 1

    def get_product(self, prod_idx: int) -> Product:
        if 0 <= prod_idx < len(self.products):
            return self.products[prod_idx]
        raise IndexError(f"Product index {prod_idx} is out of range ({len(self.products) -1})")

    # Can return null value if no product has the given arg name
    def get_product_by_name(self, prod_name: str) -> Product:
        prod_name = convert_to_underscore_nomenclature(prod_name)
        index = self.product_indexes.get(prod_name, None)
        if index is None:
            raise KeyError(f"Product '{prod_name}' not found.")
        return self.products[index]

    def get_product_index_by_name(self, prod_name: str) -> int:
        prod_name = convert_to_underscore_nomenclature(prod_name)
        return self.product_indexes.get(prod_name, None)

    ################## AIRFLOW HELPER FUNCTIONS ##################

    def generate_usecase_dag_id(self, prod_idx: int) -> str:
        if 0 <= prod_idx < len(self.products):
            product = self.products[prod_idx]
            return f"{self.company_id}_{product.name}"
        raise IndexError(f"Product index {prod_idx} is out of range ({len(self.products) -1})")

    # Used for serialization purposes
    def to_dict(self):
        return {
            "company_id": self.company_id,
            "products": [product.to_dict() for product in self.products],
        }


# Static method for parsing serialized companies and products
def deserialize_companies_from_json(json_path: str) -> List[Company]:
    try:
        # Load JSON data
        with open(json_path, "r") as file:
            companies_data = json.load(file)

        if not isinstance(companies_data, list):
            raise ValueError("JSON root must be a list of companies.")

        companies = []
        for company_data in companies_data:
            # Validate required fields
            if "company_id" not in company_data or "products" not in company_data:
                raise ValueError(f"Missing required fields in company data: {company_data}")

            company = Company(company_data["company_id"])

            if not isinstance(company_data["products"], list):
                raise ValueError(f"Expected 'products' to be a list in company {company.company_id}")

            for product in company_data["products"]:
                # Validate product fields
                if "product_name" not in product or "product_keywords" not in product or "tracking_tier" not in product:
                    raise ValueError(f"Missing required fields in product data: {product}")

                try:
                    tracking_tier = product["tracking_tier"]
                except ValueError:
                    raise ValueError(
                        f"Invalid tracking tier '{product['tracking_tier']}' for product '{product['product_name']}'"
                    )

                # Add product to the company
                company.add_product(
                    prod_name=product["product_name"],
                    prod_keywords=product["product_keywords"],
                    track_tier=tracking_tier,
                )

            companies.append(company)

        return companies

    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in file.")
    except Exception as e:
        raise RuntimeError(f"Error parsing JSON: {e}")


# Function to serialize companies to JSON file
def serialize_companies_to_json(companies: List[Company], json_path: str):
    try:
        with open(json_path, "w") as file:
            json.dump([company.to_dict() for company in companies], file, indent=4)
        logger.success(f"Companies successfully saved to {json_path}")
    except Exception as e:
        logger.error(f"Error saving to JSON file: {e}")


# Just a simple main to showcase functionalities
if __name__ == "__main__":
    # Parse companies from the input JSON
    companies = deserialize_companies_from_json("management/companies.json")
    for company in companies:
        pprint.pprint(company.to_dict(), indent=2)
    # Dump companies back into a new JSON file
    serialize_companies_to_json(companies, "management/test_companies.json")
