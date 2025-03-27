import json
from typing import List
from datetime import datetime, timedelta
import logging
from src.utils.company import deserialize_companies_from_json
from src.orchestration.dag_factory import (
    create_batch_product_tracking_dag,
    create_dummy_test_dag,
)

# ===----------------------------------------------------------------------===#
# Automatic USE-CASE DAG Generation                                           #
#                                                                             #
# This script automatically parses the list of companies from a JSON file     #
# and navigates through all these companies products (use-cases) in order to  #
# automatically generate all the batch jobs with their specified configuration#            
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


# Function that automatically generates DAGs for all products across companies
# in a serialized JSON. For batch DAG generation, streaming flag is False.
def generate_dynamic_dags_from_serialized_companies(
    data_path: str, is_test: bool = True
):
    # Deserialize the list of companies from the JSON data
    companies = deserialize_companies_from_json(data_path)

    for company in companies:
        for i, product in enumerate(company.products):
            dag_id = company.generate_usecase_dag_id(
                i
            )  # Create a unique DAG ID for each product of a company

            if is_test:
                dag = create_dummy_test_dag(company, i)
            else:
                create_batch_product_tracking_dag(company, i)

            # Assign the generated DAG to a global variable
            globals()[dag_id] = dag
            logging.info(
                f"DAG created for {company.company_id} - {product.name} with dag_id: {dag_id}"
            )


generate_dynamic_dags_from_serialized_companies(
    "src/orchestration/companies.json", is_test=True
)
