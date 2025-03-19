import json
from typing import List
from datetime import datetime, timedelta
from loguru import logger
from src.utils.company import Company, TrackingTier, deserialize_companies_from_json
from dag_factory import (
    create_batch_product_tracking_dag,
    create_stream_product_tracking_dag,
    create_dummy_test_dag,
)

# ===----------------------------------------------------------------------===#
# Automatic USE-CASE DAG Generation                                           #
#                                                                             #
# This script automatically parses the list of companies from a JSON file     #
# and navigates through all these companies products (use-cases) in order to  #
# automatically generate all the
# Author: Walter J.T.V                                                        #
# ===----------------------------------------------------------------------===#


def generate_dynamic_dags_from_serialized_companies(
    data_path: str, streaming: bool = False, is_test: bool = True
):
    """
    Function that automatically generates DAGs for all products across companies
    in a serialized JSON. For batch DAG generation, streaming flag is False.
    """
    # Deserialize the list of companies from the JSON data
    companies = deserialize_companies_from_json(data_path)

    for company in companies:
        for i, product in enumerate(company.products):
            dag_id = company.get_dag_id(
                i
            )  # Create a unique DAG ID for each product of a company

            if is_test:
                dag = create_dummy_test_dag(company, i)
            else:
                # If streaming is True, use the stream DAG creation function
                if streaming:
                    dag = create_stream_product_tracking_dag(company, i)
                else:
                    dag = create_batch_product_tracking_dag(company, i)

            # Assign the generated DAG to a global variable
            globals()[dag_id] = dag
            logger.success(
                f"DAG created for {company.company_id} - {product.product_name} with dag_id: {dag_id}"
            )


if __name__ == "__main__":
    generate_dynamic_dags_from_serialized_companies(
        "./companies.json", streaming=False, is_test=True
    )
