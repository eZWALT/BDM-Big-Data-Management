from dataclasses import dataclass
from functools import cache
from typing import Dict, List

import yaml


@dataclass
class Tier:
    """Class representing a tier in the system."""

    airflow_dag_args: Dict
    social_networks: List[str]


@cache
def get_tier_definitions() -> Dict[str, Tier]:
    """Load the tier definitions"""
    with open("configuration/tiers.yaml", "r") as file:
        tier_definitions = yaml.safe_load(file)
    return {tier_name: Tier(**tier_def) for tier_name, tier_def in tier_definitions.items()}


@cache
def get_tier_definition(tier_name: str) -> Tier:
    """Get the tier definition for a specific tier"""
    tier_definitions = get_tier_definitions()
    if tier_name in tier_definitions:
        return tier_definitions[tier_name]
    else:
        raise ValueError(f"Tier '{tier_name}' not found in definitions.")
