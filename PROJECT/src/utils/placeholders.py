from typing import Any


def replace_placeholders(object: Any, **placeholder_values: Any) -> Any:
    """
    Replace placeholders recursively in the given object with values from placeholder_values.
    The placeholders are in the format {key} and are replaced with the corresponding
    value from placeholder_values.
    """
    if isinstance(object, dict):
        return {k: replace_placeholders(v, **placeholder_values) for k, v in object.items()}
    elif isinstance(object, list):
        return list(replace_placeholders(v, **placeholder_values) for v in object)
    elif isinstance(object, str):
        for key, value in placeholder_values.items():
            object = object.replace(f"{{{key}}}", str(value))
        return object
    else:
        return object
