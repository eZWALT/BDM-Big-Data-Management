import os
import re
from typing import Any

# Patterns with the format ${ENV_VAR|DEFAULT} are considered environment variables
environ_regex = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?:\|([^}]+))?\}")

# Patterns with the format {PLACEHOLDER|DEFAULT} are considered placeholders
placeholder_regex = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?:\|([^}]+))?\}")


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

        def _get_from_env(match):
            env_var = match.group(1)
            if env_var not in os.environ:
                default_value = match.group(2)
                if default_value is None:
                    raise ValueError(f"Environment variable {env_var} not found and no default value provided.")
                else:
                    return default_value
            else:
                return os.environ[env_var]

        def _get_from_vars(match):
            key = match.group(1)
            if key not in placeholder_values:
                default_value = match.group(2)
                if default_value is None:
                    raise ValueError(f"Placeholder {key} not found and no default value provided.")
                else:
                    return default_value
            else:
                return placeholder_values[key]

        # First, replace environment variables
        object = environ_regex.sub(_get_from_env, object)
        # Then, replace placeholders
        object = placeholder_regex.sub(_get_from_vars, object)
        return object
    else:
        return object
