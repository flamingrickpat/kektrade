from typing import Dict, Any

from jsonschema import Draft7Validator, validators

from kektrade.constants import CONFIG_SCHEMA


def extend_with_default(validator_class):
    """
    Set unset values in dict with default value form property
    :param validator_class:
    :return:
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

    return validators.extend(
        validator_class, {"properties" : set_defaults},
    )


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate the config with the scheme from scheme.py.
    Unset values are set with the default value from the scheme.
    :param config: Config dict
    :return: Validated config dict
    """
    DefaultValidatingDraft7Validator = extend_with_default(Draft7Validator)
    DefaultValidatingDraft7Validator(CONFIG_SCHEMA).validate(config)
    return config