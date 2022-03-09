def get_json_schema():
    return {
        "$schema": "http://json-schema.org/draft-04/schema",
        "type": "object",
        "properties": {
            "tsm_schema": {
            "type": "object",
            "properties": {
                "root": {
                "type": "string",
                "minLength": 1,
                "pattern": "^[A-Za-z_-]*$"
                },
                "types": {
                "type": "object",
                "minProperties": 1,
                "propertyNames": {
                    "pattern": "^[A-Za-z_-]*$"
                },
                "additionalProperties": {
                    "type": "object",
                    "required": ["fields"],
                    "properties": {
                    "fields": {
                        "type": "object",
                        "minProperties": 1,
                        "propertyNames": {
                        "pattern": "^[A-Za-z_-]*$"
                        },
                        "additionalProperties": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "validators": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "oneOf": [
                                {
                                    "type": "string",
                                    "enum": [
                                    "tsm_name_validator",
                                    "tsm_to_lowercase",
                                    "tsm_to_uppercase",
                                    "tsm_string_only",
                                    "tsm_isodate",
                                    "tsm_to_string",
                                    "tsm_get_nested"
                                    ]
                                },
                                {
                                    "type": "array",
                                    "minItems": 2,
                                    "items": [
                                    {
                                        "type": "string",
                                        "enum": ["tsm_get_nested"]
                                    }
                                    ],
                                    "additionalItems": { "$ref": "#/$defs/anytype" }
                                }
                                ]
                            }
                            },
                            "map": {
                            "type": "string"
                            },
                            "default": { "$ref": "#/$defs/anytype" },
                            "default_from": {
                            "type": "string"
                            },
                            "replace_from": {
                            "type": "string"
                            },
                            "value": { "$ref": "#/$defs/anytype" },
                            "multiple": {
                            "type": "boolean"
                            },
                            "remove": {
                            "type": "boolean"
                            },
                            "type": {
                            "type": "string"
                            }
                        }
                        }
                    }
                    }
                }
                }
            },
            "required": ["root", "types"]
            }
        },
        "$defs": {
            "anytype": {
            "type": ["number", "string", "boolean", "object", "array", "null"]
            }
        }
    }
