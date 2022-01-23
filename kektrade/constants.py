CONFIG_SCHEMA = \
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://kektrade.com/kektrade.schema.json",
        "title": "kektrade",
        "description": "kektrade metastrategy configuration file",
        "type": "object",
        "required": ["metastrategy_id", "subaccounts"],
        "properties": {
            "metastrategy_id": {
                "type": "string",
                "description": "identifier for metastrategy"
            },
            "exchange_default_parameters": {
                "type": "object",
                "description": "default parameters for the exchanges, overridden by values in subaccount exchange",
                "default": {}
            },
            "user_data_dir": {
                "type": "string",
                "description": "path to folder where history and ticker data is saved",
                "default": "user_data"
            },
            "strategy_data_dir": {
                "type": "string",
                "description": "path to folder where custom strategies are saved",
                "default": "strategies"
            },
            "data_data_dir": {
                "type": "string",
                "description": "path to folder where candles are cached",
                "default": "data"
            },
            "log_level": {
                "type": "string",
                "description": "minimum importance to show log line",
                "default": "info",
                "enum": ["debug", "info", "warning", "error", "critical"]
            },
            "log_console": {
                "type": "boolean",
                "description": "write log to stdout",
                "default": True
            },

            "subaccounts": {
                "type": "array",
                "items": {"$ref": "#/$defs/subaccount"}
            }
        },


        "$defs": {
            "pair_data_info": {
                "type": "object",
                "required": ["endpoint", "pair", "timeframe"],
                "properties": {
                    "endpoint": {
                        "type": "string",
                        "description": "exchange endpoint from which to load ohlc data"
                    },
                    "pair": {
                        "type": "string",
                        "description": "pair to trade on this subaccount"
                    },
                    "timeframe": {
                        "type": "integer",
                        "description": "timeframe for ticker data in minutes"
                    }
                }
            },

            "exchange": {
                "type": "object",
                "required": ["endpoint"],
                "properties": {
                    "type": {
                        "endpoint": "string",
                        "description": "exchange endpoint on which the trade operations are executed"
                    }
                }
            },

            "subaccount": {
                "type": "object",
                "required": ["subaccount_id", "strategy", "exchange"],
                "properties": {
                    "subaccount_id": {
                        "type": "string",
                        "description": "unique identifier for updates, logging and plotting"
                    },
                    "strategy": {
                        "type": "string",
                        "description": "class name of strategy in strategy_data_dir folder"
                    },
                    "strategy_params": {
                        "type": "object",
                        "description": "override parameters in custom strategy"
                    },
                    "exchange": {
                        "$ref": "#/$defs/exchange"
                    },
                    "main_pair": {
                        "$ref": "#/$defs/pair_data_info"
                    },
                    "aux_pairs": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/pair_data_info"}
                    }
                }
            }


        }
    }
