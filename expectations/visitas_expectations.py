import great_expectations as ge
import pandas as pd
from great_expectations.core.expectation_suite import ExpectationSuite

def create_visitas_expectations():
    """Create expectation suite for visitas data"""

    # Create expectation suite
    suite = ExpectationSuite(
        expectation_suite_name="visitas_suite",
        expectations=[
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "email", "jk", "badmail", "baja", "fecha_envio",
                        "fecha_open", "opens", "opens_virales", "fecha_click",
                        "clicks", "clicks_virales", "links", "ips",
                        "navegadores", "plataformas"
                    ]
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 1,
                    "max_value": 10000
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "email"
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "email",
                    "regex": r"^[^@]+@[^@]+\.[^@]+$"
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "badmail",
                    "value_set": ["HARD", ""]
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "baja",
                    "value_set": ["SI", ""]
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "opens",
                    "min_value": 0,
                    "max_value": 1000
                },
                "meta": {}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "clicks",
                    "min_value": 0,
                    "max_value": 1000
                },
                "meta": {}
            }
        ]
    )

    return suite

def validate_dataframe(df: pd.DataFrame) -> dict:
    """Validate dataframe against expectations"""
    suite = create_visitas_expectations()

    # Create GE dataset
    ge_df = ge.from_pandas(df)

    # Run validation
    results = ge_df.validate(expectation_suite=suite)

    return {
        "success": results.success,
        "statistics": results.statistics,
        "results": [r.to_json_dict() for r in results.results]
    }