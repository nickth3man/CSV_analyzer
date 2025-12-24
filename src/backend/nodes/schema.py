"""Schema analysis and data profiling nodes."""

import logging

import pandas as pd
from pocketflow import Node


logger = logging.getLogger(__name__)


class SchemaInference(Node):
    """Infer schema details from loaded DataFrames and expose summaries."""

    def prep(self, shared):
        """
        Provide the pipeline's loaded DataFrame mapping from the shared context.

        Returns:
            dict: Mapping from table name (str) to pandas.DataFrame stored under shared["dfs"].
        """
        return shared["dfs"]

    def exec(self, prep_res):
        """
        Infer column lists and split schemas by observed source.

        Parameters:
            prep_res (dict): Mapping of table name to pandas DataFrame.

        Returns:
            tuple: A 3-tuple (schemas, csv_schema, api_schema) where
                - schemas (dict): table name -> list of column names for every DataFrame.
                - csv_schema (dict): table name -> list of column names for tables labeled with `_source == "csv"`.
                - api_schema (dict): table name -> list of column names for tables labeled with `_source == "api"`.
        """
        dfs = prep_res
        schemas = {}
        csv_schema = {}
        api_schema = {}

        for name, df in dfs.items():
            schemas[name] = list(df.columns)
        for name, df in dfs.items():
            source = None
            if name in dfs and "_source" in df.columns:
                source = df["_source"].iloc[0] if not df.empty else None
            if source == "api" and name not in csv_schema:
                api_schema[name] = list(df.columns)
            elif source == "csv" and name not in api_schema:
                csv_schema[name] = list(df.columns)
        return schemas, csv_schema, api_schema

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store inferred schema information in the shared state and produce human-readable schema strings.

        This writes the following keys into the shared dictionary:
        - "schemas": mapping of table name to list of column names.
        - "csv_schema_str": formatted multi-line string of CSV-only table schemas.
        - "api_schema_str": formatted multi-line string of API-only table schemas.
        - "schema_str": formatted multi-line string listing each table with its source (from shared["data_sources"] or "MERGED") and columns.

        Also prints a brief schema summary to stdout.

        Returns:
            result (str): The string "default".
        """
        schemas, csv_schema, api_schema = exec_res
        shared["schemas"] = schemas
        shared["csv_schema_str"] = "\n".join(
            [f"Table '{name}' [CSV]: [{', '.join(cols)}]" for name, cols in csv_schema.items()]
        )
        shared["api_schema_str"] = "\n".join(
            [f"Table '{name}' [API]: [{', '.join(cols)}]" for name, cols in api_schema.items()]
        )
        schema_lines = []
        for name, cols in schemas.items():
            source = shared.get("data_sources", {}).get(name, "merged")
            schema_lines.append(f"Table '{name}' [{source.upper()}]: [{', '.join(cols)}]")
        shared["schema_str"] = "\n".join(schema_lines)
        logger.info(f"Schema inferred:\n{shared['schema_str']}")
        return "default"


class DataProfiler(Node):
    """Analyze data quality, column types, and identify key columns for each table."""

    def prep(self, shared):
        """
        Provide the pipeline's loaded DataFrame mapping from the shared context.

        Returns:
            dict: Mapping from table name (str) to pandas.DataFrame stored under shared["dfs"].
        """
        return shared["dfs"]

    def exec(self, prep_res):
        """
        Builds a profiling summary for each DataFrame in the provided mapping.

        Parameters:
            prep_res (dict): Mapping from table name (str) to pandas DataFrame to be profiled.

        Returns:
            dict: A mapping from table name to a profile dictionary with the following keys:
                - row_count (int): Number of rows in the table.
                - column_count (int): Number of columns in the table.
                - columns (dict): Per-column metadata mapping column name to a dict with:
                    - dtype (str): Column dtype as a string.
                    - null_count (int): Number of null values in the column.
                    - unique_count (int): Number of unique values in the column.
                - name_columns / name_cols (list[str]): Columns whose names suggest person/name fields.
                - id_columns / id_cols (list[str]): Columns whose names contain "id".
                - numeric_columns / numeric_cols (list[str]): Columns with numeric dtype.
                - date_columns / date_cols (list[str]): Columns whose names suggest date or year fields.
        """
        dfs = prep_res
        profile = {}
        for table_name, df in dfs.items():
            table_profile = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": {},
                "name_columns": [],
                "name_cols": [],
                "id_columns": [],
                "id_cols": [],
                "numeric_columns": [],
                "numeric_cols": [],
                "date_columns": [],
                "date_cols": [],
            }

            for col in df.columns:
                col_lower = col.lower()
                col_info = {
                    "dtype": str(df[col].dtype),
                    "null_count": int(df[col].isnull().sum()),
                    "unique_count": int(df[col].nunique()),
                }

                if "name" in col_lower or "first" in col_lower or "last" in col_lower:
                    table_profile["name_columns"].append(col)
                    table_profile["name_cols"].append(col)
                if "id" in col_lower:
                    table_profile["id_columns"].append(col)
                    table_profile["id_cols"].append(col)
                if pd.api.types.is_numeric_dtype(df[col]):
                    table_profile["numeric_columns"].append(col)
                    table_profile["numeric_cols"].append(col)
                if "date" in col_lower or "year" in col_lower:
                    table_profile["date_columns"].append(col)
                    table_profile["date_cols"].append(col)

                table_profile["columns"][col] = col_info

            profile[table_name] = table_profile

        return profile

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the profiling results in the shared state and print a brief summary.

        Stores the provided profiling dictionary into shared["data_profile"] and shared["profiles"], counts how many tables include detected name columns, and prints a one-line summary of total profiled tables and how many contain name columns.

        Returns:
            str: The string "default".
        """
        shared["data_profile"] = exec_res
        shared["profiles"] = exec_res
        tables_with_names = [t for t, p in exec_res.items() if p["name_columns"]]
        logger.info(f"Data profiled: {len(exec_res)} tables, {len(tables_with_names)} with name columns")
        return "default"
