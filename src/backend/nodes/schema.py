"""
Schema analysis and data profiling nodes.
"""

import pandas as pd
from pocketflow import Node


class SchemaInference(Node):
    """Infer schema details from loaded dataframes and expose summaries."""

    def prep(self, shared):
        return shared["dfs"]

    def exec(self, prep_res):
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

    def post(self, shared, prep_res, exec_res):
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
        print(f"Schema inferred:\n{shared['schema_str']}")
        return "default"


class DataProfiler(Node):
    """Analyze data quality, column types, and identify key columns for each table."""

    def prep(self, shared):
        return shared["dfs"]

    def exec(self, prep_res):
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

    def post(self, shared, prep_res, exec_res):
        shared["data_profile"] = exec_res
        shared["profiles"] = exec_res
        tables_with_names = [t for t, p in exec_res.items() if p["name_columns"]]
        print(f"Data profiled: {len(exec_res)} tables, {len(tables_with_names)} with name columns")
        return "default"
