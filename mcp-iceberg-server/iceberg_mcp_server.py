#!/usr/bin/env python3
from mcp.server.fastmcp import FastMCP, Context, Image
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pandas as pd
from pyiceberg.expressions import EqualTo
import boto3
import json
import os
from tabulate import tabulate
from typing import Dict, List, Optional, Any, Tuple
from pyiceberg.io.pyarrow import schema_to_pyarrow
import numpy as np
from datetime import datetime

# Create MCP server
mcp = FastMCP("AWS Glue Iceberg Explorer")

# ... existing code ...

# Global variables with default values
REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
CATALOG = os.environ.get('ICEBERG_CATALOG', 's3tablescatalog')
DATABASE = os.environ.get('ICEBERG_DATABASE', 'pyiceberg')
TABLE_BUCKET = os.environ.get('ICEBERG_TABLE_BUCKET', 'claude-table-bucket')
# Fix the role ARN to use proper format
LF_ROLE_ARN = os.environ.get('LF_ROLE_ARN', 'arn:aws:iam::471112766652:role/pyiceberg-etl-role')

def get_catalog():
    """Connect to the Iceberg catalog via Glue REST endpoint with detailed error handling"""
    try:
        # Get AWS account ID
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()["Account"]
        
        rest_catalog = load_catalog(
            CATALOG,
            **{
                "type": "rest",
                "warehouse": f"{account_id}:s3tablescatalog/{TABLE_BUCKET}",
                "uri": f"https://glue.{REGION}.amazonaws.com/iceberg",
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": REGION,
                # Configure the LF role explicitly if needed
                "lf.role.arn": LF_ROLE_ARN
            }
        )
        return rest_catalog
    except Exception as e:
        detailed_error = f"Catalog connection error: {str(e)}\n"
        detailed_error += "Ensure your AWS credentials have appropriate permissions and Lake Formation is configured correctly."
        raise Exception(detailed_error)



# Metadata Tools

@mcp.tool()
def get_table_snapshots(database: str, table: str) -> str:
    """Get snapshot history for a specific table in a nicely formatted table"""
    try:
        catalog = get_catalog()
        table_obj = catalog.load_table(f"{database}.{table}")
        snapshots = table_obj.inspect.snapshots()
        
        # Convert PyArrow table to pandas DataFrame
        df = snapshots.to_pandas()
        
        if df.empty:
            return f"No snapshots found for {database}.{table}"
        
        # Format the summary column for better readability
        if 'summary' in df.columns:
            df['summary'] = df['summary'].apply(
                lambda x: dict(zip(x['key'], x['value'])) if isinstance(x, dict) and 'key' in x and 'value' in x else x
            )
            
            # Extract key metrics from summary for better visualization
            for metric in ['added-records', 'total-records', 'added-data-files']:
                df[metric] = df['summary'].apply(
                    lambda x: x.get(metric, 'N/A') if isinstance(x, dict) else 'N/A'
                )
        
        # Select and reorder columns for better readability
        display_cols = ['snapshot_id', 'parent_id', 'committed_at', 'operation', 
                         'added-records', 'total-records', 'added-data-files']
        display_df = df[display_cols] if all(col in df.columns for col in display_cols) else df
        
        # Create a header with table information
        header = f"\nSnapshot History for {database}.{table}\n"
        header += "=" * 80 + "\n"
        
        # Format as table using tabulate
        table_formatted = tabulate(display_df, headers='keys', tablefmt='grid', showindex=False)
        
        return header + table_formatted
    except Exception as e:
        return f"Error getting table snapshots: {str(e)}"

# Database and Table Query Tools
    
@mcp.tool()
def list_databases():
    catalog = get_catalog()
    databases = catalog.list_namespaces()
    return json.dumps([{"name": db[0]} for db in databases])

@mcp.tool()
def list_tables(database: str) -> str:
    """List all tables in a specific database"""
    catalog = get_catalog()
    tables = catalog.list_tables(database)
    return json.dumps({"database": database, "tables": tables})

@mcp.tool()
def get_table_data(database: str, table: str, limit: str) -> str:
    """Get sample data from a specific table with limit"""
    try:
        limit_int = int(limit)
        catalog = get_catalog()
        table_obj = catalog.load_table(f"{database}.{table}")
        data = table_obj.scan(limit=limit_int).to_pandas()
        return data.to_json(orient="records")
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
def get_table_schema(database: str, table: str) -> str:
    """Get schema details for a specific table"""
    catalog = get_catalog()
    table_obj = catalog.load_table(f"{database}.{table}")
    schema = table_obj.schema()
    fields = [{"name": field.name, "type": str(field.field_type), "required": field.required} 
              for field in schema.fields]
    return json.dumps({"database": database, "table": table, "schema": fields})


@mcp.tool()
def query_table(database: str, table_name: str, column: str, value: str, limit: int = 10) -> str:
    """
    Query table with a simple equality filter
    
    Args:
        database: Database name
        table_name: Table name
        column: Column to filter on
        value: Value to filter by
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")
        
        # Apply filter
        result = table.scan(
            row_filter=EqualTo(column, value),
            limit=limit
        ).to_pandas()
        
        return result.to_json(orient="records")
    except Exception as e:
        return f"Error querying data: {str(e)}"



# Table Operation Tools


def _create_table_internal(database: str, table_name: str, schema_list: list) -> str:
    """Internal implementation to create a table with the given schema list"""
    # Make sure we have a list
    if not isinstance(schema_list, list):
        return "Error: Schema must be a list of column definitions"
    
    # Import PyArrow
    import pyarrow as pa
    
    # Convert to PyArrow fields
    pa_fields = []
    for field in schema_list:
        field_name = field["name"]
        field_type_str = field["type"].lower()
        nullable = not field.get("required", False)
        
        # Map type strings to PyArrow types
        field_type = None
        if field_type_str == "string":
            field_type = pa.string()
        elif field_type_str in ["int", "integer"]:
            field_type = pa.int32()
        elif field_type_str == "long":
            field_type = pa.int64()
        elif field_type_str == "float":
            field_type = pa.float32()
        elif field_type_str == "double":
            field_type = pa.float64()
        elif field_type_str in ["boolean", "bool"]:
            field_type = pa.bool_()
        elif field_type_str == "timestamp":
            field_type = pa.timestamp('ms')
        elif field_type_str == "date":
            field_type = pa.date32()
        
        if field_type:
            pa_fields.append(pa.field(field_name, field_type, nullable=nullable))
        else:
            return f"Error: Unsupported field type '{field_type_str}' for field '{field_name}'"
    
    # Create PyArrow schema
    pa_schema = pa.schema(pa_fields)
    
    # Get catalog and create table using PyArrow schema
    catalog = get_catalog()
    catalog.create_table(
        identifier=f"{database}.{table_name}",
        schema=pa_schema
    )
    
    # Build response with details
    response = f"Table {database}.{table_name} created successfully\n"
    response += f"Schema: {len(pa_fields)} fields\n"
    for field in pa_fields:
        nullable_str = "nullable" if field.nullable else "required"
        response += f"  - {field.name} ({field.type}, {nullable_str})\n"
        
    return response

def _create_table_with_identifiers(database: str, table_name: str, schema_list: list, identifier_fields: list) -> str:
    """Create a table with identifier fields using PyIceberg Schema"""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType, IntegerType, LongType, FloatType, DoubleType, 
        DateType, TimestampType, StringType, NestedField
    )
    
    # Map type strings to PyIceberg types
    type_mapping = {
        "string": StringType,
        "int": IntegerType,
        "integer": IntegerType,
        "long": LongType,
        "float": FloatType,
        "double": DoubleType,
        "boolean": BooleanType,
        "bool": BooleanType,
        "timestamp": TimestampType,
        "date": DateType,
    }
    
    # Create nested fields for the schema
    nested_fields = []
    field_id = 1  # Start with field_id 1
    field_id_map = {}  # Maps field names to field IDs
    
    for field in schema_list:
        field_name = field["name"]
        field_type_str = field["type"].lower()
        is_required = field.get("required", False)
        
        if field_type_str in type_mapping:
            nested_fields.append(
                NestedField(
                    field_id=field_id,
                    name=field_name,
                    field_type=type_mapping[field_type_str](),
                    required=is_required
                )
            )
            field_id_map[field_name] = field_id
            field_id += 1
        else:
            return f"Error: Unsupported field type '{field_type_str}' for field '{field_name}'"
    
    # Validate that all identifier fields exist in the schema
    identifier_field_ids = []
    for field_name in identifier_fields:
        if field_name in field_id_map:
            identifier_field_ids.append(field_id_map[field_name])
        else:
            return f"Error: Identifier field '{field_name}' not found in schema"
    
    # Create the PyIceberg Schema
    schema = Schema(*nested_fields, identifier_field_ids=identifier_field_ids)
    
    # Get catalog and create table
    catalog = get_catalog()
    catalog.create_table(
        identifier=f"{database}.{table_name}",
        schema=schema
    )
    
    # Build response with details
    response = f"Table {database}.{table_name} created successfully\n"
    response += f"Schema: {len(nested_fields)} fields\n"
    for field in schema_list:
        required = "required" if field.get("required", False) else "optional"
        response += f"  - {field['name']} ({field['type']}, {required})\n"
    
    response += f"\nIdentifier Fields: {', '.join(identifier_fields)}\n"
    
    return response


@mcp.tool()
def create_table(database: str, table_name: str, schema: list, identifier_fields: list = None) -> str:
    """
    Create a new Iceberg table with specified schema and optional identifier fields (primary keys).
    
    Args:
        database: Database name
        table_name: Table name to create
        schema: List of dictionaries describing the schema (e.g. [{"name": "col1", "type": "string", "required": true}])
        identifier_fields: Optional list of field names to use as identifier fields (primary keys)
    """
    try:
        # If identifier fields are provided, use PyIceberg Schema approach
        if identifier_fields and len(identifier_fields) > 0:
            return _create_table_with_identifiers(database, table_name, schema, identifier_fields)
        else:
            # Otherwise use the PyArrow approach
            return _create_table_internal(database, table_name, schema)
    except Exception as e:
        import traceback
        error_msg = f"Error creating table: {str(e)}\n\n"
        error_msg += traceback.format_exc()
        return error_msg



@mcp.tool()
def insert_data(database: str, table_name: str, data: list) -> str:
    """
    Insert data into an Iceberg table, automatically updating existing records and inserting new ones.
    
    Args:
        database: Database name
        table_name: Table name
        data: List of records to insert or update
              (e.g. [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    
    Note:
        This function uses upsert which requires identifier fields (primary keys) to be defined on the table.
        If no identifier fields are defined, all records will be inserted as new rows.
    """
    try:
        # Validate input
        if not isinstance(data, list):
            return "Error: Data must be a list of records"
        
        if len(data) == 0:
            return "Error: Data list is empty, nothing to insert"
        
        # Get the catalog and table
        catalog = get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")
        
        # Get the Iceberg table schema
        iceberg_schema = table.schema()
        
        # Convert Iceberg schema to PyArrow schema
        arrow_schema = schema_to_pyarrow(iceberg_schema)
        
        # Create a dictionary of column types for conversion
        column_types = {field.name: field.type for field in arrow_schema}
        
        # Convert data to a pandas DataFrame first
        df = pd.DataFrame(data)
        
        # Make sure DataFrame columns match the expected schema
        expected_columns = [field.name for field in arrow_schema]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        extra_columns = [col for col in df.columns if col not in expected_columns]
        
        # Handle missing columns
        for col in missing_columns:
            df[col] = None
        
        # Remove extra columns
        if extra_columns:
            df = df.drop(columns=extra_columns)
            
        # Reorder columns to match schema
        df = df[expected_columns]
        
        # Apply type conversions based on schema
        for col in df.columns:
            if col in column_types:
                col_type = column_types[col]
                
                # Handle date conversion
                if pa.types.is_date(col_type):
                    try:
                        # Try to convert string dates to proper date objects
                        df[col] = pd.to_datetime(df[col]).dt.date
                    except Exception as e:
                        return f"Error converting column '{col}' to date: {str(e)}"
                
                # Handle timestamp conversion
                elif pa.types.is_timestamp(col_type):
                    try:
                        # Convert string timestamps to proper timestamp objects
                        df[col] = pd.to_datetime(df[col])
                    except Exception as e:
                        return f"Error converting column '{col}' to timestamp: {str(e)}"
                
                # Handle integer conversion
                elif pa.types.is_integer(col_type):
                    try:
                        # Fill NaN with None before conversion to avoid errors
                        df[col] = df[col].replace({np.nan: None})
                        # Only convert non-null values
                        df.loc[df[col].notna(), col] = df.loc[df[col].notna(), col].astype(int)
                    except Exception as e:
                        return f"Error converting column '{col}' to integer: {str(e)}"
                
                # Handle floating point conversion
                elif pa.types.is_floating(col_type):
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except Exception as e:
                        return f"Error converting column '{col}' to float: {str(e)}"
                
                # Handle boolean conversion
                elif pa.types.is_boolean(col_type):
                    try:
                        # Convert string representations to boolean
                        bool_map = {'true': True, 'false': False, '1': True, '0': False, 
                                   'yes': True, 'no': False, 'y': True, 'n': False}
                        df[col] = df[col].map(lambda x: bool_map.get(str(x).lower(), x) if pd.notna(x) else None)
                    except Exception as e:
                        return f"Error converting column '{col}' to boolean: {str(e)}"
        
        # Convert DataFrame to Arrow table with the correct schema
        try:
            arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        except Exception as e:
            return f"Error converting data to Arrow table: {str(e)}\n\nData types in DataFrame:\n{df.dtypes}"
        
        # Check if the table has identifier fields defined
        identifier_fields = iceberg_schema.identifier_field_ids
        
        # Always use upsert which will handle both operations
        try:
            result = table.upsert(arrow_table)
            
            # Provide feedback
            message = f"Operation completed for {database}.{table_name}:\n"
            message += f"- New records inserted: {result.rows_inserted}\n"
            message += f"- Existing records updated: {result.rows_updated}\n"
            
            if not identifier_fields:
                message += "\nNote: This table has no identifier fields defined, so all records were inserted as new rows.\n"
                message += "To enable updates, define identifier fields when creating the table.\n"
                
            if missing_columns:
                message += f"\nNote: The following columns were missing in your data and set to NULL: {', '.join(missing_columns)}\n"
                
            if extra_columns:
                message += f"\nNote: The following extra columns were ignored: {', '.join(extra_columns)}\n"
                
            return message
        except AttributeError:
            # If upsert is not available, fall back to append
            table.append(arrow_table)
            return f"Successfully appended {len(data)} records to {database}.{table_name} (upsert not available)"
            
    except Exception as e:
        import traceback
        error_details = f"Error processing data: {str(e)}\n\n"
        error_details += traceback.format_exc()
        return error_details



if __name__ == "__main__":
    mcp.run()