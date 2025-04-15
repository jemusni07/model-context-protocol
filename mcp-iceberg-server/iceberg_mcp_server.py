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

# ... rest of the code ...

# Resources
@mcp.resource("iceberg://databases")
def list_databases() -> str:
    """List all available databases in the catalog"""
    catalog = get_catalog()
    databases = catalog.list_namespaces()
    return json.dumps([{"name": db[0]} for db in databases])

@mcp.resource("iceberg://tables/{database}")
def list_tables(database: str) -> str:
    """List all tables in a specific database"""
    catalog = get_catalog()
    tables = catalog.list_tables(database)
    return json.dumps({"database": database, "tables": tables})

@mcp.resource("iceberg://schema/{database}/{table}")
def get_table_schema(database: str, table: str) -> str:
    """Get schema details for a specific table"""
    catalog = get_catalog()
    table_obj = catalog.load_table(f"{database}.{table}")
    schema = table_obj.schema()
    fields = [{"name": field.name, "type": str(field.field_type), "required": field.required} 
              for field in schema.fields]
    return json.dumps({"database": database, "table": table, "schema": fields})

@mcp.resource("iceberg://data/{database}/{table}/{limit}")
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

@mcp.resource("iceberg://snapshots/{database}/{table}")
def get_table_snapshots(database: str, table: str) -> str:
    """Get snapshot history for a specific table"""
    catalog = get_catalog()
    table_obj = catalog.load_table(f"{database}.{table}")
    snapshots = table_obj.snapshots()
    snapshot_info = [{
        "snapshot_id": s.snapshot_id,
        "parent_id": s.parent_id,
        "timestamp": s.timestamp_ms,
        "operation": s.operation
    } for s in snapshots]
    return json.dumps({"database": database, "table": table, "snapshots": snapshot_info})

# Tools
@mcp.tool()
def create_table(database: str, table_name: str, schema_json: str) -> str:
    """
    Create a new Iceberg table with specified schema. if there are errors, show the error verbose form
    
    Args:
        database: Database name
        table_name: Table name to create
        schema_json: JSON string describing the schema (format: [{"name": "col1", "type": "string", "required": true}])
    """
    try:
        schema_list = json.loads(schema_json)
        fields = []
        
        for field in schema_list:
            field_type = None
            if field["type"] == "string":
                field_type = pa.string()
            elif field["type"] == "int" or field["type"] == "integer":
                field_type = pa.int32()
            elif field["type"] == "long":
                field_type = pa.int64()
            elif field["type"] == "float":
                field_type = pa.float32()
            elif field["type"] == "double":
                field_type = pa.float64()
            elif field["type"] == "boolean":
                field_type = pa.bool_()
            
            if field_type:
                fields.append(pa.field(field["name"], field_type, nullable=not field.get("required", False)))
        
        schema = pa.schema(fields)
        
        catalog = get_catalog()
        catalog.create_table(
            identifier=f"{database}.{table_name}",
            schema=schema
        )
        
        return f"Table {database}.{table_name} created successfully"
    except Exception as e:
        return f"Error creating table: {str(e)}"

@mcp.tool()
def insert_data(database: str, table_name: str, data_json: str) -> str:
    """
    Insert data into an Iceberg table
    
    Args:
        database: Database name
        table_name: Table name
        data_json: JSON string with rows to insert ([{"col1": "val1"}, {"col1": "val2"}])
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")
        data = json.loads(data_json)
        
        # Convert to PyArrow table
        df = pd.DataFrame(data)
        arrow_table = pa.Table.from_pandas(df)
        
        # Append data
        table.append(arrow_table)
        
        return f"Inserted {len(data)} rows into {database}.{table_name}"
    except Exception as e:
        return f"Error inserting data: {str(e)}"

@mcp.tool()
def update_data(database: str, table_name: str, data_json: str) -> str:
    """
    Overwrite data in an Iceberg table
    
    Args:
        database: Database name
        table_name: Table name
        data_json: JSON string with rows to update ([{"col1": "val1"}, {"col1": "val2"}])
    """
    try:
        catalog = get_catalog()
        table = catalog.load_table(f"{database}.{table_name}")
        data = json.loads(data_json)
        
        # Convert to PyArrow table
        df = pd.DataFrame(data)
        arrow_table = pa.Table.from_pandas(df)
        
        # Overwrite data
        table.overwrite(arrow_table)
        
        return f"Updated {database}.{table_name} with {len(data)} rows"
    except Exception as e:
        return f"Error updating data: {str(e)}"

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

@mcp.tool()
def time_travel(database: str, table_name: str, snapshot_id: Optional[str] = None, timestamp_ms: Optional[int] = None) -> str:
    """
    Perform time travel on an Iceberg table.
    
    Args:
        database: Database name
        table_name: Table name
        snapshot_id: (Optional) Specific snapshot ID to read
        timestamp_ms: (Optional) Timestamp in milliseconds to read as of
    """
    try:
        catalog = get_catalog()
        table_identifier = f"{database}.{table_name}"
        
        if snapshot_id:
            # Time travel to specific snapshot
            table = catalog.load_table(table_identifier, snapshot_id=int(snapshot_id))
        elif timestamp_ms:
            # Time travel to specific timestamp
            table = catalog.load_table(table_identifier, as_of_time=int(timestamp_ms))
        else:
            return "Either snapshot_id or timestamp_ms must be provided"
            
        # Get data from historical state
        data = table.scan().to_pandas()
        
        return data.to_json(orient="records")
    except Exception as e:
        return f"Error during time travel: {str(e)}"

@mcp.tool()
def list_catalog_databases() -> str:
    """
    List all existing databases in the catalog with detailed information. 
    
    Returns:
        A formatted string listing all databases in the catalog with their details or return verbose error.
    """ 
    try:
        catalog = get_catalog()
        databases = catalog.list_namespaces()
        
        if not databases:
            return "No databases found in the catalog."
        
        # Get more details about the connection
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()["Account"]
        
        result = f"AWS Account: {account_id}\n"
        result += f"Catalog: {CATALOG}\n"
        result += f"Region: {REGION}\n"
        result += f"Role ARN: {LF_ROLE_ARN}\n\n"
        result += "Databases in catalog:\n"
        
        for i, db in enumerate(databases, 1):
            result += f"{i}. {db[0]}\n"
        
        return result
    except Exception as e:
        import traceback
        error_details = f"Error listing databases: {str(e)}\n\n"
        error_details += f"Error type: {type(e).__name__}\n"
        error_details += f"Error details: {str(e)}\n\n"
        error_details += "Traceback:\n"
        error_details += traceback.format_exc()
        error_details += "\n\nEnvironment:\n"
        error_details += f"AWS_DEFAULT_REGION: {REGION}\n"
        error_details += f"CATALOG: {CATALOG}\n"
        error_details += f"DATABASE: {DATABASE}\n"
        error_details += f"TABLE_BUCKET: {TABLE_BUCKET}\n"
        error_details += f"LF_ROLE_ARN: {LF_ROLE_ARN}\n"
        return error_details

# Prompts
@mcp.prompt()
def create_table_prompt() -> str:
    """Create a new Iceberg table"""
    return """
I'd like to create a new Iceberg table. Please use the create_table tool with the following arguments:
- database: The database name
- table_name: A descriptive name for the table
- schema_json: A JSON array describing the columns, e.g.:
[
  {"name": "id", "type": "int", "required": true},
  {"name": "name", "type": "string", "required": true},
  {"name": "age", "type": "int", "required": false},
  {"name": "email", "type": "string", "required": false}
]
"""

@mcp.prompt()
def insert_data_prompt() -> str:
    """Insert data into an Iceberg table"""
    return """
I'd like to insert data into an Iceberg table. Please use the insert_data tool with the following arguments:
- database: The database name
- table_name: The table name
- data_json: A JSON array with the data to insert, e.g.:
[
  {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"}
]
"""

@mcp.prompt()
def time_travel_prompt() -> str:
    """Perform time travel on an Iceberg table"""
    return """
I'd like to travel back in time to view a previous version of an Iceberg table. Please use the time_travel tool with:
- database: The database name
- table_name: The table name

And either:
- snapshot_id: The specific snapshot ID to read
or
- timestamp_ms: A timestamp in milliseconds (epoch time) to read as of that point in time
"""

if __name__ == "__main__":
    mcp.run()