# Model Context Protocol (MCP) Implementation

This repository contains implementations for the Model Context Protocol (MCP), including both client and server components for interacting with external tools and services.

## Repository Structure

### Root Level
- `client.py` - Standalone MCP client implementation
- `README.md` - This file

### MCP Client (`mcp-client/`)
A Python-based MCP client that connects to MCP servers and processes queries using Claude AI.

**Features:**
- Connect to MCP servers (Python or Node.js)
- Interactive chat loop for querying
- Tool execution and result processing
- Integration with Anthropic's Claude API

**Key Components:**
- `client.py` - Main client implementation with MCPClient class
- `pyproject.toml` - Project dependencies and configuration

### MCP Iceberg Server (`mcp-iceberg-server/`)
A specialized MCP server for working with Apache Iceberg tables through AWS Glue.

**Features:**
- Database and table management
- Schema inspection and metadata retrieval
- Data querying with filtering capabilities
- Table creation with custom schemas
- Data insertion and upsert operations
- Snapshot history tracking

**Key Components:**
- `iceberg_mcp_server.py` - Main server implementation using FastMCP
- `pyproject.toml` - Project dependencies and configuration

**Available Tools:**
- `list_databases()` - List available databases
- `list_tables()` - List tables in a database
- `get_table_schema()` - Get table schema details
- `get_table_data()` - Query table data with limits
- `query_table()` - Filter table data by column values
- `get_table_snapshots()` - View table snapshot history
- `create_table()` - Create new tables with custom schemas
- `insert_data()` - Insert or upsert data into tables

## Usage

### MCP Client
```bash
cd mcp-client
python client.py <path_to_server_script>
```

### MCP Iceberg Server
```bash
cd mcp-iceberg-server
python iceberg_mcp_server.py
```

## Configuration

The Iceberg server supports configuration through environment variables:
- `AWS_DEFAULT_REGION` - AWS region (default: us-east-1)
- `ICEBERG_CATALOG` - Catalog name (default: s3tablescatalog)
- `ICEBERG_DATABASE` - Database name (default: pyiceberg)
- `ICEBERG_TABLE_BUCKET` - S3 bucket for tables (default: claude-table-bucket)
- `LF_ROLE_ARN` - Lake Formation role ARN for permissions