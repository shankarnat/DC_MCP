# Salesforce Data Cloud MCP Server

A simple MCP (Model Context Protocol) server for connecting to Salesforce Data Cloud.

## Setup

1. Create a Python virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your Salesforce credentials
```

4. Run the server:
```bash
python salesforce_mcp_server.py
```

## Available Tools

- **query_data_cloud**: Execute SQL queries using Data Cloud Query API V2
- **get_data_cloud_objects**: List available objects
- **describe_object**: Get object metadata
- **get_data_cloud_metadata**: Get Data Cloud metadata about entities (Calculated Insights, Engagement, Profile, etc.)

## Configuration

Set these environment variables:
- `SALESFORCE_USERNAME`: Your Salesforce username
- `SALESFORCE_PASSWORD`: Your Salesforce password
- `SALESFORCE_SECURITY_TOKEN`: Your security token
- `SALESFORCE_INSTANCE_URL`: Your Salesforce instance URL (optional)
- `SALESFORCE_API_VERSION`: API version (default: v59.0)

## Usage with Claude Desktop

1. Copy the `claude.json` file to your Claude Desktop configuration directory:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
   - Linux: `~/.config/Claude/claude_desktop_config.json`

2. Update the configuration:
   - Replace `/path/to/your/venv/bin/python` with the full path to your virtual environment's Python executable
   - Replace `/path/to/salesforce_mcp_server.py` with the full path to the server script
   - Update all Salesforce credentials with your actual values

3. Restart Claude Desktop

Alternatively, manually add the salesforce server configuration from `claude.json` to your existing `claude_desktop_config.json` file.