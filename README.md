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

### Core Data Cloud Tools
- **query_data_cloud**: Execute SQL queries using Data Cloud Query API V2
- **get_data_cloud_objects**: List available objects
- **describe_object**: Get object metadata
- **get_data_cloud_metadata**: Get Data Cloud metadata about entities (Calculated Insights, Engagement, Profile, etc.)

### Segment-based AI Consumption Tools
- **get_segments**: Fetch Segment definitions from Data Cloud
- **get_segment_members**: Get members of a specific segment
- **enrich_profiles**: Enrich profile data for segment members
- **generate_ai_prompt**: Generate AI prompts based on segment data
- **execute_ai_analysis**: Execute AI analysis (requires OpenAI API key)
- **store_ai_results**: Store AI results back to Data Cloud

### Data Cloud Connect API Tools (v64.0)
- **activate_segment**: Activate segments for real-time actions
- **ingest_data_cloud**: Ingest data into Data Cloud using Connect API
- **get_calculated_insights**: Retrieve calculated insights
- **manage_profiles**: Profile identity resolution and management
- **get_segment_activations**: Get activation status and history
- **real_time_segment_events**: Set up real-time event triggers

### Connect Data API Tools (Official Salesforce)
- **get_connect_segments**: Get segments using official Connect Data API
- **get_connect_segment_details**: Get detailed segment information
- **get_connect_segment_members**: Get segment members with pagination
- **search_connect_segments**: Search segments by name

## Configuration

Set these environment variables:
- `SALESFORCE_USERNAME`: Your Salesforce username
- `SALESFORCE_PASSWORD`: Your Salesforce password
- `SALESFORCE_SECURITY_TOKEN`: Your security token
- `SALESFORCE_INSTANCE_URL`: Your Salesforce instance URL (optional)
- `SALESFORCE_API_VERSION`: API version (default: v59.0)
- `OPENAI_API_KEY`: OpenAI API key (optional, for AI analysis)

## Example Usage

### Segment-based AI Analysis Workflow

1. **Get available segments:**
   ```
   "Show me all active segments in Data Cloud"
   ```

2. **Analyze segment members:**
   ```
   "Get 50 members from the High Value Customers segment and analyze their characteristics"
   ```

3. **Complete workflow:**
   ```
   "Find customers at risk of churning and create personalized retention strategies"
   ```

4. **Real-time activation:**
   ```
   "Activate the high-value customer segment for email marketing campaigns"
   ```

5. **Profile management:**
   ```
   "Resolve identity conflicts and merge duplicate customer profiles"
   ```

The MCP server will automatically:
- Fetch relevant segments
- Get segment members
- Enrich profile data
- Generate AI prompts
- Execute analysis
- Store results back to Data Cloud
- Activate segments for real-time actions
- Set up event monitoring

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