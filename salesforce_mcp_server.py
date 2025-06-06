#!/usr/bin/env python3
"""
Salesforce Data Cloud MCP Server
A simple MCP server for connecting to Salesforce Data Cloud
"""

import json
import logging
from typing import Any
import asyncio
import os
from dotenv import load_dotenv

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SalesforceDataCloudServer:
    def __init__(self):
        self.server = Server("salesforce-data-cloud")
        self.session_id = None
        self.instance_url = None
        
        # Get configuration from environment
        self.username = os.getenv("SALESFORCE_USERNAME", "")
        self.password = os.getenv("SALESFORCE_PASSWORD", "")
        self.security_token = os.getenv("SALESFORCE_SECURITY_TOKEN", "")
        self.api_version = os.getenv("SALESFORCE_API_VERSION", "v59.0")
        
        # Setup handlers
        self.setup_handlers()
    
    def setup_handlers(self):
        @self.server.list_tools()
        async def list_tools() -> list[types.Tool]:
            return [
                types.Tool(
                    name="query_data_cloud",
                    description="Execute a SQL query against Salesforce Data Cloud using Query API V2",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute (Data Cloud SQL syntax)"
                            }
                        },
                        "required": ["query"]
                    }
                ),
                types.Tool(
                    name="get_data_cloud_objects",
                    description="List available Data Cloud objects",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                types.Tool(
                    name="describe_object",
                    description="Get metadata about a specific Data Cloud object",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "object_name": {
                                "type": "string",
                                "description": "Name of the Data Cloud object"
                            }
                        },
                        "required": ["object_name"]
                    }
                ),
                types.Tool(
                    name="get_data_cloud_metadata",
                    description="Get Data Cloud metadata about entities, including Calculated Insights, Engagement, Profile, and other entities",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "entityType": {
                                "type": "string",
                                "description": "The requested metadata entity type",
                                "enum": ["DataLakeObject", "DataModelObject", "CalculatedInsight"]
                            },
                            "entityCategory": {
                                "type": "string", 
                                "description": "The requested metadata entity category (not applicable for CalculatedInsight)",
                                "enum": ["Profile", "Engagement", "Related"]
                            },
                            "entityName": {
                                "type": "string",
                                "description": "The name of the requested metadata entity (e.g., UnifiedIndividual__dlm)"
                            }
                        }
                    }
                )
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Any) -> list[types.TextContent]:
            try:
                # Ensure we're authenticated
                if not self.session_id:
                    await self._authenticate()
                
                if name == "query_data_cloud":
                    result = await self._query_data_cloud(arguments["query"])
                elif name == "get_data_cloud_objects":
                    result = await self._get_data_cloud_objects()
                elif name == "describe_object":
                    result = await self._describe_object(arguments["object_name"])
                elif name == "get_data_cloud_metadata":
                    result = await self._get_data_cloud_metadata(arguments)
                else:
                    raise ValueError(f"Unknown tool: {name}")
                
                return [types.TextContent(
                    type="text",
                    text=json.dumps(result, indent=2)
                )]
            
            except Exception as e:
                logger.error(f"Error calling tool {name}: {str(e)}")
                return [types.TextContent(
                    type="text",
                    text=f"Error: {str(e)}"
                )]
        
        @self.server.list_resources()
        async def list_resources() -> list[types.Resource]:
            return [
                types.Resource(
                    uri="salesforce://config",
                    name="Salesforce Configuration",
                    description="Current Salesforce connection configuration",
                    mimeType="application/json"
                ),
                types.Resource(
                    uri="salesforce://objects",
                    name="Data Cloud Objects",
                    description="List of available Data Cloud objects",
                    mimeType="application/json"
                )
            ]
        
        @self.server.read_resource()
        async def read_resource(uri: str) -> str:
            if uri == "salesforce://config":
                return json.dumps({
                    "username": self.username,
                    "api_version": self.api_version,
                    "connected": bool(self.session_id)
                }, indent=2)
            elif uri == "salesforce://objects":
                if not self.session_id:
                    await self._authenticate()
                objects = await self._get_data_cloud_objects()
                return json.dumps(objects, indent=2)
            else:
                raise ValueError(f"Unknown resource: {uri}")
    
    async def _authenticate(self):
        """Authenticate with Salesforce using username/password flow"""
        import aiohttp
        import xml.etree.ElementTree as ET
        
        login_url = "https://login.salesforce.com/services/Soap/u/" + self.api_version
        
        # Build SOAP envelope for login
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">
            <soapenv:Body>
                <urn:login>
                    <urn:username>{self.username}</urn:username>
                    <urn:password>{self.password}{self.security_token}</urn:password>
                </urn:login>
            </soapenv:Body>
        </soapenv:Envelope>"""
        
        headers = {
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": "login"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(login_url, data=soap_body, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Login failed with status {response.status}")
                
                response_text = await response.text()
                root = ET.fromstring(response_text)
                
                # Extract session ID and instance URL
                ns = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/", 
                      "urn": "urn:partner.soap.sforce.com"}
                
                session_id_elem = root.find(".//urn:sessionId", ns)
                server_url_elem = root.find(".//urn:serverUrl", ns)
                
                if session_id_elem is None or server_url_elem is None:
                    raise Exception("Failed to extract session ID or server URL from login response")
                
                self.session_id = session_id_elem.text
                # Extract instance URL from server URL
                server_url = server_url_elem.text
                self.instance_url = server_url.split('/services/')[0]
                
                logger.info(f"Successfully authenticated to Salesforce at {self.instance_url}")
    
    async def _query_data_cloud(self, query: str) -> dict[str, Any]:
        """Execute a SQL query against Data Cloud using Query API V2"""
        import aiohttp
        
        # Use Data Cloud Query API V2 endpoint
        url = f"{self.instance_url}/api/v2/query"
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        # Prepare the query payload
        payload = {
            "sql": query
        }
        
        all_records = []
        batch_id = None
        
        async with aiohttp.ClientSession() as session:
            # Make the initial POST request
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Query failed: {error_text}")
                
                result = await response.json()
                
                # Extract data and batchId if present
                if "data" in result:
                    all_records.extend(result["data"])
                
                batch_id = result.get("nextBatchId")
                
                # If there are more batches, fetch them using GET requests
                while batch_id:
                    batch_url = f"{url}?batchId={batch_id}"
                    async with session.get(batch_url, headers=headers) as batch_response:
                        if batch_response.status != 200:
                            error_text = await batch_response.text()
                            raise Exception(f"Batch query failed: {error_text}")
                        
                        batch_result = await batch_response.json()
                        
                        if "data" in batch_result:
                            all_records.extend(batch_result["data"])
                        
                        batch_id = batch_result.get("nextBatchId")
                
                # Return in a format similar to SOQL query response
                return {
                    "records": all_records,
                    "done": True,
                    "totalSize": len(all_records)
                }
    
    async def _get_data_cloud_objects(self) -> list[dict[str, Any]]:
        """Get list of available Data Cloud objects"""
        import aiohttp
        
        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects"
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Failed to get objects: {error_text}")
                
                data = await response.json()
                # Filter for Data Cloud objects (you might want to customize this)
                return [obj for obj in data["sobjects"] if obj["queryable"]]
    
    async def _describe_object(self, object_name: str) -> dict[str, Any]:
        """Get metadata about a specific object"""
        import aiohttp
        
        url = f"{self.instance_url}/services/data/{self.api_version}/sobjects/{object_name}/describe"
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Failed to describe object: {error_text}")
                
                return await response.json()
    
    async def _get_data_cloud_metadata(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Get Data Cloud metadata using the v1 metadata API"""
        import aiohttp
        from urllib.parse import urlencode
        
        # Build query parameters
        params = {}
        if "entityType" in arguments:
            params["entityType"] = arguments["entityType"]
        if "entityCategory" in arguments:
            params["entityCategory"] = arguments["entityCategory"]
        if "entityName" in arguments:
            params["entityName"] = arguments["entityName"]
        
        # Construct URL
        url = f"{self.instance_url}/api/v1/metadata/"
        if params:
            url += "?" + urlencode(params)
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Metadata API failed: {error_text}")
                
                return await response.json()
    
    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            initialization_options = self.server.create_initialization_options()
            await self.server.run(read_stream, write_stream, initialization_options)

async def main():
    """Main entry point"""
    # Check for required credentials
    if not all([os.getenv("SALESFORCE_USERNAME"), 
                os.getenv("SALESFORCE_PASSWORD"), 
                os.getenv("SALESFORCE_SECURITY_TOKEN")]):
        logger.error("Missing required Salesforce credentials")
        logger.error("Please set: SALESFORCE_USERNAME, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN")
        return
    
    server = SalesforceDataCloudServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())