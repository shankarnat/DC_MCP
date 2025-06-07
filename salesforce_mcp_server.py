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
from datetime import datetime
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
                ),
                types.Tool(
                    name="get_segments",
                    description="Fetch all Segment definitions from Data Cloud",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_type": {
                                "type": "string",
                                "description": "Type of segments to fetch",
                                "enum": ["all", "active", "archived"]
                            }
                        }
                    }
                ),
                types.Tool(
                    name="get_segment_members",
                    description="Get members of a specific segment",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Number of members to fetch",
                                "default": 100
                            }
                        },
                        "required": ["segment_id"]
                    }
                ),
                types.Tool(
                    name="enrich_profiles",
                    description="Enrich profiles for segment members",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "profile_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of profile IDs to enrich"
                            },
                            "fields": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Fields to include in enrichment"
                            }
                        },
                        "required": ["profile_ids"]
                    }
                ),
                types.Tool(
                    name="generate_ai_prompt",
                    description="Generate AI prompt based on segment data",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_data": {
                                "type": "object",
                                "description": "Segment member data"
                            },
                            "prompt_template": {
                                "type": "string",
                                "description": "Template for AI prompt"
                            }
                        },
                        "required": ["segment_data"]
                    }
                ),
                types.Tool(
                    name="execute_ai_analysis",
                    description="Execute AI analysis on segment data",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "prompt": {
                                "type": "string",
                                "description": "AI prompt"
                            },
                            "model": {
                                "type": "string",
                                "description": "AI model to use",
                                "default": "gpt-4"
                            }
                        },
                        "required": ["prompt"]
                    }
                ),
                types.Tool(
                    name="store_ai_results",
                    description="Store AI analysis results back to Data Cloud",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "results": {
                                "type": "object",
                                "description": "AI analysis results"
                            },
                            "target_object": {
                                "type": "string",
                                "description": "Target Data Cloud object"
                            }
                        },
                        "required": ["results", "target_object"]
                    }
                ),
                types.Tool(
                    name="activate_segment",
                    description="Activate a segment for real-time actions using Data Cloud Connect API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment to activate"
                            },
                            "activation_target": {
                                "type": "string",
                                "description": "Target for activation (e.g., 'email', 'advertising', 'personalization')"
                            },
                            "activation_config": {
                                "type": "object",
                                "description": "Configuration for the activation"
                            }
                        },
                        "required": ["segment_id", "activation_target"]
                    }
                ),
                types.Tool(
                    name="ingest_data_cloud",
                    description="Ingest data into Data Cloud using Connect API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "object_name": {
                                "type": "string",
                                "description": "Name of the Data Cloud object"
                            },
                            "data": {
                                "type": "array",
                                "items": {"type": "object"},
                                "description": "Array of records to ingest"
                            },
                            "operation": {
                                "type": "string",
                                "description": "Ingestion operation type",
                                "enum": ["insert", "upsert", "update"],
                                "default": "upsert"
                            }
                        },
                        "required": ["object_name", "data"]
                    }
                ),
                types.Tool(
                    name="get_calculated_insights",
                    description="Retrieve calculated insights from Data Cloud",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "insight_name": {
                                "type": "string",
                                "description": "Name of the calculated insight"
                            },
                            "filter_criteria": {
                                "type": "object",
                                "description": "Filter criteria for the insights"
                            }
                        }
                    }
                ),
                types.Tool(
                    name="manage_profiles",
                    description="Perform profile identity resolution and management",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "operation": {
                                "type": "string",
                                "description": "Profile operation type",
                                "enum": ["resolve", "merge", "split", "get_identity_graph"]
                            },
                            "profile_data": {
                                "type": "object",
                                "description": "Profile data for the operation"
                            }
                        },
                        "required": ["operation"]
                    }
                ),
                types.Tool(
                    name="get_segment_activations",
                    description="Get activation status and history for segments",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment"
                            },
                            "activation_type": {
                                "type": "string",
                                "description": "Type of activation to check"
                            }
                        }
                    }
                ),
                types.Tool(
                    name="real_time_segment_events",
                    description="Set up real-time event triggers for segment changes",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment to monitor"
                            },
                            "event_types": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Types of events to monitor (e.g., 'member_added', 'member_removed')"
                            },
                            "webhook_url": {
                                "type": "string",
                                "description": "Webhook URL for event notifications"
                            }
                        },
                        "required": ["segment_id", "event_types"]
                    }
                ),
                types.Tool(
                    name="get_connect_segments",
                    description="Get segments using official Connect Data API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_name": {
                                "type": "string",
                                "description": "Optional filter by segment name"
                            },
                            "status": {
                                "type": "string",
                                "description": "Filter by status (Active, Inactive, etc.)"
                            }
                        }
                    }
                ),
                types.Tool(
                    name="get_connect_segment_details",
                    description="Get detailed information about a specific segment using Connect Data API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment"
                            }
                        },
                        "required": ["segment_id"]
                    }
                ),
                types.Tool(
                    name="get_connect_segment_members",
                    description="Get segment members using Connect Data API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "segment_id": {
                                "type": "string",
                                "description": "ID of the segment"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Number of members to return",
                                "default": 100
                            },
                            "offset": {
                                "type": "integer",
                                "description": "Offset for pagination",
                                "default": 0
                            }
                        },
                        "required": ["segment_id"]
                    }
                ),
                types.Tool(
                    name="search_connect_segments",
                    description="Search segments by name using Connect Data API",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "search_term": {
                                "type": "string",
                                "description": "Search term to find segments"
                            },
                            "exact_match": {
                                "type": "boolean",
                                "description": "Whether to perform exact match",
                                "default": False
                            }
                        },
                        "required": ["search_term"]
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
                elif name == "get_segments":
                    result = await self._get_segments(arguments.get("segment_type", "all"))
                elif name == "get_segment_members":
                    result = await self._get_segment_members(
                        arguments["segment_id"], 
                        arguments.get("limit", 100)
                    )
                elif name == "enrich_profiles":
                    result = await self._enrich_profiles(
                        arguments["profile_ids"],
                        arguments.get("fields", None)
                    )
                elif name == "generate_ai_prompt":
                    prompt = await self._generate_ai_prompt(
                        arguments["segment_data"],
                        arguments.get("prompt_template", None)
                    )
                    result = {"prompt": prompt}
                elif name == "execute_ai_analysis":
                    result = await self._execute_ai_analysis(
                        arguments["prompt"],
                        arguments.get("model", "gpt-4")
                    )
                elif name == "store_ai_results":
                    result = await self._store_ai_results(
                        arguments["results"],
                        arguments["target_object"]
                    )
                elif name == "activate_segment":
                    result = await self._activate_segment(
                        arguments["segment_id"],
                        arguments["activation_target"],
                        arguments.get("activation_config", {})
                    )
                elif name == "ingest_data_cloud":
                    result = await self._ingest_data_cloud(
                        arguments["object_name"],
                        arguments["data"],
                        arguments.get("operation", "upsert")
                    )
                elif name == "get_calculated_insights":
                    result = await self._get_calculated_insights(
                        arguments.get("insight_name"),
                        arguments.get("filter_criteria", {})
                    )
                elif name == "manage_profiles":
                    result = await self._manage_profiles(
                        arguments["operation"],
                        arguments.get("profile_data", {})
                    )
                elif name == "get_segment_activations":
                    result = await self._get_segment_activations(
                        arguments.get("segment_id"),
                        arguments.get("activation_type")
                    )
                elif name == "real_time_segment_events":
                    result = await self._real_time_segment_events(
                        arguments["segment_id"],
                        arguments["event_types"],
                        arguments.get("webhook_url")
                    )
                elif name == "get_connect_segments":
                    result = await self._get_connect_segments(
                        arguments.get("segment_name"),
                        arguments.get("status")
                    )
                elif name == "get_connect_segment_details":
                    result = await self._get_connect_segment_details(
                        arguments["segment_id"]
                    )
                elif name == "get_connect_segment_members":
                    result = await self._get_connect_segment_members(
                        arguments["segment_id"],
                        arguments.get("limit", 100),
                        arguments.get("offset", 0)
                    )
                elif name == "search_connect_segments":
                    result = await self._search_connect_segments(
                        arguments["search_term"],
                        arguments.get("exact_match", False)
                    )
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
        from urllib.parse import quote
        
        # Try Data Cloud Query API V2 first
        dc_url = f"{self.instance_url}/api/v2/query"
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        # Prepare the query payload for Data Cloud API V2
        payload = {
            "sql": query
        }
        
        all_records = []
        
        async with aiohttp.ClientSession() as session:
            try:
                # Make the initial POST request to Data Cloud API V2
                async with session.post(dc_url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # Extract data from initial response
                        if "data" in result:
                            all_records.extend(result["data"])
                        
                        # Handle pagination with batchId
                        batch_id = result.get("nextBatchId")
                        while batch_id:
                            batch_url = f"{dc_url}?batchId={batch_id}"
                            async with session.get(batch_url, headers=headers) as batch_response:
                                if batch_response.status != 200:
                                    break
                                
                                batch_result = await batch_response.json()
                                
                                if "data" in batch_result:
                                    all_records.extend(batch_result["data"])
                                
                                batch_id = batch_result.get("nextBatchId")
                        
                        # Return Data Cloud format
                        return {
                            "data": all_records,
                            "metadata": result.get("metadata", {}),
                            "done": True,
                            "totalSize": len(all_records)
                        }
                    
                    elif response.status == 404:
                        # Data Cloud API not available, fall back to SOQL
                        logger.info("Data Cloud API V2 not available, falling back to SOQL")
                        pass  # Will try SOQL below
                    else:
                        error_text = await response.text()
                        raise Exception(f"Data Cloud Query failed: {error_text}")
                        
            except Exception as e:
                if "404" in str(e) or "URL No Longer Exists" in str(e):
                    logger.info("Data Cloud API V2 not available, falling back to SOQL")
                else:
                    raise e
            
            # Fallback to standard SOQL if Data Cloud API is not available
            # Convert SQL-like syntax to SOQL
            soql_query = self._convert_sql_to_soql(query)
            
            soql_url = f"{self.instance_url}/services/data/{self.api_version}/query?q={quote(soql_query)}"
            
            # Make SOQL request
            next_url = soql_url
            while next_url:
                if next_url.startswith('/'):
                    next_url = self.instance_url + next_url
                    
                async with session.get(next_url, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"SOQL Query failed: {error_text}")
                    
                    result = await response.json()
                    
                    if "records" in result:
                        all_records.extend(result["records"])
                    
                    if result.get("done", True):
                        next_url = None
                    else:
                        next_url = result.get("nextRecordsUrl")
            
            # Return in SOQL format
            return {
                "records": all_records,
                "done": True,
                "totalSize": len(all_records)
            }
    
    def _convert_sql_to_soql(self, query: str) -> str:
        """Convert SQL-like syntax to SOQL for fallback"""
        # Handle SELECT TOP syntax
        if query.upper().startswith("SELECT TOP"):
            query = query.replace("SELECT TOP ", "SELECT ", 1)
            parts = query.split(" FROM ")
            if len(parts) == 2:
                select_part = parts[0]
                from_part = parts[1]
                select_words = select_part.split()
                if len(select_words) > 2:
                    limit_num = select_words[2]
                    remaining_select = " ".join(select_words[3:])
                    query = f"SELECT {remaining_select} FROM {from_part} LIMIT {limit_num}"
        
        # Note: This is a basic conversion. Data Lake Objects (__dlm) won't work with SOQL
        # but this provides a fallback for standard objects
        return query
    
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
    
    async def _get_segments(self, segment_type: str = "all") -> dict[str, Any]:
        """Fetch Segment definitions from Data Cloud"""
        query = "SELECT Id, Name, Description, Status, MemberCount FROM Segment__dlm"
        if segment_type == "active":
            query += " WHERE Status = 'Active'"
        elif segment_type == "archived":
            query += " WHERE Status = 'Archived'"
        
        return await self._query_data_cloud(query)

    async def _get_segment_members(self, segment_id: str, limit: int = 100) -> dict[str, Any]:
        """Get members of a specific segment"""
        query = f"""
        SELECT ProfileId__c, UnifiedIndividualId__c, SegmentId__c, MembershipStatus__c
        FROM SegmentMembership__dlm  
        WHERE SegmentId__c = '{segment_id}'
        AND MembershipStatus__c = 'Active'
        LIMIT {limit}
        """
        return await self._query_data_cloud(query)

    async def _enrich_profiles(self, profile_ids: list[str], fields: list[str] = None) -> dict[str, Any]:
        """Enrich profiles with additional data"""
        if not fields:
            fields = ["Id", "FirstName__c", "LastName__c", "Email__c", "Phone__c", 
                     "TotalPurchases__c", "LifetimeValue__c", "LastActivityDate__c"]
        
        fields_str = ", ".join(fields)
        ids_str = "', '".join(profile_ids)
        
        query = f"""
        SELECT {fields_str}
        FROM UnifiedIndividual__dlm
        WHERE Id IN ('{ids_str}')
        """
        return await self._query_data_cloud(query)

    async def _generate_ai_prompt(self, segment_data: dict, prompt_template: str = None) -> str:
        """Generate AI prompt based on segment data"""
        # Extract relevant data
        records = segment_data.get("records", []) if "records" in segment_data else segment_data.get("data", [])
        member_count = len(records)
        
        # Build context
        context = {
            "member_count": member_count,
            "timestamp": datetime.now().isoformat(),
            "profiles": json.dumps(records[:10], indent=2)  # Limit to first 10 for prompt
        }
        
        # Use template or default
        if not prompt_template:
            prompt_template = """
        Analyze the following segment of {member_count} customers (showing first 10):
        
        Timestamp: {timestamp}
        
        Customer Profiles:
        {profiles}
        
        Please provide:
        1. Key characteristics and patterns
        2. Behavioral insights
        3. Recommended marketing strategies
        4. Potential risks or opportunities
        """
        
        return prompt_template.format(**context)

    async def _execute_ai_analysis(self, prompt: str, model: str = "gpt-4") -> dict[str, Any]:
        """Execute AI analysis using external AI service"""
        import aiohttp
        
        # Check for API key
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            return {
                "error": "OpenAI API key not configured",
                "message": "Please set OPENAI_API_KEY environment variable",
                "analysis": "AI analysis unavailable - API key missing"
            }
        
        ai_endpoint = "https://api.openai.com/v1/chat/completions"
        ai_headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a data analyst specializing in customer segmentation and marketing insights."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 1000
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(ai_endpoint, json=payload, headers=ai_headers) as response:
                    if response.status != 200:
                        error_data = await response.json()
                        return {
                            "error": f"AI API failed with status {response.status}",
                            "details": error_data,
                            "analysis": "Analysis failed"
                        }
                    
                    result = await response.json()
                    return {
                        "analysis": result["choices"][0]["message"]["content"],
                        "model": model,
                        "timestamp": datetime.now().isoformat(),
                        "usage": result.get("usage", {})
                    }
        except Exception as e:
            return {
                "error": str(e),
                "analysis": "Analysis failed due to error"
            }

    async def _store_ai_results(self, results: dict, target_object: str) -> dict[str, Any]:
        """Store AI analysis results back to Data Cloud"""
        # Prepare data for storage
        storage_data = {
            "analysis": results.get("analysis", ""),
            "timestamp": datetime.now().isoformat(),
            "model": results.get("model", "unknown"),
            "segment_id": results.get("segment_id", ""),
            "metadata": json.dumps(results)
        }
        
        # In a real implementation, this would use Data Cloud Ingestion API
        # For now, we'll simulate the storage
        logger.info(f"Storing AI results to {target_object}")
        logger.info(f"Data: {storage_data}")
        
        return {
            "status": "success",
            "message": f"Results would be stored to {target_object}",
            "object": target_object,
            "data_preview": storage_data,
            "note": "Actual Data Cloud ingestion not implemented - would require Ingestion API setup"
        }
    
    def _get_data_cloud_connect_base_url(self) -> str:
        """Get the Data Cloud Connect API base URL"""
        # Extract the Data Cloud instance URL from the main instance URL
        # Format: https://{dne_cdpInstanceUrl}/services/data/v64.0
        # For now, we'll use the same instance URL with the Connect API path
        return f"{self.instance_url}/services/data/v64.0"
    
    async def _activate_segment(self, segment_id: str, activation_target: str, activation_config: dict = None) -> dict[str, Any]:
        """Activate a segment for real-time actions using Data Cloud Connect API"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/segments/{segment_id}/activations"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "activationTarget": activation_target,
            "config": activation_config or {},
            "status": "active"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 201 or response.status == 200:
                    return await response.json()
                else:
                    # Fallback response for when Connect API is not available
                    return {
                        "status": "simulated",
                        "message": f"Segment activation simulated for {segment_id}",
                        "activation_target": activation_target,
                        "config": activation_config,
                        "note": "Data Cloud Connect API not available - activation simulated"
                    }
    
    async def _ingest_data_cloud(self, object_name: str, data: list, operation: str = "upsert") -> dict[str, Any]:
        """Ingest data into Data Cloud using Connect API"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/ingest/{object_name}"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "operation": operation,
            "records": data
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 201 or response.status == 200:
                    return await response.json()
                else:
                    # Fallback for when Connect API is not available
                    return {
                        "status": "simulated",
                        "message": f"Data ingestion simulated for {object_name}",
                        "operation": operation,
                        "records_processed": len(data),
                        "records_preview": data[:3] if data else [],
                        "note": "Data Cloud Connect API not available - ingestion simulated"
                    }
    
    async def _get_calculated_insights(self, insight_name: str = None, filter_criteria: dict = None) -> dict[str, Any]:
        """Retrieve calculated insights from Data Cloud"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/insights"
        
        if insight_name:
            url += f"/{insight_name}"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        params = {}
        if filter_criteria:
            params.update(filter_criteria)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback using Query API for calculated insights
                    query = "SELECT Id, Name, Value, CalculationDate FROM CalculatedInsight__dlm"
                    if insight_name:
                        query += f" WHERE Name = '{insight_name}'"
                    query += " LIMIT 100"
                    
                    try:
                        result = await self._query_data_cloud(query)
                        return {
                            "insights": result.get("records", []),
                            "source": "Query API fallback",
                            "note": "Data Cloud Connect API not available - using Query API"
                        }
                    except:
                        return {
                            "status": "simulated",
                            "message": f"Calculated insights simulated for {insight_name or 'all insights'}",
                            "note": "Data Cloud Connect API not available - insights simulated"
                        }
    
    async def _manage_profiles(self, operation: str, profile_data: dict = None) -> dict[str, Any]:
        """Perform profile identity resolution and management"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/profiles/{operation}"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        payload = profile_data or {}
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback for profile operations
                    if operation == "get_identity_graph":
                        try:
                            query = """
                            SELECT Id, UnifiedRecordId__c, SourceRecordId__c, SourceSystem__c
                            FROM UnifiedIndividual__dlm 
                            LIMIT 10
                            """
                            result = await self._query_data_cloud(query)
                            return {
                                "identity_graph": result.get("records", []),
                                "operation": operation,
                                "source": "Query API fallback"
                            }
                        except:
                            pass
                    
                    return {
                        "status": "simulated",
                        "operation": operation,
                        "message": f"Profile {operation} operation simulated",
                        "profile_data": profile_data,
                        "note": "Data Cloud Connect API not available - operation simulated"
                    }
    
    async def _get_segment_activations(self, segment_id: str = None, activation_type: str = None) -> dict[str, Any]:
        """Get activation status and history for segments"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/segments"
        
        if segment_id:
            url += f"/{segment_id}/activations"
        else:
            url += "/activations"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        params = {}
        if activation_type:
            params["type"] = activation_type
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return {
                        "status": "simulated",
                        "segment_id": segment_id,
                        "activation_type": activation_type,
                        "activations": [
                            {
                                "id": "activation_001",
                                "type": activation_type or "email",
                                "status": "active",
                                "created_date": datetime.now().isoformat(),
                                "target_count": 1250
                            }
                        ],
                        "note": "Data Cloud Connect API not available - activations simulated"
                    }
    
    async def _real_time_segment_events(self, segment_id: str, event_types: list, webhook_url: str = None) -> dict[str, Any]:
        """Set up real-time event triggers for segment changes"""
        import aiohttp
        
        base_url = self._get_data_cloud_connect_base_url()
        url = f"{base_url}/segments/{segment_id}/events/subscribe"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "eventTypes": event_types,
            "webhookUrl": webhook_url,
            "enabled": True
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 201 or response.status == 200:
                    return await response.json()
                else:
                    return {
                        "status": "simulated",
                        "segment_id": segment_id,
                        "event_types": event_types,
                        "webhook_url": webhook_url,
                        "subscription_id": f"sub_{segment_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                        "message": "Real-time event subscription simulated",
                        "note": "Data Cloud Connect API not available - event subscription simulated"
                    }
    
    def _get_connect_data_base_url(self) -> str:
        """Get the Connect Data API base URL"""
        return f"{self.instance_url}/services/data/{self.api_version}/connect/data"
    
    async def _get_connect_segments(self, segment_name: str = None, status: str = None) -> dict[str, Any]:
        """Get segments using official Connect Data API"""
        import aiohttp
        from urllib.parse import urlencode
        
        url = self._get_connect_data_base_url() + "/segments"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        # Build query parameters
        params = {}
        if segment_name:
            params["name"] = segment_name
        if status:
            params["status"] = status
        
        if params:
            url += "?" + urlencode(params)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback to Query API
                    logger.info(f"Connect Data API not available (status {response.status}), falling back to Query API")
                    try:
                        query = "SELECT Id, Name, Description, Status, MemberCount FROM Segment__dlm"
                        conditions = []
                        
                        if segment_name:
                            conditions.append(f"Name LIKE '%{segment_name}%'")
                        if status:
                            conditions.append(f"Status = '{status}'")
                        
                        if conditions:
                            query += " WHERE " + " AND ".join(conditions)
                        
                        query += " LIMIT 100"
                        
                        result = await self._query_data_cloud(query)
                        return {
                            "segments": result.get("records", []) if "records" in result else result.get("data", []),
                            "source": "Query API fallback",
                            "note": "Connect Data API not available - using Query API"
                        }
                    except Exception as e:
                        return {
                            "error": f"Both Connect Data API and Query API failed: {str(e)}",
                            "segments": [],
                            "note": "All segment APIs unavailable"
                        }
    
    async def _get_connect_segment_details(self, segment_id: str) -> dict[str, Any]:
        """Get detailed information about a specific segment using Connect Data API"""
        import aiohttp
        
        url = f"{self._get_connect_data_base_url()}/segments/{segment_id}"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback to Query API
                    try:
                        query = f"""
                        SELECT Id, Name, Description, Status, MemberCount, 
                               CreatedDate, LastModifiedDate, SegmentType
                        FROM Segment__dlm 
                        WHERE Id = '{segment_id}'
                        """
                        result = await self._query_data_cloud(query)
                        records = result.get("records", []) if "records" in result else result.get("data", [])
                        
                        if records:
                            return {
                                "segment": records[0],
                                "source": "Query API fallback",
                                "note": "Connect Data API not available - using Query API"
                            }
                        else:
                            return {
                                "error": f"Segment {segment_id} not found",
                                "segment": None
                            }
                    except Exception as e:
                        return {
                            "error": f"Failed to get segment details: {str(e)}",
                            "segment": None
                        }
    
    async def _get_connect_segment_members(self, segment_id: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """Get segment members using Connect Data API"""
        import aiohttp
        from urllib.parse import urlencode
        
        url = f"{self._get_connect_data_base_url()}/segments/{segment_id}/members"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        params = {
            "limit": limit,
            "offset": offset
        }
        url += "?" + urlencode(params)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback to Query API
                    try:
                        query = f"""
                        SELECT ProfileId__c, UnifiedIndividualId__c, SegmentId__c, 
                               MembershipStatus__c, JoinDate__c
                        FROM SegmentMembership__dlm  
                        WHERE SegmentId__c = '{segment_id}'
                        AND MembershipStatus__c = 'Active'
                        LIMIT {limit}
                        """
                        if offset > 0:
                            query += f" OFFSET {offset}"
                        
                        result = await self._query_data_cloud(query)
                        return {
                            "members": result.get("records", []) if "records" in result else result.get("data", []),
                            "totalCount": len(result.get("records", []) if "records" in result else result.get("data", [])),
                            "limit": limit,
                            "offset": offset,
                            "source": "Query API fallback",
                            "note": "Connect Data API not available - using Query API"
                        }
                    except Exception as e:
                        return {
                            "error": f"Failed to get segment members: {str(e)}",
                            "members": []
                        }
    
    async def _search_connect_segments(self, search_term: str, exact_match: bool = False) -> dict[str, Any]:
        """Search segments by name using Connect Data API"""
        import aiohttp
        from urllib.parse import urlencode
        
        url = f"{self._get_connect_data_base_url()}/segments/search"
        
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }
        
        params = {
            "q": search_term,
            "exact": str(exact_match).lower()
        }
        url += "?" + urlencode(params)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    # Fallback to Query API with LIKE search
                    try:
                        if exact_match:
                            query = f"SELECT Id, Name, Description, Status, MemberCount FROM Segment__dlm WHERE Name = '{search_term}'"
                        else:
                            query = f"SELECT Id, Name, Description, Status, MemberCount FROM Segment__dlm WHERE Name LIKE '%{search_term}%'"
                        
                        query += " LIMIT 50"
                        
                        result = await self._query_data_cloud(query)
                        return {
                            "segments": result.get("records", []) if "records" in result else result.get("data", []),
                            "searchTerm": search_term,
                            "exactMatch": exact_match,
                            "source": "Query API fallback",
                            "note": "Connect Data API not available - using Query API"
                        }
                    except Exception as e:
                        return {
                            "error": f"Segment search failed: {str(e)}",
                            "segments": [],
                            "searchTerm": search_term
                        }
    
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