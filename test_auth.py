#!/usr/bin/env python3
"""Test Salesforce authentication"""

import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_auth():
    username = os.getenv("SALESFORCE_USERNAME", "pradeep.krishna@pskdemo.com")
    password = os.getenv("SALESFORCE_PASSWORD", "*55$YJ$6x4BC2&Jv")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN", "bxcOKgqw6QOH9NxY39GoW881")
    api_version = os.getenv("SALESFORCE_API_VERSION", "v59.0")
    
    print(f"Testing authentication for: {username}")
    print(f"API Version: {api_version}")
    
    # Try login.salesforce.com first
    login_url = f"https://login.salesforce.com/services/Soap/u/{api_version}"
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">
        <soapenv:Body>
            <urn:login>
                <urn:username>{username}</urn:username>
                <urn:password>{password}{security_token}</urn:password>
            </urn:login>
        </soapenv:Body>
    </soapenv:Envelope>"""
    
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": "login"
    }
    
    print(f"\nTrying login URL: {login_url}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(login_url, data=soap_body, headers=headers) as response:
                print(f"Response status: {response.status}")
                response_text = await response.text()
                
                if response.status != 200:
                    print(f"Login failed with status {response.status}")
                    print(f"Response: {response_text}")
                    
                    # Try test.salesforce.com for sandbox
                    print("\nTrying sandbox login...")
                    login_url = f"https://test.salesforce.com/services/Soap/u/{api_version}"
                    
                    async with session.post(login_url, data=soap_body, headers=headers) as response2:
                        print(f"Sandbox response status: {response2.status}")
                        response_text = await response2.text()
                        
                        if response2.status != 200:
                            print(f"Sandbox login also failed")
                            print(f"Response: {response_text}")
                            return
                
                # Parse response
                root = ET.fromstring(response_text)
                ns = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/", 
                      "urn": "urn:partner.soap.sforce.com"}
                
                session_id_elem = root.find(".//urn:sessionId", ns)
                server_url_elem = root.find(".//urn:serverUrl", ns)
                
                if session_id_elem is not None and server_url_elem is not None:
                    session_id = session_id_elem.text
                    server_url = server_url_elem.text
                    instance_url = server_url.split('/services/')[0]
                    
                    print(f"\nAuthentication successful!")
                    print(f"Instance URL: {instance_url}")
                    print(f"Session ID: {session_id[:20]}...")
                    
                    # Test Query API V2
                    print("\nTesting Query API V2...")
                    query_url = f"{instance_url}/api/v2/query"
                    query_headers = {
                        "Authorization": f"Bearer {session_id}",
                        "Content-Type": "application/json"
                    }
                    
                    # Simple test query
                    test_query = {"sql": "SELECT Id, Name FROM Account LIMIT 1"}
                    
                    async with session.post(query_url, json=test_query, headers=query_headers) as query_response:
                        print(f"Query API V2 response status: {query_response.status}")
                        if query_response.status != 200:
                            error_text = await query_response.text()
                            print(f"Query error: {error_text}")
                        else:
                            result = await query_response.json()
                            print(f"Query successful! Result: {result}")
                else:
                    print("Failed to extract session ID or server URL")
                    print(f"Response: {response_text}")
                    
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_auth())