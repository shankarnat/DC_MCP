#!/usr/bin/env python3
"""Check available Salesforce APIs"""

import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from test_auth import test_auth
import xml.etree.ElementTree as ET

load_dotenv()

async def check_apis():
    # First authenticate
    username = os.getenv("SALESFORCE_USERNAME", "shankarnatraj630@agentforce.com")
    password = os.getenv("SALESFORCE_PASSWORD", "Qwer1234#")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN", "iCIlUkxn2YkWu1AM7EWcpvbCZ")
    api_version = os.getenv("SALESFORCE_API_VERSION", "v59.0")
    
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
    
    async with aiohttp.ClientSession() as session:
        async with session.post(login_url, data=soap_body, headers=headers) as response:
            response_text = await response.text()
            root = ET.fromstring(response_text)
            ns = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/", 
                  "urn": "urn:partner.soap.sforce.com"}
            
            session_id = root.find(".//urn:sessionId", ns).text
            server_url = root.find(".//urn:serverUrl", ns).text
            instance_url = server_url.split('/services/')[0]
            
            auth_headers = {
                "Authorization": f"Bearer {session_id}",
                "Content-Type": "application/json"
            }
            
            print(f"Instance URL: {instance_url}\n")
            
            # Check various endpoints
            endpoints = [
                f"/services/data/{api_version}/",
                f"/services/data/{api_version}/sobjects",
                f"/services/data/{api_version}/query?q=SELECT+Id+FROM+Account+LIMIT+1",
                "/api/v2/query",
                "/services/data/v2/query",
                f"/services/data/{api_version}/wave/query",
                "/api/v1/query",
                "/services/data/v1/analytics/reports",
            ]
            
            for endpoint in endpoints:
                url = instance_url + endpoint
                print(f"Checking {endpoint}...")
                
                try:
                    if "query" in endpoint and "/api/" in endpoint:
                        # Try POST for query endpoints
                        payload = {"sql": "SELECT Id FROM Account LIMIT 1"}
                        async with session.post(url, json=payload, headers=auth_headers) as resp:
                            print(f"  POST Status: {resp.status}")
                            if resp.status == 200:
                                data = await resp.json()
                                print(f"  Success! Response keys: {list(data.keys())}")
                    else:
                        # Try GET for other endpoints
                        async with session.get(url, headers=auth_headers) as resp:
                            print(f"  GET Status: {resp.status}")
                            if resp.status == 200:
                                if resp.content_type == 'application/json':
                                    data = await resp.json()
                                    if isinstance(data, dict):
                                        print(f"  Success! Response keys: {list(data.keys())[:5]}...")
                                    elif isinstance(data, list):
                                        print(f"  Success! Got list with {len(data)} items")
                except Exception as e:
                    print(f"  Error: {str(e)}")
                
                print()

if __name__ == "__main__":
    asyncio.run(check_apis())