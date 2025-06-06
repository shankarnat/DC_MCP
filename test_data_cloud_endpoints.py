#!/usr/bin/env python3
"""Test various Data Cloud endpoints"""

import asyncio
import aiohttp
import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

async def test_data_cloud_endpoints():
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
            
            # Test various Data Cloud endpoints
            endpoints = [
                # Data Cloud Query API variations
                "/api/v1/query",
                "/api/v2/query", 
                "/services/data/v1/query",
                "/services/data/v2/query",
                f"/services/data/{api_version}/datacloud/query",
                
                # Data Cloud specific endpoints
                "/services/data/v1/datacloud",
                "/services/data/v2/datacloud", 
                f"/services/data/{api_version}/datacloud",
                f"/services/data/{api_version}/wave",
                f"/services/data/{api_version}/analytics",
                
                # Check what's available under /api/
                "/api/",
                "/api/v1/",
                "/api/v2/",
            ]
            
            test_query = {"sql": "SELECT Id, Name FROM Account LIMIT 1"}
            simple_soql = {"q": "SELECT Id, Name FROM Account LIMIT 1"}
            
            for endpoint in endpoints:
                url = instance_url + endpoint
                print(f"Testing {endpoint}...")
                
                # Try GET first
                try:
                    async with session.get(url, headers=auth_headers) as resp:
                        print(f"  GET Status: {resp.status}")
                        if resp.status == 200:
                            if 'application/json' in resp.content_type:
                                data = await resp.json()
                                print(f"  GET Success! Keys: {list(data.keys()) if isinstance(data, dict) else 'List response'}")
                            else:
                                text = await resp.text()
                                print(f"  GET Success! Text response (first 100 chars): {text[:100]}...")
                        elif resp.status == 405:
                            print("  GET Method not allowed - trying POST")
                        elif resp.status == 403:
                            print("  GET Forbidden - may need different permissions")
                        elif resp.status != 404:
                            print(f"  GET Unexpected status: {resp.status}")
                except Exception as e:
                    print(f"  GET Error: {str(e)}")
                
                # Try POST with SQL query for query endpoints
                if "query" in endpoint:
                    try:
                        async with session.post(url, json=test_query, headers=auth_headers) as resp:
                            print(f"  POST (SQL) Status: {resp.status}")
                            if resp.status == 200:
                                data = await resp.json()
                                print(f"  POST SQL Success! Keys: {list(data.keys())}")
                            elif resp.status != 404 and resp.status != 405:
                                error_text = await resp.text()
                                print(f"  POST SQL Error: {error_text[:200]}...")
                    except Exception as e:
                        print(f"  POST SQL Error: {str(e)}")
                    
                    # Try POST with SOQL query format
                    try:
                        async with session.post(url, json=simple_soql, headers=auth_headers) as resp:
                            print(f"  POST (SOQL) Status: {resp.status}")
                            if resp.status == 200:
                                data = await resp.json()
                                print(f"  POST SOQL Success! Keys: {list(data.keys())}")
                    except Exception as e:
                        print(f"  POST SOQL Error: {str(e)}")
                
                print()

if __name__ == "__main__":
    asyncio.run(test_data_cloud_endpoints())