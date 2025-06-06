#!/usr/bin/env python3
"""Test Wave/Analytics query endpoint"""

import asyncio
import aiohttp
import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

async def test_wave_query():
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
            
            # Test Wave query endpoint
            wave_query_url = f"{instance_url}/services/data/{api_version}/wave/query"
            
            print(f"Testing Wave query endpoint: {wave_query_url}")
            
            # Test different query formats
            queries = [
                # SAQL (Salesforce Analytics Query Language)
                {"query": "q = load \"Account\"; q = foreach q generate Name; q = limit q 1;"},
                
                # SQL-like query (might work if Data Cloud is enabled)
                {"sql": "SELECT Name FROM Account LIMIT 1"},
                
                # Standard format
                {"q": "q = load \"Account\"; q = foreach q generate Name; q = limit q 1;"},
            ]
            
            for i, query_payload in enumerate(queries):
                print(f"\nTesting query format {i+1}: {query_payload}")
                
                try:
                    async with session.post(wave_query_url, json=query_payload, headers=auth_headers) as resp:
                        print(f"Status: {resp.status}")
                        
                        if resp.status == 200:
                            result = await resp.json()
                            print(f"Success! Response keys: {list(result.keys())}")
                            if "results" in result:
                                print(f"Results: {result['results']}")
                        else:
                            error_text = await resp.text()
                            print(f"Error: {error_text[:300]}...")
                            
                except Exception as e:
                    print(f"Exception: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_wave_query())