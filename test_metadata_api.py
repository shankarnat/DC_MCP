#!/usr/bin/env python3
"""Test Data Cloud metadata API"""

import asyncio
import aiohttp
import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

async def test_metadata_api():
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
            
            # Test metadata API
            metadata_url = f"{instance_url}/api/v1/metadata/"
            
            print(f"Testing metadata API: {metadata_url}")
            
            try:
                async with session.get(metadata_url, headers=auth_headers) as resp:
                    print(f"Status: {resp.status}")
                    
                    if resp.status == 200:
                        result = await resp.json()
                        print(f"Success! Response keys: {list(result.keys())}")
                        print(f"Full response: {result}")
                    else:
                        error_text = await resp.text()
                        print(f"Error: {error_text}")
                        
            except Exception as e:
                print(f"Exception: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_metadata_api())