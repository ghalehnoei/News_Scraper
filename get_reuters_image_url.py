"""Script to get exact Reuters image URL for manual testing."""

import asyncio
import sys
import aiohttp
import xml.etree.ElementTree as ET

# Set UTF-8 encoding for stdout
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')


async def get_image_url():
    """Get exact Reuters image URL."""
    
    async with aiohttp.ClientSession() as session:
        try:
            # Step 1: Authentication
            print("Step 1: Getting Token...")
            auth_url = "https://commerce.reuters.com/rmd/rest/xml/login"
            auth_params = {
                "username": "IRRINN2Foryou",
                "password": "Webservice2You",
            }
            
            async with session.get(auth_url, params=auth_params) as response:
                xml = await response.text()
                root = ET.fromstring(xml)
                token = root.text.strip()
                print(f"Token: {token}\n")
            
            # Step 2: Get Items List
            print("Step 2: Getting Items List...")
            items_url = "http://rmb.reuters.com/rmd/rest/xml/items"
            items_params = {
                "token": token,
                "channel": "pwu404",
                "limit": "3",
                "mediaType": "P",
            }
            
            # Build full URL for manual testing
            full_items_url = f"{items_url}?token={token}&channel=pwu404&limit=3&mediaType=P"
            print(f"Items List URL:")
            print(f"{full_items_url}\n")
            
            async with session.get(items_url, params=items_params) as response:
                items_xml = await response.text()
                root = ET.fromstring(items_xml)
                
                results = root.findall(".//result")
                print(f"Found {len(results)} items\n")
                
                # Show first 3 items
                for i, result in enumerate(results[:3], 1):
                    item_id = result.find("id").text
                    headline = result.find("headline").text
                    print(f"Item {i}:")
                    print(f"  ID: {item_id}")
                    print(f"  Headline: {headline[:60]}...")
                    print()
                
                # Use first item for detailed test
                first_result = results[0]
                item_id = first_result.find("id").text
                item_guid = first_result.find("guid").text
            
            # Step 3: Get Item Detail
            print("Step 3: Getting Item Detail XML...")
            detail_url = "http://rmb.reuters.com/rmd/rest/xml/item"
            detail_params = {
                "token": token,
                "channel": "pwu404",
                "id": item_id,
            }
            
            full_detail_url = f"{detail_url}?token={token}&channel=pwu404&id={item_id}"
            print(f"Item Detail URL:")
            print(f"{full_detail_url}\n")
            
            async with session.get(detail_url, params=detail_params) as response:
                detail_xml = await response.text()
                root = ET.fromstring(detail_xml)
                
                # Get all remoteContent elements
                remote_contents = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
                
                print(f"Found {len(remote_contents)} remoteContent elements:\n")
                
                # Show all remoteContent with their types
                for i, rc in enumerate(remote_contents):
                    href = rc.get("href", "")
                    content_type = rc.get("contenttype", "")
                    
                    # Extract type from href (THUMBNAIL, PREVIEW, BASEIMAGE, etc.)
                    if "THUMBNAIL" in href:
                        img_type = "THUMBNAIL"
                    elif "PREVIEW" in href:
                        img_type = "PREVIEW"
                    elif "BASEIMAGE" in href:
                        img_type = "BASEIMAGE"
                    elif "VIEWIMAGE" in href:
                        img_type = "VIEWIMAGE"
                    elif "LIMITEDIMAGE" in href:
                        img_type = "LIMITEDIMAGE"
                    else:
                        img_type = "UNKNOWN"
                    
                    alt_id_elem = rc.find("{http://www.reuters.com/ns/2003/08/content}altId")
                    filename = alt_id_elem.text if alt_id_elem is not None else "N/A"
                    
                    print(f"[{i}] {img_type}")
                    print(f"    Filename: {filename}")
                    print(f"    URL: {href}")
                    if i == 2:
                        print(f"    ⭐ THIS IS remoteContent[2] - BASEIMAGE")
                    print()
                
                # Get remoteContent[2] (BASEIMAGE)
                if len(remote_contents) > 2:
                    baseimage_rc = remote_contents[2]
                    image_href = baseimage_rc.get("href", "")
                    
                    alt_id_elem = baseimage_rc.find("{http://www.reuters.com/ns/2003/08/content}altId")
                    filename = alt_id_elem.text if alt_id_elem is not None else "image.jpg"
                    
                    # Create full image URL with token
                    full_image_url = f"{image_href}?token={token}"
                    
                    print("="*80)
                    print("🎯 ALL URLs FOR MANUAL TESTING:")
                    print("="*80)
                    
                    print("\n📌 Step 1: Authentication URL")
                    print("https://commerce.reuters.com/rmd/rest/xml/login?username=IRRINN2Foryou&password=Webservice2You")
                    print(f"Token: {token}\n")
                    
                    print("📌 Step 2: Items List URL")
                    print(full_items_url + "\n")
                    
                    print("📌 Step 3: Item Detail URL")
                    print(full_detail_url + "\n")
                    
                    print("📌 Step 4: Image URL (remoteContent[2] - BASEIMAGE)")
                    print(full_image_url)
                    print(f"Filename: {filename}")
                    print(f"GUID: {item_guid}\n")
                    
                    print("="*80)
                    print("📋 TESTING METHODS:")
                    print("="*80)
                    print("\n1️⃣ Test with curl:")
                    print(f'   curl -L -o test_image.jpg "{full_image_url}"')
                    print("\n2️⃣ Test with browser:")
                    print("   Copy and paste each URL in browser")
                    print("\n3️⃣ Test with Postman/Insomnia:")
                    print("   GET request with 'Follow redirects' enabled")
                    print("\n4️⃣ Test Items List:")
                    print(f'   curl "{full_items_url}"')
                    print("\n5️⃣ Test Item Detail:")
                    print(f'   curl "{full_detail_url}"')
                    print("\n" + "="*80)
                    
                    # Try to download and show result
                    print("\nAttempting download with aiohttp...")
                    async with session.get(full_image_url, allow_redirects=True) as img_resp:
                        print(f"Status Code: {img_resp.status}")
                        print(f"Content-Type: {img_resp.headers.get('Content-Type', 'N/A')}")
                        print(f"Final URL: {str(img_resp.url)[:120]}...")
                        
                        if img_resp.status == 200:
                            data = await img_resp.read()
                            print(f"✅ Success! Downloaded {len(data):,} bytes")
                        else:
                            error = await img_resp.text()
                            print(f"❌ Failed with error:")
                            print(f"{error[:200]}")
                else:
                    print("❌ Not enough remoteContent elements")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    print("\n" + "="*80)
    print("Reuters Image URL Getter - For Manual Testing")
    print("="*80 + "\n")
    asyncio.run(get_image_url())

