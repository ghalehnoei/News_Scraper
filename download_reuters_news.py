#!/usr/bin/env python3
"""
دانلود اخبار رویترز و ذخیره به صورت XML در لوکال

این اسکریپت با استفاده از API رویترز، اخبار را دانلود کرده و به صورت فایل‌های XML در پوشه مشخص شده ذخیره می‌کند.
نیازی به اتصال به دیتابیس ندارد.

Usage:
    python download_reuters_news.py --username USERNAME --password PASSWORD [OPTIONS]

Options:
    --username USERNAME        نام کاربری رویترز
    --password PASSWORD        رمز عبور رویترز
    --output-dir DIR           مسیر پوشه خروجی (پیش‌فرض: ./reuters_news)
    --content-type TYPE        نوع محتوا: Video, Text, Photo (پیش‌فرض: Video)
    --limit N                  تعداد آیتم‌ها برای هر کانال (پیش‌فرض: 10)
    --channels CHANNELS        لیست کانال‌های خاص (جدا شده با کاما)
    --s3-endpoint URL          آدرس S3 endpoint
    --s3-bucket BUCKET         نام S3 bucket
    --s3-access-key KEY        کلید دسترسی S3
    --s3-secret-key KEY        کلید مخفی S3
    --s3-region REGION         منطقه S3 (پیش‌فرض: us-east-1)
    --upload-to-s3             آپلود فایل‌ها به S3
"""

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import quote, urlencode

import aiohttp
import aioboto3
from botocore.config import Config
import xml.etree.ElementTree as ET

# URL های API رویترز
AUTH_URL = "https://commerce.reuters.com/rmd/rest/xml/login"
CHANNELS_URL = "http://rmb.reuters.com/rmd/rest/xml/channels"
ITEMS_URL = "http://rmb.reuters.com/rmd/rest/xml/items"
ITEM_URL = "http://rmb.reuters.com/rmd/rest/xml/item"

# Namespace برای XML
NAMESPACES = {
    'nar': 'http://iptc.org/std/nar/2006-10-01/',
    'rtr': 'http://www.reuters.com/ns/2003/08/content'
}


def log(message: str, level: str = "INFO"):
    """تابع لاگ"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


async def authenticate(username: str, password: str) -> Optional[str]:
    """احراز هویت با API رویترز"""
    log("در حال احراز هویت با API رویترز...")
    
    try:
        params = {
            "username": username,
            "password": password
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(AUTH_URL, params=params) as response:
                if response.status != 200:
                    log(f"خطا در احراز هویت: HTTP {response.status}", "ERROR")
                    return None
                
                xml_content = await response.text()
                root = ET.fromstring(xml_content)
                
                if root.tag == "authToken" and root.text:
                    token = root.text.strip()
                    log("احراز هویت موفق. توکن دریافت شد.", "SUCCESS")
                    return token
                else:
                    log("خطا: توکن در پاسخ یافت نشد.", "ERROR")
                    return None
    except Exception as e:
        log(f"خطا در احراز هویت: {str(e)}", "ERROR")
        return None


async def get_channels(token: str, content_type: str) -> List[str]:
    """دریافت لیست کانال‌ها"""
    log("در حال دریافت لیست کانال‌ها...")
    
    try:
        # تعیین نوع کانال بر اساس نوع محتوا
        channel_category = {
            "Video": "BRV",
            "Text": "TXT",
            "Photo": "PIC"
        }.get(content_type, "BRV")
        
        params = {
            "channelCategory": channel_category,
            "token": token
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(CHANNELS_URL, params=params) as response:
                if response.status != 200:
                    log(f"خطا در دریافت کانال‌ها: HTTP {response.status}", "ERROR")
                    return []
                
                xml_content = await response.text()
                
                # Debug: نمایش XML برای بررسی
                log(f"  XML Response (first 2000 chars): {xml_content[:2000]}", "DEBUG")
                
                root = ET.fromstring(xml_content)
                log(f"  Root tag: {root.tag}, Root attribs: {root.attrib}", "DEBUG")
                
                channels = []
                
                # استخراج کانال‌ها از XML
                if content_type == "Text":
                    # برای Text channels، از channelInformation و alias استفاده می‌شود
                    for channel_info in root.findall(".//channelInformation"):
                        alias = channel_info.find("alias")
                        if alias is not None and alias.text:
                            channel_id = alias.text.strip()
                            if channel_id:
                                channels.append(channel_id)
                    
                    # اگر با channelInformation پیدا نشد، از result امتحان کن
                    if not channels:
                        for result in root.findall(".//result"):
                            channel_id = result.get("id")
                            if channel_id:
                                channels.append(channel_id)
                else:
                    # برای Video و Photo از result و id استفاده می‌شود
                    for result in root.findall(".//result"):
                        channel_id = result.get("id")
                        if channel_id:
                            channels.append(channel_id)
                    
                    # اگر با result پیدا نشد، از channelInformation امتحان کن
                    if not channels:
                        for channel_info in root.findall(".//channelInformation"):
                            alias = channel_info.find("alias")
                            if alias is not None and alias.text:
                                channel_id = alias.text.strip()
                                if channel_id:
                                    channels.append(channel_id)
                
                # Debug: نمایش XML برای بررسی
                if not channels:
                    log(f"  XML Response (first 1000 chars): {xml_content[:1000]}", "DEBUG")
                    log(f"  Root tag: {root.tag}, Root attribs: {root.attrib}", "DEBUG")
                    # بررسی تمام عناصر
                    for elem in root.iter():
                        log(f"  Element: {elem.tag}, Text: {elem.text}, Attribs: {elem.attrib}", "DEBUG")
                
                # Debug: بررسی تمام عناصر سطح اول
                if not channels:
                    log("  بررسی عناصر سطح اول XML...", "DEBUG")
                    for elem in list(root)[:20]:  # فقط 20 عنصر اول
                        log(f"    Element: {elem.tag}, Text: {elem.text[:100] if elem.text else None}, Attribs: {elem.attrib}", "DEBUG")
                
                log(f"تعداد {len(channels)} کانال یافت شد.", "SUCCESS")
                return channels
    except Exception as e:
        log(f"خطا در دریافت کانال‌ها: {str(e)}", "ERROR")
        return []


async def get_items(token: str, channel: str, limit: int, content_type: str) -> List[Dict[str, str]]:
    """دریافت لیست آیتم‌ها"""
    log(f"در حال دریافت آیتم‌ها از کانال: {channel}")
    
    try:
        params = {
            "token": token,
            "channel": channel,
            "limit": str(limit)
        }
        
        # برای ویدئو، پارامترهای اضافی
        if content_type == "Video":
            params["mediaType"] = "V"
            params["remoteContentComplete"] = "True"
        
        # Debug: نمایش URL و پارامترها
        log(f"  URL: {ITEMS_URL}, Params: {params}", "DEBUG")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(ITEMS_URL, params=params) as response:
                if response.status != 200:
                    log(f"خطا در دریافت آیتم‌ها از کانال {channel}: HTTP {response.status}", "ERROR")
                    response_text = await response.text()
                    log(f"  Response body: {response_text[:500]}", "DEBUG")
                    return []
                
                xml_content = await response.text()
                
                # Debug: نمایش XML برای بررسی
                log(f"  XML Response (first 2000 chars): {xml_content[:2000]}", "DEBUG")
                
                root = ET.fromstring(xml_content)
                log(f"  Root tag: {root.tag}, Root attribs: {root.attrib}", "DEBUG")
                
                items = []
                
                # استخراج آیتم‌ها از XML
                # در XML رویترز، id و guid به صورت child elements هستند نه attributes
                for result in root.findall(".//result"):
                    # اول از attribute امتحان کن
                    item_id = result.get("id")
                    item_guid = result.get("guid")
                    
                    # اگر attribute نبود، از child elements استفاده کن
                    if not item_id:
                        id_elem = result.find("id")
                        if id_elem is not None and id_elem.text:
                            item_id = id_elem.text.strip()
                    
                    if not item_guid:
                        guid_elem = result.find("guid")
                        if guid_elem is not None and guid_elem.text:
                            item_guid = guid_elem.text.strip()
                    
                    if item_id:
                        items.append({
                            "id": item_id,
                            "guid": item_guid if item_guid else item_id
                        })
                
                # Debug: بررسی تمام عناصر سطح اول
                if not items:
                    log("  بررسی عناصر سطح اول XML...", "DEBUG")
                    for elem in list(root)[:20]:  # فقط 20 عنصر اول
                        log(f"    Element: {elem.tag}, Text: {elem.text[:100] if elem.text else None}, Attribs: {elem.attrib}", "DEBUG")
                    # بررسی تعداد result elements
                    all_results = root.findall(".//result")
                    log(f"  تعداد کل result elements: {len(all_results)}", "DEBUG")
                    for idx, result in enumerate(all_results[:5]):  # فقط 5 نتیجه اول
                        log(f"    Result {idx}: id={result.get('id')}, guid={result.get('guid')}, attribs={result.attrib}", "DEBUG")
                
                log(f"تعداد {len(items)} آیتم از کانال {channel} دریافت شد.", "SUCCESS")
                return items
    except Exception as e:
        log(f"خطا در دریافت آیتم‌ها از کانال {channel}: {str(e)}", "ERROR")
        return []


async def get_item_detail(token: str, item_id: str, channel: str, content_type: str) -> Optional[str]:
    """دریافت جزئیات آیتم"""
    try:
        params = {
            "token": token,
            "id": item_id
        }
        
        if content_type == "Text" and channel:
            params["channel"] = channel
        
        async with aiohttp.ClientSession() as session:
            async with session.get(ITEM_URL, params=params) as response:
                if response.status != 200:
                    log(f"خطا در دریافت جزئیات آیتم {item_id}: HTTP {response.status}", "ERROR")
                    return None
                
                # برای خبرهای متنی، XML خام را دریافت کن
                if content_type == "Text":
                    return await response.text()
                else:
                    return await response.text()
    except Exception as e:
        log(f"خطا در دریافت جزئیات آیتم {item_id}: {str(e)}", "ERROR")
        return None


def get_filename_from_xml(xml_content: str) -> Optional[str]:
    """استخراج fileName از XML"""
    try:
        root = ET.fromstring(xml_content)
        
        # استخراج fileName از itemMeta/fileName
        for item_meta in root.findall(".//{http://iptc.org/std/nar/2006-10-01/}itemMeta"):
            filename = item_meta.find("{http://iptc.org/std/nar/2006-10-01/}fileName")
            if filename is not None and filename.text:
                file_name = filename.text.strip()
                # اطمینان از اینکه extension .xml دارد
                if not file_name.lower().endswith(".xml"):
                    file_name += ".xml"
                return file_name
        
        # اگر با namespace پیدا نشد، بدون namespace امتحان کن
        for item_meta in root.findall(".//itemMeta"):
            filename = item_meta.find("fileName")
            if filename is not None and filename.text:
                file_name = filename.text.strip()
                if not file_name.lower().endswith(".xml"):
                    file_name += ".xml"
                return file_name
        
        # اگر هنوز پیدا نشد، در همه جا جستجو کن
        for filename in root.findall(".//fileName"):
            if filename.text:
                file_name = filename.text.strip()
                if not file_name.lower().endswith(".xml"):
                    file_name += ".xml"
                return file_name
        
        return None
    except Exception as e:
        log(f"خطا در استخراج fileName: {str(e)}", "ERROR")
        return None


def get_headline_from_xml(xml_content: str) -> Optional[str]:
    """استخراج headline از XML"""
    try:
        root = ET.fromstring(xml_content)
        
        # استخراج headline
        headline = root.find(".//{http://iptc.org/std/nar/2006-10-01/}headline")
        if headline is not None and headline.text:
            return headline.text.strip()
        
        # اگر با namespace پیدا نشد، بدون namespace امتحان کن
        headline = root.find(".//headline")
        if headline is not None and headline.text:
            return headline.text.strip()
        
        return None
    except Exception as e:
        return None


def normalize_headline(headline: str) -> str:
    """Normalize کردن headline برای مقایسه"""
    if not headline:
        return ""
    
    # حذف فاصله‌های اضافی، تبدیل به lowercase، حذف کاراکترهای خاص
    normalized = headline.lower().strip()
    normalized = re.sub(r'\s+', ' ', normalized)  # چند فاصله به یک فاصله
    normalized = re.sub(r'[^\w\s]', '', normalized)  # حذف کاراکترهای خاص
    return normalized


def get_video_url_from_xml(xml_content: str, token: str) -> Optional[str]:
    """استخراج URL ویدئو از XML"""
    try:
        root = ET.fromstring(xml_content)
        
        target_rendition = "rend:stream:8256:16x9:mp4"
        
        # استخراج URL ویدئو با rendition="rend:stream:8256:16x9:mp4"
        for remote_content in root.findall(".//{http://iptc.org/std/nar/2006-10-01/}remoteContent"):
            rendition = remote_content.get("rendition", "")
            if rendition and rendition.lower() == target_rendition.lower():
                # اول altLoc را امتحان کن (authenticated URL)
                alt_loc = remote_content.find(".//{http://www.reuters.com/ns/2003/08/content}altLoc")
                if alt_loc is not None and alt_loc.text:
                    url = alt_loc.text.strip()
                    # اضافه کردن token اگر موجود نیست
                    if "token=" not in url:
                        separator = "&" if "?" in url else "?"
                        url = f"{url}{separator}token={token}"
                    return url
                
                # اگر altLoc نبود، از href استفاده کن
                href = remote_content.get("href", "")
                if href:
                    if "token=" not in href:
                        separator = "&" if "?" in href else "?"
                        href = f"{href}{separator}token={token}"
                    return href
        
        return None
    except Exception as e:
        log(f"خطا در استخراج URL ویدئو: {str(e)}", "ERROR")
        return None


def get_image_url_from_xml(xml_content: str, token: str) -> Optional[str]:
    """استخراج URL عکس از XML با rendition="rend:baseImage" """
    try:
        root = ET.fromstring(xml_content)
        
        target_rendition = "rend:baseImage"
        
        # استخراج URL عکس با rendition="rend:baseImage"
        # جستجو در تمام remoteContent elements
        import re
        for remote_content in root.findall(".//{http://iptc.org/std/nar/2006-10-01/}remoteContent"):
            rendition = remote_content.get("rendition", "")
            if rendition and rendition.lower() == target_rendition.lower():
                # اول altLoc را امتحان کن (authenticated URL) - این اولویت دارد
                # altLoc به صورت مستقیم child از remoteContent است
                alt_loc = remote_content.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                if alt_loc is not None and alt_loc.text:
                    url = alt_loc.text.strip()
                    # برای auth-server URLs، همیشه token را اضافه کن
                    if "auth-server" in url:
                        # حذف token موجود (اگر وجود دارد)
                        url = re.sub(r'[?&]token=[^&]*', '', url)
                        # اضافه کردن token جدید
                        separator = "&" if "?" in url else "?"
                        url = f"{url}{separator}token={token}"
                    elif "token=" not in url:
                        # اگر auth-server نیست اما token ندارد، اضافه کن
                        separator = "&" if "?" in url else "?"
                        url = f"{url}{separator}token={token}"
                    log(f"  URL عکس از altLoc استخراج شد: {url[:100]}...", "DEBUG")
                    return url
                
                # اگر altLoc نبود، از href استفاده کن
                href = remote_content.get("href", "")
                if href:
                    # برای auth-server URLs، همیشه token را اضافه کن
                    if "auth-server" in href:
                        href = re.sub(r'[?&]token=[^&]*', '', href)
                        separator = "&" if "?" in href else "?"
                        href = f"{href}{separator}token={token}"
                    elif "token=" not in href:
                        separator = "&" if "?" in href else "?"
                        href = f"{href}{separator}token={token}"
                    log(f"  URL عکس از href استخراج شد: {href[:100]}...", "DEBUG")
                    return href
        
        # اگر با rendition پیدا نشد، لاگ کن
        log("  هشدار: remoteContent با rendition=rend:baseImage یافت نشد", "DEBUG")
        # نمایش تمام rendition های موجود برای debugging
        all_renditions = []
        for remote_content in root.findall(".//{http://iptc.org/std/nar/2006-10-01/}remoteContent"):
            rend = remote_content.get("rendition", "")
            if rend:
                all_renditions.append(rend)
        if all_renditions:
            log(f"  Rendition های موجود: {', '.join(all_renditions)}", "DEBUG")
        
        return None
    except Exception as e:
        log(f"خطا در استخراج URL عکس: {str(e)}", "ERROR")
        return None


async def download_video(video_url: str, output_path: str, filename: str) -> bool:
    """دانلود ویدئو"""
    try:
        log(f"  در حال دانلود ویدئو: {filename}", "INFO")
        
        file_path = os.path.join(output_path, filename)
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=600),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'video/mp4,video/*,*/*;q=0.8',
            }
        ) as session:
            async with session.get(video_url) as response:
                if response.status != 200:
                    log(f"  خطا در دانلود ویدئو: HTTP {response.status}", "ERROR")
                    return False
                
                # دانلود و ذخیره
                with open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
        
        log(f"  ویدئو با موفقیت دانلود شد: {filename}", "SUCCESS")
        return True
    except Exception as e:
        log(f"  خطا در دانلود ویدئو {filename}: {str(e)}", "ERROR")
        return False


async def download_image(image_url: str, output_path: str, filename: str, token: str = None) -> bool:
    """دانلود عکس - استفاده از روش موفق reuters_photos.py با urllib.request"""
    import urllib.request
    
    try:
        log(f"  در حال دانلود عکس: {filename}", "INFO")
        
        file_path = os.path.join(output_path, filename)
        
        # اطمینان از اینکه token در URL موجود است
        # روش مشابه reuters_photos.py
        final_url = image_url
        if token:
            # Add token to URL
            final_url = f"{final_url}?token={token}"
        
        log(f"  URL عکس: {final_url[:150]}...", "DEBUG")
        
        # استفاده از همان روش reuters_photos.py - urllib.request در thread pool
        def download_sync():
            req = urllib.request.Request(
                final_url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            )
            with urllib.request.urlopen(req, timeout=300) as response:
                with open(file_path, 'wb') as f:
                    f.write(response.read())
        
        # Run download in thread pool (مشابه reuters_photos.py)
        await asyncio.to_thread(download_sync)
        
        file_size = os.path.getsize(file_path)
        log(f"  عکس با موفقیت دانلود شد: {filename} ({file_size / 1024 / 1024:.2f} MB)", "SUCCESS")
        return True
    except Exception as e:
        log(f"  خطا در دانلود عکس {filename}: {str(e)}", "ERROR")
        import traceback
        log(f"  Traceback: {traceback.format_exc()}", "DEBUG")
        return False


async def upload_file_to_s3(
    file_path: str,
    s3_key: str,
    s3_endpoint: str,
    s3_bucket: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_region: str,
    mime_type: str = "application/octet-stream"
) -> bool:
    """آپلود فایل به S3"""
    try:
        log(f"  در حال آپلود به S3: {s3_key}", "INFO")
        
        # ساخت config
        boto_config = Config(
            connect_timeout=60,
            read_timeout=60,
            retries={'max_attempts': 3}
        )
        
        # تنظیمات client
        client_kwargs = {
            "endpoint_url": s3_endpoint,
            "aws_access_key_id": s3_access_key,
            "aws_secret_access_key": s3_secret_key,
            "region_name": s3_region,
            "config": boto_config,
        }
        
        # برای HTTPS، SSL verification را غیرفعال کن
        if s3_endpoint.startswith("https://"):
            client_kwargs["verify"] = False
        
        # آپلود
        async with aioboto3.Session().client("s3", **client_kwargs) as s3:
            with open(file_path, 'rb') as f:
                await s3.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=f,
                    ContentType=mime_type
                )
        
        log(f"  فایل با موفقیت به S3 آپلود شد: {s3_key}", "SUCCESS")
        return True
    except Exception as e:
        log(f"  خطا در آپلود به S3: {str(e)}", "ERROR")
        return False


def save_xml_to_file(xml_content: str, file_path: str, save_as_raw: bool = False) -> bool:
    """ذخیره XML به فایل"""
    try:
        # برای خبرهای متنی، XML را به صورت خام ذخیره کن
        if save_as_raw:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(xml_content)
        else:
            # برای سایر انواع، XML را parse و ذخیره کن
            root = ET.fromstring(xml_content)
            tree = ET.ElementTree(root)
            tree.write(file_path, encoding='utf-8', xml_declaration=True)
        
        log(f"فایل ذخیره شد: {os.path.basename(file_path)}", "SUCCESS")
        return True
    except Exception as e:
        log(f"خطا در ذخیره فایل {os.path.basename(file_path)}: {str(e)}", "ERROR")
        return False


def load_downloaded_headlines(history_file: str) -> Set[str]:
    """بارگذاری history از فایل"""
    downloaded = set()
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    downloaded = set(data.keys())
            log(f"تعداد {len(downloaded)} headline از history بارگذاری شد", "INFO")
        except Exception as e:
            log(f"خطا در بارگذاری history: {str(e)}", "WARNING")
    return downloaded


def save_downloaded_headlines(downloaded: Set[str], history_file: str):
    """ذخیره history به فایل"""
    try:
        data = {headline: True for headline in downloaded}
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"خطا در ذخیره history: {str(e)}", "WARNING")


async def process_channel(
    token: str,
    channel: str,
    content_type: str,
    limit: int,
    output_dir: str,
    today: str,
    downloaded_headlines: Set[str],
    upload_to_s3: bool,
    s3_config: Optional[Dict]
):
    """پردازش یک کانال"""
    log("----------------------------------------")
    log(f"پردازش کانال: {channel}")
    
    # دریافت لیست آیتم‌ها
    items = await get_items(token, channel, limit, content_type)
    
    if not items:
        log(f"هیچ آیتمی در کانال {channel} یافت نشد.")
        return 0
    
    # ساختار پوشه‌ها
    content_type_dir = os.path.join(output_dir, content_type)
    date_dir = os.path.join(content_type_dir, today)
    channels_dir = os.path.join(date_dir, "channels")
    items_dir = os.path.join(date_dir, "items")
    details_dir = os.path.join(date_dir, "details")
    
    # ایجاد پوشه‌ها
    for dir_path in [channels_dir, items_dir, details_dir]:
        os.makedirs(dir_path, exist_ok=True)
    
    # ذخیره لیست آیتم‌ها
    items_xml = ET.Element("items", channel=channel)
    for item in items:
        item_elem = ET.SubElement(items_xml, "item")
        ET.SubElement(item_elem, "id").text = item["id"]
        ET.SubElement(item_elem, "guid").text = item["guid"]
    
    items_tree = ET.ElementTree(items_xml)
    items_file = os.path.join(items_dir, f"items_{channel}.xml")
    items_tree.write(items_file, encoding='utf-8', xml_declaration=True)
    
    # دریافت جزئیات هر آیتم
    for idx, item in enumerate(items, 1):
        log(f"  [{idx}/{len(items)}] دریافت جزئیات آیتم: {item['id']}")
        
        detail_xml = await get_item_detail(token, item["id"], channel, content_type)
        
        if detail_xml:
            # استخراج fileName از XML
            detail_filename = get_filename_from_xml(detail_xml)
            
            # اگر fileName یافت نشد، از GUID استفاده کن
            if not detail_filename:
                safe_guid = item["guid"].replace(":", "_").replace("/", "_")
                detail_filename = f"detail_{safe_guid}.xml"
            
            # چک کردن وجود فایل
            file_path = os.path.join(details_dir, detail_filename)
            file_exists = os.path.exists(file_path)
            
            if file_exists:
                log(f"  فایل موجود است، از دانلود مجدد صرف‌نظر شد: {detail_filename}", "INFO")
                # اگر فایل موجود است، همچنان به شمارش اضافه می‌شود
                continue
            
            # ذخیره فایل
            save_xml_to_file(detail_xml, file_path, save_as_raw=(content_type == "Text"))
            
            # آپلود XML به S3
            if upload_to_s3 and s3_config:
                s3_key = f"{content_type}/{today}/{detail_filename}"
                await upload_file_to_s3(
                    file_path, s3_key,
                    s3_config["endpoint"], s3_config["bucket"],
                    s3_config["access_key"], s3_config["secret_key"],
                    s3_config["region"], "application/xml"
                )
            
            # برای ویدئو، دانلود فایل ویدئو
            if content_type == "Video":
                # استخراج headline برای چک کردن تکراری بودن
                headline = get_headline_from_xml(detail_xml)
                normalized_headline = normalize_headline(headline) if headline else ""
                
                # چک کردن اینکه آیا ویدئو با همین headline قبلاً دانلود شده یا نه
                if normalized_headline and normalized_headline in downloaded_headlines:
                    log(f"  ویدئو با headline مشابه قبلاً دانلود شده، از دانلود صرف‌نظر شد: {headline}", "INFO")
                    continue
                
                video_url = get_video_url_from_xml(detail_xml, token)
                if video_url:
                    # نام فایل ویدئو: همان نام XML اما با پسوند .mp4
                    video_filename = os.path.splitext(detail_filename)[0] + ".mp4"
                    video_file_path = os.path.join(details_dir, video_filename)
                    
                    # چک کردن وجود فایل ویدئو
                    if os.path.exists(video_file_path):
                        log(f"  فایل ویدئو موجود است، از دانلود مجدد صرف‌نظر شد: {video_filename}", "INFO")
                        if normalized_headline:
                            downloaded_headlines.add(normalized_headline)
                    else:
                        download_success = await download_video(video_url, details_dir, video_filename)
                        if download_success:
                            if normalized_headline:
                                downloaded_headlines.add(normalized_headline)
                            
                            # آپلود ویدئو به S3
                            if upload_to_s3 and s3_config:
                                s3_key = f"{content_type}/{today}/{video_filename}"
                                await upload_file_to_s3(
                                    video_file_path, s3_key,
                                    s3_config["endpoint"], s3_config["bucket"],
                                    s3_config["access_key"], s3_config["secret_key"],
                                    s3_config["region"], "video/mp4"
                                )
                else:
                    log("  هشدار: URL ویدئو با rendition=rend:stream:8256:16x9:mp4 یافت نشد", "WARNING")
            
            # برای عکس، دانلود فایل عکس
            if content_type == "Photo":
                image_url = get_image_url_from_xml(detail_xml, token)
                if image_url:
                    # نام فایل عکس: همان نام XML اما با پسوند .jpg
                    image_filename = os.path.splitext(detail_filename)[0] + ".jpg"
                    image_file_path = os.path.join(details_dir, image_filename)
                    
                    # چک کردن وجود فایل عکس
                    if os.path.exists(image_file_path):
                        log(f"  فایل عکس موجود است، از دانلود مجدد صرف‌نظر شد: {image_filename}", "INFO")
                    else:
                        download_success = await download_image(image_url, details_dir, image_filename, token)
                        if download_success:
                            # آپلود عکس به S3
                            if upload_to_s3 and s3_config:
                                s3_key = f"{content_type}/{today}/{image_filename}"
                                await upload_file_to_s3(
                                    image_file_path, s3_key,
                                    s3_config["endpoint"], s3_config["bucket"],
                                    s3_config["access_key"], s3_config["secret_key"],
                                    s3_config["region"], "image/jpeg"
                                )
                else:
                    log("  هشدار: URL عکس با rendition=rend:baseImage یافت نشد", "WARNING")
            
            # تاخیر کوتاه برای جلوگیری از rate limiting
            await asyncio.sleep(0.5)


async def run_download(args):
    """اجرای یک دور دانلود"""
    
    log("================================================")
    log("شروع دانلود اخبار رویترز")
    log(f"نوع محتوا: {args.content_type}")
    log(f"محدودیت: {args.limit} آیتم برای هر کانال")
    log("================================================")
    
    # ایجاد پوشه خروجی
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    log(f"پوشه خروجی ایجاد شد: {output_dir}")
    
    # ایجاد ساختار پوشه بر اساس نوع محتوا و تاریخ امروز
    today = datetime.now().strftime("%Y-%m-%d")
    content_type_dir = os.path.join(output_dir, args.content_type)
    date_dir = os.path.join(content_type_dir, today)
    
    os.makedirs(date_dir, exist_ok=True)
    log(f"پوشه‌های خروجی: {date_dir}")
    
    # احراز هویت
    token = await authenticate(args.username, args.password)
    if not token:
        log("خطا: امکان احراز هویت وجود ندارد.", "ERROR")
        raise Exception("احراز هویت ناموفق بود")
    
    # دریافت کانال‌ها
    if args.channels:
        channels_to_process = [c.strip() for c in args.channels.split(",")]
        log(f"استفاده از کانال‌های مشخص شده: {', '.join(channels_to_process)}")
    else:
        channels_to_process = await get_channels(token, args.content_type)
        if not channels_to_process:
            log("هیچ کانالی یافت نشد.", "ERROR")
            raise Exception("هیچ کانالی یافت نشد")
        log(f"تعداد {len(channels_to_process)} کانال از API دریافت شد")
    
    # ذخیره لیست کانال‌ها
    channels_xml = ET.Element("channels")
    for ch in channels_to_process:
        ET.SubElement(channels_xml, "channel").text = ch
    
    channels_tree = ET.ElementTree(channels_xml)
    channels_file = os.path.join(content_type_dir, today, "channels", "channels_list.xml")
    os.makedirs(os.path.dirname(channels_file), exist_ok=True)
    channels_tree.write(channels_file, encoding='utf-8', xml_declaration=True)
    
    # فایل history برای tracking headline های دانلود شده
    history_file = os.path.join(content_type_dir, "downloaded_headlines.json")
    downloaded_headlines = load_downloaded_headlines(history_file)
    
    # تنظیمات S3
    s3_config = None
    if args.upload_to_s3:
        if not all([args.s3_endpoint, args.s3_bucket, args.s3_access_key, args.s3_secret_key]):
            log("هشدار: تنظیمات S3 کامل نیست، از آپلود صرف‌نظر شد", "WARNING")
        else:
            s3_config = {
                "endpoint": args.s3_endpoint,
                "bucket": args.s3_bucket,
                "access_key": args.s3_access_key,
                "secret_key": args.s3_secret_key,
                "region": args.s3_region
            }
    
    # پردازش هر کانال
    total_items = 0
    total_details = 0
    
    for channel in channels_to_process:
        items = await get_items(token, channel, args.limit, args.content_type)
        total_items += len(items)
        
        if items:
            await process_channel(
                token, channel, args.content_type, args.limit,
                output_dir, today, downloaded_headlines,
                args.upload_to_s3, s3_config
            )
    
    # ذخیره history
    save_downloaded_headlines(downloaded_headlines, history_file)
    
    # شمارش نهایی فایل‌های XML
    details_dir = os.path.join(date_dir, "details")
    if os.path.exists(details_dir):
        total_details = len([f for f in os.listdir(details_dir) if f.endswith('.xml')])
    else:
        total_details = 0
    
    # خلاصه
    log("================================================")
    log("دانلود کامل شد!")
    log(f"تعداد کانال‌های پردازش شده: {len(channels_to_process)}")
    log(f"تعداد کل آیتم‌ها: {total_items}")
    log(f"تعداد جزئیات دانلود شده: {total_details}")
    log(f"مسیر خروجی: {output_dir}")
    log("================================================")


async def main():
    """تابع اصلی"""
    parser = argparse.ArgumentParser(
        description="دانلود اخبار رویترز و ذخیره به صورت XML",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--username", required=True, help="نام کاربری رویترز")
    parser.add_argument("--password", required=True, help="رمز عبور رویترز")
    parser.add_argument("--output-dir", default="./reuters_news", help="مسیر پوشه خروجی")
    parser.add_argument("--content-type", choices=["Video", "Text", "Photo"], default="Video", help="نوع محتوا")
    parser.add_argument("--limit", type=int, default=10, help="تعداد آیتم‌ها برای هر کانال")
    parser.add_argument("--channels", help="لیست کانال‌های خاص (جدا شده با کاما)")
    parser.add_argument("--s3-endpoint", help="آدرس S3 endpoint")
    parser.add_argument("--s3-bucket", help="نام S3 bucket")
    parser.add_argument("--s3-access-key", help="کلید دسترسی S3")
    parser.add_argument("--s3-secret-key", help="کلید مخفی S3")
    parser.add_argument("--s3-region", default="us-east-1", help="منطقه S3")
    parser.add_argument("--upload-to-s3", action="store_true", help="آپلود فایل‌ها به S3")
    parser.add_argument("--loop", action="store_true", default=True, help="اجرای مداوم اسکریپت در loop (پیش‌فرض: فعال)")
    parser.add_argument("--no-loop", dest="loop", action="store_false", help="غیرفعال کردن حالت loop")
    parser.add_argument("--loop-interval", type=int, default=3600, help="فاصله زمانی بین هر اجرا به ثانیه (پیش‌فرض: 3600 = 1 ساعت)")
    
    args = parser.parse_args()
    
    if args.loop:
        log("================================================")
        log("اجرای مداوم اسکریپت در حالت loop")
        log(f"فاصله زمانی بین هر اجرا: {args.loop_interval} ثانیه ({args.loop_interval // 60} دقیقه)")
        log("برای توقف، Ctrl+C را فشار دهید")
        log("================================================")
        
        iteration = 0
        while True:
            iteration += 1
            try:
                log("================================================")
                log(f"شروع اجرای دور {iteration}")
                log("================================================")
                
                await run_download(args)
                
                log("================================================")
                log(f"دور {iteration} با موفقیت به پایان رسید")
                log(f"در انتظار {args.loop_interval} ثانیه برای دور بعدی...")
                log("================================================")
                
                await asyncio.sleep(args.loop_interval)
                
            except KeyboardInterrupt:
                log("================================================")
                log("توقف توسط کاربر (Ctrl+C)")
                log("================================================")
                break
            except Exception as e:
                log(f"خطا در دور {iteration}: {str(e)}", "ERROR")
                log(f"در انتظار {args.loop_interval} ثانیه برای دور بعدی...")
                await asyncio.sleep(args.loop_interval)
    else:
        await run_download(args)


if __name__ == "__main__":
    asyncio.run(main())

