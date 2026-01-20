import os
import sys
import json
import asyncio
import aiohttp

from dotenv import load_dotenv


async def main() -> int:
    load_dotenv(override=True)

    username = os.getenv("AFP_USERNAME")
    password = os.getenv("AFP_PASSWORD")
    basic_auth = os.getenv("AFP_BASIC_AUTH")

    print(f"AFP_USERNAME={username}")
    print(f"AFP_PASSWORD={password}")
    print(f"AFP_BASIC_AUTH={basic_auth}")

    if not username or not password or not basic_auth:
        print("Missing required AFP env variables. Check .env in project root.")
        return 2

    auth_url = (
        "https://afp-apicore-prod.afp.com/oauth/token?grant_type=password&username=ghasemzade@gmail.com&password=1234@Qwe"
    )

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJREN1WGczREQ1OWk2YUg",
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(auth_url, headers=headers) as response:
                status = response.status
                body = await response.text()
                print(f"Status: {status}")
                try:
                    print(json.dumps(await response.json(), ensure_ascii=False, indent=2))
                except json.JSONDecodeError:
                    print(body)
                return 0 if status == 200 else 1
    except aiohttp.ClientError as exc:
        print(f"RequestError: {exc}")
        return 1
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

