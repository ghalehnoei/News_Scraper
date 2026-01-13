#!/usr/bin/env python3
import requests
import json

response = requests.get("http://localhost:3000/news/latest?limit=1")
data = response.json()

item = data.get('items', [{}])[0]
print('First article:')
print(f'  ID: {item.get("id")}')
print(f'  Title: {item.get("title", "N/A")[:50]}...')
vertical = item.get('is_vertical_image')
print(f'  is_vertical_image: {vertical} (type: {type(vertical)})')
