import requests

url = "https://afp-apicore-prod.afp.com/oauth/token?username=ghasemzade@gmail.com&password=1234@Qwe&grant_type=password"

payload = {}
headers = {
  'Accept': 'application/json',
  'Content-Type': 'application/x-www-form-urlencoded',
  'Authorization': 'Basic SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJRUN1WGczREQ1OWk2YUg='
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
