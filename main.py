import requests

url = "https://www.data.gov.my/data/api/3/action/datastore_search?limit=5&resource_id=2903e39a-dde6-4053-818e-ebdb8066d8b7&q=title:jones"
response = requests.get(url)

try:
    response = requests.get(url)
    print(response.json())
except Exception as e:
    print("error", e)