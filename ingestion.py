import requests


url = "https://api.data.gov.sg/v1/transport/taxi-availability"
response = requests.get(url)
featurecollection = response.json()

print(featurecollection)