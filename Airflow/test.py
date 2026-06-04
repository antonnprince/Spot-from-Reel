import requests 
import json

headers = {
    'Content-Type':'application/json'
}

res = requests.get('https://dummyjson.com/products?limit=10&skip=5&select=key1,key2', headers = headers)
print(json.dumps(res.json(),indent = 4))