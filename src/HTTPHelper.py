import requests
import json

cache = {}



def send_request(url):
    if url in cache:
        return cache[url]

    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    cache[url] = data
    return data