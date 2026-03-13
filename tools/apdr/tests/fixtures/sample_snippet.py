import json
import requests
from bs4 import BeautifulSoup


def fetch_title(url: str) -> str:
    response = requests.get(url, timeout=5)
    soup = BeautifulSoup(response.text, "html.parser")
    return soup.title.text if soup.title else "untitled"

