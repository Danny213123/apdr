#!/usr/bin/env python
# encoding=utf-8
"""Uses pushbullet (pushbullet.py on PyPI), weibo, and xtls.
xtls is NOT on PyPI — it must be skipped."""
import codecs
import json
import os
import sys

from pushbullet import Pushbullet
from weibo import Client
from xtls.util import BeautifulSoup

pb = Pushbullet('API_KEY')
client = Client('APP_KEY', 'APP_SECRET', 'https://example.com/callback')
