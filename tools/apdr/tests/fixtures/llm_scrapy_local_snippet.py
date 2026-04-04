"""Scrapy test runner that imports from my_project — a LOCAL module.
scrapy -> scrapy, twisted -> Twisted. my_project must be SKIPPED."""
import subprocess
import unittest
from scrapy.crawler import Crawler
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, task
from my_project.spiders.spider1 import Spider1
from my_project.spiders.spider2 import Spider2
