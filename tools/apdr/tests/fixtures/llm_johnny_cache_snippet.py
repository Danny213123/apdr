"""Django management command using johnny-cache.
'johnny' import maps to 'johnny-cache' on PyPI."""
import sys
import logging
from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache
from johnny.cache import invalidate
from johnny.middleware import QueryCacheMiddleware
