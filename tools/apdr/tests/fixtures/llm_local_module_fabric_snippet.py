"""Fabric deployment script — 'deployment' is a LOCAL project module, not a PyPI package."""
from deployment.cuisine import *
from fabric.api import *
from fabric.context_managers import *
from fabric.utils import puts
from fabric.colors import red, green
import simplejson
import os

def deploy():
    puts(green("Deploying..."))
