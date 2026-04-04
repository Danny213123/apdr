#!/usr/bin/env python
"""Uses 'from plist import *' — plist is NOT a real PyPI package.
(biplist exists but has a completely different API.)"""
from plist import *
import struct
import sys
from binascii import hexlify
from collections import OrderedDict

data = read_plist("backup.plist")
