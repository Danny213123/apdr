#!/usr/bin/python
"""macOS-only snippet using Objective-C frameworks via pyobjc bridge.
These imports CANNOT work in a Docker Linux environment."""
from getpass import getpass
from os import system
from OpenDirectory import ODSession, ODNode, kODRecordTypeComputers
from SystemConfiguration import SCDynamicStoreCreate, SCDynamicStoreCopyValue
import plistlib
import CoreFoundation
from Foundation import NSDate, NSMutableArray, NSMutableDictionary
