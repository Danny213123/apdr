from Foundation import NSBundle
from SystemConfiguration import SCPreferencesCreate
import objc


def main():
    bundle = NSBundle.bundleWithPath_("/System/Library/PrivateFrameworks/EAP8021X.framework")
    return bundle, SCPreferencesCreate(None, "python", None), objc
