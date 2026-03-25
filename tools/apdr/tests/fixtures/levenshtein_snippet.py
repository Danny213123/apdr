import re
import os
import subprocess
import Levenshtein

theLicenses = {
    'MIT': 'Permission is hereby granted...',
    'BSD': 'Redistribution and use...',
}

theFileLicense = "some license text"
theLicenseScores = []
for theLicenseName, theLicense in theLicenses.items():
    theLicenseScores.append((Levenshtein.distance(theFileLicense, theLicense), theLicenseName))
theLicenseScores.sort()
print theLicenseScores[0]
