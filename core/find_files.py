import re
import os

def findFilesByRegexp(directory, pattern, flags = 0):
    if os.path.exists(directory) and os.path.isdir(directory):
        found = []
        for dirpath, dirs, files in os.walk(directory):
            found.extend(map(lambda a: "{0}/{1}".format(dirpath, a), filter(lambda a: re.match(pattern, a, flags = flags), files)))
        return found

          
