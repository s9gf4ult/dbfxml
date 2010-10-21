# -*- coding: utf-8 -*-
import re
import os

def declare_type(type, *vars):
    for var in vars:
        if not isinstance(var, type):
            raise Exception("{0}: {1} not inherits {2}".format(__name__, var.__name__, type.__name__))

def declare_class(type, *classes):
    for classname in classes:
        if not issubclass(classname, type):
            raise Exception(u'{0}: {1} is not subclass of {2}'.format(__name__, classname.__name__, type.__name__))

def findFilesByRegexp(directory, pattern, flags = 0):
    if os.path.exists(directory) and os.path.isdir(directory):
        found = []
        for dirpath, dirs, files in os.walk(directory):
            found.extend(map(lambda a: "{0}/{1}".format(dirpath, a), filter(lambda a: re.match(pattern, a, flags = flags), files)))
        return found

def join_list(a, delim = ', '):
    return reduce(lambda one, two: u'{0}{1}{2}'.format(one, delim, two), a)

class iterSummator:
    def __init__(self, *iters):
        self.iters = map(lambda a:a.__iter__(), iters)

    def __iter__(self):
        return self

    def next(self):
        while True:
            try:
                return self.iters[0].next()
            except StopIteration:
                del self.iters[0]
                if self.iters.__len__() == 0:
                    raise
                    
                    
            
    
