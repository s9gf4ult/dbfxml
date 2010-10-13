# -*- coding: utf-8 -*-

def declare_type(type, *vars):
    for var in vars:
        if not isinstance(var, type):
            raise Exception("{0}: {1} not inherits {2}".format(__name__, var.__name__, type.__name__))

def declare_class(type, *classes):
    for classname in classes:
        if not issubclass(classname, type):
            raise Exception(u'{0}: {1} is not subclass of {2}'.format(__name__, classname.__name__, type.__name__))
