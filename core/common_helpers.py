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
                    
                    
            
    
"""
Multiple argument dispacthing.

Call multimethod on a variable number of types.
It returns a decorator which finds the multimethod of the same name,
creating it if necessary, and adds that function to it.  For example:

    @multimethod(*types)
    def func(*args):
        ...

'func' is now a multimethod which will delegate to the above function,
when called with arguments of the specified types.  If an exact match
can't be found, the next closest method will be called (and cached).
A function can have more than one multimethod decorator.

See tests for more example usage.
"""

import sys
from itertools import imap, izip

class DispatchError(TypeError):
    pass

class signature(tuple):
    "A tuple of types that supports partial ordering."
    __slots__ = ()
    def __le__(self, other):
        return len(self) <= len(other) and all(imap(issubclass, other, self))
    def __lt__(self, other):
        return self != other and self <= other
    def __sub__(self, other):
        "Return relative distances, assuming self >= other."
        return [list(left.__mro__).index(right) for left, right in izip(self, other)]

class multimethod(dict):
    "A callable directed acyclic graph of methods."
    def __new__(cls, *types):
        "Return a decorator which will add the function."
        namespace = sys._getframe(1).f_locals
        def decorator(func):
            if isinstance(func, cls):
                self, func = func, func.last
            elif func.__name__ in namespace:
                self = namespace[func.__name__]
            else:
                self = dict.__new__(cls)
                self.__name__, self.cache = func.__name__, {}
            self[types] = self.last = func
            return self
        return decorator
    def parents(self, types):
        "Find immediate parents of potential key."
        parents, ancestors = set(), set()
        for key, (value, superkeys) in self.items():
            if key < types:
                parents.add(key)
                ancestors |= superkeys
        return parents - ancestors
    def __getitem__(self, types):
        return dict.__getitem__(self, types)[0]
    def __setitem__(self, types, func):
        self.cache.clear()
        types = signature(types)
        parents = self.parents(types)
        for key, (value, superkeys) in self.items():
            if types < key and (not parents or parents & superkeys):
                superkeys -= parents
                superkeys.add(types)
        dict.__setitem__(self, types, (func, parents))
    def __delitem__(self, types):
        self.cache.clear()
        dict.__delitem__(self, types)
        for key, (value, superkeys) in self.items():
            if types in superkeys:
                dict.__setitem__(self, key, (value, self.parents(key)))
    def super(self, *types):
        "Return the next applicable method of given types."
        types = signature(types)
        keys = self.parents(types)
        if keys:
            return self[min(keys, key=types.__sub__)]
        raise DispatchError("%s%s: no methods found" % (self.__name__, types))
    def __call__(self, *args, **kwargs):
        "Resolve and dispatch to best method."
        types = tuple(imap(type, args))
        try:
            func = self.cache[types]
        except KeyError:
            func = self.cache[types] = self[types] if types in self else self.super(*types)
        return func(*args, **kwargs)

class strict_multimethod(multimethod):
    "A multimethod which requires a single unambiguous best match."
    def super(self, *types):
        "Return the next applicable method of given types."
        keys = self.parents(signature(types))
        if len(keys) == 1:
            return self[keys.pop()]
        raise DispatchError("%s%s: %d methods found" % (self.__name__, types, len(keys)))
