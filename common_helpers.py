

def declare_type(type, *vars):
    for var in vars:
        if not issubclass(var.__class__, type):
            raise Exception("{0}: {1} not inherits {2}".format(__name__, var, type))
        
