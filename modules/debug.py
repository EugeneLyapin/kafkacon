import sys
import pprint
import config

def errx(msg=None):
    print
    print("[ERROR]: %s" % msg)
    sys.exit('')

def trace(data=None):
    if config.debug >= 9:
        pp = pprint.PrettyPrinter(indent=1)
        pp.pprint(data)

def debug(level=1, **kwargs):
    out = ''
    if config.debug >= level:
        for key in kwargs:
            value = kwargs[key]
            if type(value) is unicode:
                value = value.encode('ascii', 'ignore')
            if len(out) > 0:
                out += " "
            out += key + "=" + "'" + str(value) + "'"
        print("[DEBUG] " + out)
