import argparse
import json
import sys
import os
import yaml
from debug import errx, debug, trace

def getConf(args):
    global debug

    default_filename = 'config.yaml'
    parser = argparse.ArgumentParser()
    parser.add_argument('--brokers', type=str, dest='brokers', nargs='*', action='store', default=[])
    parser.add_argument('--offset', type=str, action='store', default=None)
    parser.add_argument('--topic', type=str, action='store', default=None)
    parser.add_argument('--group_id', type=str, action='store', default=None)
    parser.add_argument('--filename', type=str, action='store', default=default_filename)
    parser.add_argument('--debug', type=int, action='store', default=0)
    options = parser.parse_args(args)

    filename = getattr(options, 'filename')
    debug = getattr(options, 'debug')

    try:
        fd = open(filename)
    except IOError as error:
        filename = default_filename
        fd = open(filename)

    data = yaml.safe_load(fd)

    conf = {
      'services': data,
      'parser': {}
    }

    for key in ['offset', 'topic', 'debug', 'group_id']:
        val = getattr(options, key)
        if val != None:
            conf['parser'][key] = val

    val = options.brokers
    if val != []:
        conf['parser']['brokers'] = str.join(',', val)

    return conf
