# -*- coding: utf-8 -*-
import argparse
import json
import sys
import os
import yaml
from debug import errx, debug, trace

def getConf(args):
    global debug

    default_filename = 'config.yaml'
    parser = argparse.ArgumentParser('KafkaCon: Create a new Consumer instance using the provided configuration, poll value from the offset and decrypt field(s) using AWS CMK (optional)')
    parser.add_argument('--brokers', type=str, dest='brokers', nargs='*', action='store', default=[], help='The List of brokers to connect (required)')
    parser.add_argument('--offset', type=str, action='store', default=None, help='The offset to seek to (required)')
    parser.add_argument('--topic', type=str, action='store', default=None, help='The topic (required)')
    parser.add_argument('--groupid', type=str, action='store', default=None, help='Client group id string. All clients sharing the same group.id belong to the same group (required)')
    parser.add_argument('--filename', type=str, action='store', default=default_filename, help='The filename to read configuration for KMS/Kafka (optional, default: config.yaml)')
    parser.add_argument('--debug', type=int, action='store', default=0, help='Debug level (0..3) (optional)')
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

    for key in ['offset', 'topic', 'debug', 'groupid']:
        val = getattr(options, key)
        if val != None:
            conf['parser'][key] = val

    val = options.brokers
    if val != []:
        conf['parser']['brokers'] = str.join(',', val)

    return conf
