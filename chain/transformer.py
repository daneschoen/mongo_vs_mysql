
from __future__ import with_statement
import os, errno
import yaml


CONFIG_YML = "config.yml"

try:
    with open(CONFIG_YML) as f:
        data = f.read()
        config = yaml.load(data)
        print config
        print
        print yaml.dump(data)
    
except IOError as exc:
    if exc.errno == 2:
        print "Configuration file not found"
    else:
        print "Configuration file could not be read"