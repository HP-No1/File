# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 00:53:51 2018

@author: qianx

v0.1 setup logging, you can call setup_logging when starting your program. It reads logging.json or logging.yaml by default. You can also set LOG_CFG environment variable to load the logging configuration from specific path. For example,
LOG_CFG=my_logging.json python my_server.py
or if you prefer YAML
LOG_CFG=my_logging.yaml python my_server.py

"""
import os
import json
import logging.config

def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
