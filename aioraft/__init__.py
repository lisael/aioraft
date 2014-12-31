import imp
import os
import warnings
import sys
import aioraft._defaultsettings as _defaultsettings
import logging
from logging.config import dictConfig

_CONFFILE = os.environ.get("AIORAFT_SETTINGS", "/etc/aioraft")

if not (os.path.exists(_CONFFILE)):
    warnings.warn("Configuration file does not exist. Using defaults")
else:
    _settings = imp.load_source("nursery.settings", _CONFFILE)
    _defaultsettings.__dict__.update(_settings.__dict__)

settings = _defaultsettings
dictConfig(settings.LOGGING)
