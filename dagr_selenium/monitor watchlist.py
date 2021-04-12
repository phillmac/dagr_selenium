import logging
from os import environ


from .functions import config, manager, monitor_watchlist

env_level = environ.get('dagr.watchlist.logging.level', None)
level_mapped = config.map_log_level(
    int(env_level)) if not env_level is None else None

manager.set_mode('watchlist')
manager.init_logging(level_mapped)

with manager.get_dagr():
    monitor_watchlist()
logging.shutdown()
