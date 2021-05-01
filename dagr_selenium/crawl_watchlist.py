from .functions import monitor_watchlist_action, manager

with manager.get_dagr():
    monitor_watchlist_action()
