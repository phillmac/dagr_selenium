deviant = input('Enter username: ')
mode = input('Enter mode: ')

from functions import rip_nolink, manager

manager.set_mode('rip_nolink')
manager.init_logging()

with manager.get_dagr():
    manager.get_browser().do_login()
    rip_nolink(mode, deviant)
