deviant = input('Enter username: ')
mode = input('Enter mode: ')

from functions import rip_queue, manager

manager.set_mode('rip_queue')
manager.init_logging()

with manager.get_dagr():
    manager.get_browser().do_login()
    rip_queue(mode, deviant)
