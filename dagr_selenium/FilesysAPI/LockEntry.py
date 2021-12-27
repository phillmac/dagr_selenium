import portalocker

class LockEntry():
    def __init__(self, diritem):
        self.__diritem = diritem
        self.__lockfile = diritem.joinpath('.lock')
        self.__lock = portalocker.Lock(
            self.__lockfile, fail_when_locked=True, flags=portalocker.LOCK_EX)
        self.__expiry = time() + 300
        self.__created = time()

        print(f"Lockfile {str(self.__lockfile)} exists:",
              self.__lockfile.exists())

    @property
    def details(self):
        return {
            'path': str(self.__diritem),
            'lockfile': str(self.__lockfile),
            'expiry': self.__expiry,
            'created': self.__created
        }

    def expired(self):
        return time() > self.__expiry

    def refresh(self):
        self.__expiry = time + 300

    def release(self):
        self.__lock.release()

        if self.__lockfile.exists():
            self.__lockfile.unlink()
        else:
            print(f"Lockfile {str(self.__lockfile)} already removed")
