from os import environ
from os.path import join, exists


class ConUtil:

    def __init__(self):
        pass

    def readFile(self, fPath):
        if exists(fPath):
            try:
                res = open(fPath, 'r')
                lst = [data for data in res]
                return lst
            except Exception as e:
                return False
            finally:
                res.close()
        else:
            return False

    def getDefaultPath(self):
        return join(environ['USERPROFILE'], 'Desktop')
