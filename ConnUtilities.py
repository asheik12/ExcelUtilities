from os import environ, path


class ConUtil:

    def __init__(self,dPath=None):
        if (dPath == None) | (not path.exists(str(dPath))):
            self.dPath = path.join(environ['USERPROFILE'], "Desktop")
        else:
            self.dPath = dPath

    def readFile(self, fName):
        fit = path.join(self.dPath, fName)
        if path.exists(fit):
            try:
                res = open(fit, 'r')
                lst = [data.rsplit('\n') for data in res]
                return lst
            except Exception as e:
                return False
            finally:
                res.close()

        else:
            return False

    def getDefaultPath(self):
        return self.dPath

    def setDefaultPath(self, dPath):
        if path.exists(dPath):
            self.dPath = dPath
            return True
        else:
            return False

