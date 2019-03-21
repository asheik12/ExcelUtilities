from ConnUtilities import ConUtil


class DataFetch:

    def __init__(self, dFile='DB.prop'):
        self.con = ConUtil()
        self.dFile = dFile

    def fetchAllDB(self, start, end):
        dbProp = self.con.readFile(self.dFile)
        print(dbProp[0][0])

    def setdPath(self, dPath):
        self.dPath = dPath

    def setdFile(self, dFile):
        self.dFile = dFile


data = DataFetch()
print(data.fetchAllDB(1,2))