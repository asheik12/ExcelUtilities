from ConnUtilities import ConUtil
from os.path import join, dirname, exists, isdir
from datetime import date, datetime, timedelta
from pymssql import connect
from pandas import DataFrame

class DataFetch:

    def __init__(self, dFilePath=None, server=None, user=None, password=None, database=None):
        self.con = ConUtil()
        if dFilePath == None:
            self.dFilePath = join(self.con.getDefaultPath(), "DB.prop")
        else:
            self.dFilePath = dFilePath
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.status = self.loadCredentials()
        self.qstatus = self.loadQueries()

    def fetchAllDB(self, start, end, qType='A'):
        self.qType = qType
        fetResult = self.preFetch(start, end)
        if type(fetResult) is bool:
            conmsg = self.connectToDatabase()
            if type(conmsg) is bool:
                try:
                    self.cur.execute(self.squery)
                    return DataFrame(self.cur.fetchall())
                except Exception as e:
                    return f"Problem executing query \n {e}"
                finally:
                    self.closeDBParams()
            else:
                return conmsg
        else:
            return fetResult


    def preFetch(self, start, end):
        if (type(self.status) is bool) & (type(self.qstatus) is bool):
            if (type(start) is date) & (type(end) is date):
                queryType = self.getEventQuery(self.qType)
                if not queryType is None:
                    if self.qType == 'R':
                        self.squery = self.updateDateQuery(start, end, queryType)
                    else:
                        self.squery = self.updateQuery(start, end, queryType)
                    return True
                else:
                    return "Query type not found"
            else:
                return "Date format should be as type date"
        else:
            if type(self.status) is str:
                return self.status
            elif type(self.qstatus) is str:
                return self.qstatus


    def exportAllDB(self, start, end, qType='A', expPath=None):
        self.qType = qType
        expPath = self.exportPath(expPath)
        fetResult = self.preFetch(start, end)
        if type(fetResult) is bool:
            conmsg = self.connectToDatabase(cType=False)
            if type(conmsg) is bool:
                resExport = self.exportToFile(expPath)
                if type(resExport) is bool:
                    return True
                else:
                    return resExport
            else:
                return conmsg
        else:
            return fetResult

    def exportOffDB(self, start, end, qType='R', expPath=None):
        self.qType = qType
        expPath = self.exportPath(expPath)
        fetResult = self.preFetch(start, end)
        if type(fetResult) is bool:
            conmsg = self.connectToDatabase(cType=False)
            if type(conmsg) is bool:
                resExport = self.exportToFile(expPath)
                if type(resExport) is bool:
                    return True
                else:
                    return resExport
            else:
                return conmsg
        else:
            return fetResult

    def exportPath(self, ePath):
        if ePath == None:
            expPath = dirname(self.dFilePath)
        elif exists(ePath):
            if (not isdir(ePath)):
                expPath = dirname(ePath)
        else:
            expPath = dirname(self.dFilePath)
        return expPath

    def exportToFile(self, ePath):
        try:
            self.cur.execute(self.squery)
            columnNames = [i[0] for i in self.cur.description]
            try:
                exportFile = open(join(ePath, "databases.csv"), 'w')
                exportFile.write(",".join(columnNames) + "\n")
                for datas in self.cur:
                    insertrow = ""
                    for coun in range(len(columnNames)):
                        if coun == 0:
                            insertrow += datas[coun]
                        else:
                            insertrow += f",{datas[coun]}"
                    insertrow += "\n"
                    exportFile.write(insertrow)
                exportFile.close()
                return True
            except Exception as e:
                return f"Error while creating export file \n {e}"
        except Exception as e:
            return f"Problem executing query \n {e}"
        finally:
            self.closeDBParams()

    def updateQuery(self, start, end, query):
        start = f"'{start.day}/{start.month}/{start.year}'"
        end = end + timedelta(days=2)
        end = f"'{end.day}/{end.month}/{end.year}'"
        try:
            query = query.replace("''", start, 1)
            query = query.replace("''", end, 1)
        except Exception as e:
            return False
        return query

    def updateDateQuery(self, start, end, query):
        start = f"'{start.day}/{start.month}/{start.year}'"
        end = f"'{end.day}/{end.month}/{end.year}'"
        try:
            for i in range(2):
                query = query.replace("''", start, 1)
                query = query.replace("''", end, 1)
            query = query.replace("''", end, 1)
        except Exception as e:
            return False
        return query

    def getEventQuery(self, qType):
        for i in self.queries:
            if (qType in i) & ("#" in i):
                splits = i.split("#")
                if splits[0] == qType:
                    return splits[1]

    def loadQueries(self):
        self.qFilePath = join(dirname(self.dFilePath), "DB.query")
        if exists(self.qFilePath):
            self.queries = self.con.readFile(self.qFilePath)
            return True
        else:
            return f"Query file not found in location {dirname(self.dFilePath)}"

    def connectToDatabase(self, cType=True):
        if (type(self.status) is bool):
            try:
                self.conn = connect(self.server, self.user, self.password, self.database)
                self.cur = self.conn.cursor(as_dict=cType)
                return True
            except Exception as e:
                return f"Could not connect to database \n {e}"
        else:
            return "Missing Some Database Credentials"

    def setDBCredentials(self, server=None, user=None, password=None, database=None):
        if server != None:
            self.server = server
        if user != None:
            self.user = user
        if password != None:
            self.password = password
        if database != None:
            self.database = database
        self.status = self.loadCredentials()
        if (not type(self.status) is bool) and ((self.server!=None) & (self.user!=None) & (self.password!=None) & (self.database!=None)):
            self.status = True

    def loadCredentials(self):
        self.dbCred = self.con.readFile(self.dFilePath)
        if self.dbCred != False:
            if (len(self.dbCred) == 4) & ((self.server==None) & (self.user==None) & (self.password==None) & (self.database==None)):
                self.server = self.dbCred[0].strip()
                self.user = self.dbCred[1].strip()
                self.password = self.dbCred[2].strip()
                self.database = self.dbCred[3].strip()
            elif (len(self.dbCred) == 4) & ((self.server!=None) | (self.user!=None) | (self.password!=None) | (self.database!=None)):
                if self.server == None:
                    self.server = self.dbCred[0].strip()
                if self.user == None:
                    self.user = self.dbCred[1].strip()
                if self.password == None:
                    self.password = self.dbCred[2].strip()
                if self.database == None:
                    self.database = self.dbCred[3].strip()
            else:
                return f"Missing database Credentials"
            return True
        else:
            return f"Error reading file - {self.dFilePath}"

    def setdPath(self, dPath):
        self.dFilePath = dPath
        self.status = self.loadCredentials()

    def closeDBParams(self):
        print("Closing DB Connections")
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()


#dataFetch = DataFetch()
#result = dataFetch.exportAllDB(date(2019,3,15), date(2019,3,31))
#if type(result) is bool:
#    print("Sucessfully Exported")
#else:
#    print(result)
