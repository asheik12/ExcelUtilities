from os import environ, listdir
from os.path import join, exists, basename
from pandas import read_excel, DataFrame
from datetime import datetime as dt

class Summary:

    def __init__(self, dirPath=None, setPath=None):
        if dirPath == None:
            self.dirPath = join(self.getDefaultPath(), 'Summary')
        else:
            self.dirPath = dirPath
        if setPath == None:
            self.setPath = join(self.getDefaultPath(), "Settings.xlsx")
        else:
            self.setPath = setPath

    def calculateSummary(self, totals=True):
        ldSet = self.loadSettings()
        if type(ldSet) is bool:
            rfls = self.readAllFiles()
            if type(rfls) is bool:
                idf = self.indexDataframe()
                if type(idf) is bool:
                    crow = self.combinerows()
                    if type(crow) is bool:
                        ccol = self.combinecols()
                        if type(ccol) is bool:
                            aop = self.doArithmeticOperations()
                            if type(aop) is bool:
                                if totals:
                                    exp = self.exportWithTotals()
                                    if type(exp) is bool:
                                        return True
                                    else:
                                        return exp
                                else:
                                    exp = self.exportWithoutTotals()
                                    if type(exp) is bool:
                                        return True
                                    else:
                                        return exp
                            else:
                                return aop
                        else:
                            return ccol
                    else:
                        return crow
                else:
                    return idf
            else:
                return rfls
        else:
            return ldSet


    def loadSettings(self):
        if exists(self.setPath):
            try:
                self.codsec = read_excel(self.setPath, header=None)
                self.setdata = read_excel(self.setPath, header=None, sheet_name=1)
            except Exception as e:
                return f"Error Opening File - {basename(self.setPath)} \n {e}"
            try:
                self.earnings = list(self.setdata[0].dropna())
                self.deductions = list(self.setdata[1].dropna())
                self.sepstuff = list(self.setdata[2].dropna())
                self.combinecol = list(self.setdata[3].dropna())
                self.combinerow = list(self.setdata[4].dropna())
                self.sumrows = list(self.setdata[5].dropna())
                self.extcols = list(self.setdata[6].dropna())
                self.codsec.set_index(0, inplace=True)
                self.odata = self.codsec.to_dict().get(1)
                self.dep = list(self.odata.values())
                self.codes = list(map(str, self.odata.keys()))
                self.totear = self.extcols[0]
                self.totded = self.extcols[1]
                self.netsal = self.extcols[2]
                self.bybank = self.extcols[3]
                self.arrears = self.extcols[4]
                self.admin = self.extcols[5]
                self.works = self.extcols[6]
                self.gtotal = self.extcols[7]
                return True
            except Exception as e:
                return f"Missing some datas from {basename(self.setPath)} file \n {e}"
        else:
            return f"Cannot find file {basename(self.setPath)} in the path"

    def readAllFiles(self):
        if exists(self.dirPath):
            self.dFrame = DataFrame()
            try:
                for files in listdir(self.dirPath):
                    if files.lower().endswith('.xlsx'):
                        if files.split('.')[0] in self.codes:
                            df = read_excel(join(self.dirPath, files)).drop_duplicates(['Wage Type Long Text'], keep='last')
                            df.dropna(inplace=True)
                            df.insert(2, 'Section', self.odata.get(int(files.split('.')[0])))
                            self.dFrame = self.dFrame.append(df, ignore_index=True)
                return True
            except Exception as e:
                return f"Error reading files from {self.dirPath} \n {e}"
        else:
            return f"Path {self.dirPath} not exists"

    def indexDataframe(self):
        try:
            self.dFrame = self.dFrame.pivot('Section', 'Wage Type Long Text', 'Amount')
            self.dFrame = self.dFrame.reindex(self.earnings + self.deductions + self.sepstuff, axis=1)
            self.dFrame = self.dFrame.reindex(self.dep, axis=0)
            self.dFrame.fillna(0, inplace=True)
            return True
        except Exception as e:
            return f"Error while rearraning & indexing the dataframe \n {e}"

    def combinerows(self):
        try:
            for i in self.combinerow:
                splitData = i.split("-")
                if splitData[0] == 'R':
                    rowname = i.replace("R-", "", 1).replace("-", "&")
                    if len(splitData) > 2:
                        consrow = splitData[1]
                        if consrow in self.dep:
                            self.dep.insert(self.dep.index(consrow), rowname)
                            self.dep.remove(consrow)
                        for j in splitData:
                            if (j != 'R') & (j != consrow):
                                self.dFrame.loc[consrow] = self.dFrame.loc[consrow].add(self.dFrame.loc[j])
                                self.dFrame.drop(j, inplace=True)
                                if j in self.dep:
                                    self.dep.remove(j)
                        self.dFrame.rename(index={consrow: rowname}, inplace=True)
            return True
        except Exception as e:
            return f"Error while combining rows in the dataframe \n {e}"

    def combinecols(self):
        try:
            for i in self.combinecol:
                splitcol = i.split("-")
                if splitcol[0] == 'C':
                    if len(splitcol) > 1:
                        colname = i.replace("C-", "", 1).replace("-", "&")
                        self.dFrame.insert(self.dFrame.columns.get_loc(colname.split("&")[0]), colname, 0)
                        if splitcol[1] in self.earnings:
                            self.earnings.insert(self.earnings.index(splitcol[1]), colname)
                        elif splitcol[1] in self.deductions:
                            self.deductions.insert(self.deductions.index(splitcol[1]), colname)
                        for j in splitcol:
                            if j != 'C':
                                self.dFrame[colname] = self.dFrame[colname].add(self.dFrame[j])
                                self.dFrame.drop(j, axis=1, inplace=True)
                                if j in self.earnings:
                                    self.earnings.remove(j)
                                elif j in self.deductions:
                                    self.deductions.remove(j)
            return True
        except Exception as e:
            return f"Error while combining columns in the dataframe \n {e}"

    def doArithmeticOperations(self):
        try:
            self.dFrame.insert(self.dFrame.columns.get_loc(self.deductions[0]), self.totear, round(self.dFrame[self.earnings].sum(axis=1), 2))
            self.dFrame.insert(self.dFrame.columns.get_loc(self.sepstuff[0]), self.totded, round(self.dFrame[self.deductions].sum(axis=1), 2))
            self.dFrame.insert(self.dFrame.columns.get_loc(self.sepstuff[0]), self.netsal, round(self.dFrame[[self.totear, self.totded]].sum(axis=1), 2))
            self.dFrame.insert(self.dFrame.columns.get_loc(self.totear), self.arrears, round(self.dFrame[self.sepstuff[1]] - self.dFrame[self.netsal], 2))
            self.dFrame[self.totear] = round(self.dFrame[self.totear] + self.dFrame[self.arrears], 2)
            self.dFrame[self.netsal] = round(self.dFrame[self.totear] + self.dFrame[self.totded], 2)
            self.dFrame.insert(len(self.dFrame.columns), self.bybank, round(self.dFrame[self.netsal] - self.dFrame[self.sepstuff[0]], 2))
            self.dFrame.drop(self.sepstuff[1], axis=1, inplace=True)
            self.sepstuff.remove(self.sepstuff[1])
            self.dFrame[self.deductions] = self.dFrame[self.deductions].mul(-1)
            self.dFrame[self.totded] = self.dFrame[self.totded].mul(-1)
            return True
        except Exception as e:
            return f"Error while performing arithmetic operations in the dataframe \n {e}"


    def exportWithTotals(self):
        try:
            ilen = self.dFrame.index.size
            totalFrame = self.dFrame.copy()
            totalFrame.loc[self.admin] = totalFrame.loc[:self.sumrows[0]].sum()
            totalFrame.loc[self.works] = totalFrame.iloc[totalFrame.index.get_loc(self.sumrows[0]) + 1:ilen].sum()
            totalFrame.loc[self.gtotal] = totalFrame.iloc[:ilen].sum()
            totalFrame.replace(0.0, "", inplace=True)
            self.exportToFile(totalFrame)
            return True
        except Exception as e:
            return f"Error while exporting datas to file \n {e}"

    def exportWithoutTotals(self):
        try:
            self.dFrame.replace(0.0, "", inplace=True)
            self.exportToFile(self.dFrame)
            return True
        except Exception as e:
            return f"Error while exporting datas to file \n {e}"

    def exportToFile(self, dataframe):
        try:
            dataframe.to_excel(join(self.dirPath, "Summary.xlsx"))
            return True
        except Exception as e:
            return f"Error while exporting datas to file \n {e}"

    def setSettings(self, dirPath, setPath):
        self.dirPath = dirPath
        self.setPath = setPath

    def getDefaultPath(self):
        return join(environ['USERPROFILE'], "Desktop")




sum = Summary()
res = sum.calculateSummary()
if type(res) is bool:
    print("Sucessfully Finished")
else:
    print("Failed with "+ res)