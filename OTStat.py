from os import environ
from os.path import join, exists, dirname
from datetime import datetime
from pandas import read_excel, DataFrame, ExcelWriter


class OTStat:

    def __init__(self, sorPath, setPath=None):
        self.sorPath = sorPath
        if setPath == None:
            self.setPath = join(self.getDefaultPath(), "OT_Set.xlsx")
        else:
            self.setPath = setPath
        self.fileStat = self.checkFilePaths()


    def startCalculations(self):
        ldset = self.loadSettings()
        if type(ldset) is bool:
            rdf = self.rearrangeDF()
            if type(rdf) is bool:
                csum = self.createSummary()
                if type(csum) is bool:
                    exp = self.exportAsFile()
                    if type(exp) is bool:
                        return True
                    else:
                        return exp
                else:
                    return csum
            else:
                return rdf
        else:
            return ldset

    def loadSettings(self):
        if type(self.fileStat) is bool:
            try:
                self.data = read_excel(self.sorPath)
                self.attribs = read_excel(self.setPath, header=None)
                self.data.dropna(1, "all", inplace=True)
                self.data.dropna(how='all', inplace=True)
                self.att_comb = self.attribs[0].dropna()
                self.att_sec = self.attribs[1].dropna()
                self.att_cols = self.attribs[2].dropna()
                self.lst_cols = list(self.att_cols)
                if len(self.data.columns) == len(self.att_cols):
                    self.data.columns = self.lst_cols
                else:
                    return "Column count loaded not matching with dataframe"
                self.data.dropna(subset=[self.lst_cols[0]], inplace=True)
                return True
            except Exception as e:
                return f"Error while loading settings \n {e}"
        else:
            return f"Not Found Error : {self.fileStat}"

    def rearrangeDF(self):
        try:
            for i in self.att_comb:
                self.dFrame = self.data[self.data[self.lst_cols[0]].isin(i.split("-"))].copy()
                self.data.drop(list(self.dFrame.index), inplace=True)
                self.dFrame.drop_duplicates(self.lst_cols[1], False, inplace=True)
                self.dFrame.loc[(list(self.dFrame.index)[-1]) + 1] = self.dFrame.loc[:, self.lst_cols[3]:].sum()
                self.dFrame.loc[self.dFrame[self.lst_cols[0]].isnull(), :self.lst_cols[0]] = "&".join(list(self.dFrame[self.lst_cols[0]].drop_duplicates().dropna()))
                self.dFrame.loc[self.dFrame[self.lst_cols[2]].isnull(), self.lst_cols[2]] = self.dFrame[self.lst_cols[1]].count()
                self.data = self.data.append(self.dFrame, ignore_index=True)
            for j in list(self.data[self.lst_cols[0]].drop_duplicates()):
                if not self.data.loc[(self.data[self.lst_cols[0]] == j) & (self.data[self.lst_cols[2]].isnull()), :].empty:
                    self.data.loc[(self.data[self.lst_cols[0]] == j) & (self.data[self.lst_cols[2]].isnull()), self.lst_cols[2]] = self.data.loc[self.data[self.lst_cols[0]] == j, self.lst_cols[1]].count()
            return True
        except Exception as e:
            return f"Error while rearranging & performing buisness logic \n {e}"

    def createSummary(self):
        try:
            data1 = self.data.copy()
            data1.dropna(inplace=True)
            groups = data1.groupby(self.lst_cols[0])
            self.data2 = groups.sum()
            self.data2.drop(self.lst_cols[1], 1, inplace=True)
            newCol = 'Count'
            self.data2.insert(0, newCol, "")
            for i in self.att_comb:
                rowName = i.replace("-", "&")
                self.data2.loc[rowName] = self.data2.loc[i.split("-")].sum()
                self.data2.loc[i.replace("-", "&"), newCol] = data1.loc[self.data[self.lst_cols[0]].isin(i.split("-")), :][self.lst_cols[1]].count()
                self.data2.drop(i.split("-"), inplace=True)
            self.data2 = self.data2.reindex(self.att_sec, axis=0)
            self.data2.dropna(how='all', inplace=True)
            for k in list(self.data2.index):
                if self.data2.loc[k, newCol] == "":
                    self.data2.loc[k, newCol] = data1.loc[self.data[self.lst_cols[0]] == k, :][self.lst_cols[1]].count()
            self.data2['Total Amt'] = self.data2[self.lst_cols[4]] + self.data2[self.lst_cols[6]]
            self.data2.loc["Grand Total"] = self.data2.iloc[:, :].sum()
            return True
        except Exception as e:
            return f"Error while creating summary logics \n {e}"

    def exportAsFile(self):
        try:
            #print(join(dirname(self.sorPath),f"OT_{datetime.now().month}_{datetime.now().year}.xlsx"))
            with ExcelWriter(join(dirname(self.sorPath),f"OT_{datetime.now().month}_{datetime.now().year}.xlsx")) as writer:
                self.data2.to_excel(writer, sheet_name='OT_Summary')
                self.data.to_excel(writer, sheet_name='OT', index=False)
            return True
        except Exception as e:
            return f"Error while exporting file \n {e}"

    def checkFilePaths(self):
        if exists(self.sorPath):
            if exists(self.setPath):
                return True
            else:
                return "Settings file not exist"
        else:
            return "Source file not exist"


    def setSettings(self, setPath):
        self.setPath = setPath

    def setSettings(self, sorPath):
        self.sorPath = sorPath

    def getDefaultPath(self):
        return join(environ['USERPROFILE'], "Desktop")


stat = OTStat("C:\\Users\\m.azad\\Desktop\\08_2019.xlsx")
res = stat.startCalculations()
if type(res) is bool:
    print("Sucessfully Finished")
else:
    print(f"Failed with {res}")