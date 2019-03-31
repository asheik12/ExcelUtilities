import shutil
from ConnUtilities import ConUtil
from SpicaEvents import SpicaEvents
from os.path import exists, isfile, join, dirname
from os import mkdir
from pandas import read_excel, DataFrame, merge, to_datetime, ExcelWriter
from datetime import datetime, timedelta


class ShiftChanges:


    def __init__(self, sFilePath=None, sorFile=None):
        self.conn = ConUtil()
        if (sFilePath == None):
            self.sFilePath = join(self.conn.getDefaultPath(), 'Req.csv')
        elif (not isfile(sFilePath)):
            self.sFilePath = join(self.conn.getDefaultPath(), 'Req.csv')
        else:
            self.sFilePath = sFilePath
        if (sorFile == None):
            self.sorFile = join(self.conn.getDefaultPath(), 'export.xlsx')
        elif (not isfile(sorFile)):
            self.sorFile = join(self.conn.getDefaultPath(), 'export.xlsx')
        else:
            self.sorFile = sorFile
        self.fStatus = self.loadProperties()

    def findShiftChanges(self, types='N'):
        if type(self.fStatus) is bool:
            if types == 'N':
                nShiftOp = self.performNormalShiftOperation()
                if type(nShiftOp) is DataFrame:
                    spicaEvents = SpicaEvents()
                    self.spicaEve = spicaEvents.spicaPunchings(nShiftOp[self.aCol[0]].min().date(), nShiftOp[self.aCol[0]].max().date())
                    if type(self.spicaEve) is DataFrame:
                        compData = self.compareWithSpica(nShiftOp, self.spicaEve)
                        if type(compData) is DataFrame:
                            export = self.exportData(compData)
                            if type(export) is bool:
                                return True
                            else:
                                return export
                        else:
                            return compData
                    else:
                        sfil = [self.nSh_In[0], self.nSh_Out[0]]
                        nfil = [self.nSh_In[3], self.nSh_Out[2]]
                        npostShift = self.postDataOperation(nShiftOp, sfil, nfil)
                        export = self.exportData(npostShift)
                        if type(export) is bool:
                            return "Sucessfully Exported Without comparing SPICA"
                        else:
                            return export
                else:
                    return nShiftOp

            elif types == 'R':
                pass
            else:
                return "Type not available"
        else:
            return self.fStatus

    def performNormalShiftOperation(self):
        try:
            data = read_excel(self.sorFile, usecols=self.lCol, parse_dates=self.pCol)
            data[self.lCol[3]] = data[self.lCol[3]].astype("category")  # Compressing memory
            data.columns = self.aCol  # [header.replace(" ","_") for header in col] # Loading columns for dataframe
            data['Diff_In'] = data[self.aCol[4]].sub(data[self.aCol[6]])  # Deriving Diff In
            data['Diff_Out'] = data[self.aCol[5]].sub(data[self.aCol[7]])  # Deriving Diff Out
            data['SDiff'] = data[self.aCol[6]] - data[self.aCol[7]]  # Deriving SDiff
            shftto = (datetime.strptime(self.nSh_Out[1], "%H:%M:%S") + timedelta(hours=1)).time()
            data.query(f"({self.aCol[4]} != '{self.nSh_In[1]}' or {self.aCol[5]} != '{shftto}') and ({self.aCol[1]} not in {self.eCode})", inplace=True)
            ind = data.query(f"((Diff_In >= '-01:00:00') and (Diff_In <= '01:00:00')) and ({self.aCol[4]} in {self.nSh_In} and {self.aCol[5]} in {self.nSh_Out})")
            data.drop(ind.index, inplace=True)
            sam = data.query("SDiff > '-01:00:00' and SDiff < '01:00:00'")
            data.drop(sam.index, inplace=True)
            data.sort_values([self.aCol[1], self.aCol[0]], inplace=True)

            return data

        except Exception as e:
            return f"Error while performing shift operations \n {e}"


    def compareWithSpica(self, sap, spica):
        try:
            sap = sap[[self.aCol[1], self.aCol[0], self.aCol[2], self.aCol[3], self.aCol[4], self.aCol[5], self.aCol[6], self.aCol[7], sap.columns[8]]]
            sapspica = merge(sap, spica, left_on=[sap.columns[0], sap.columns[1]], right_on=[spica.columns[0], spica.columns[1]])
            sapspica.loc[:,:] = sapspica.loc[sapspica.duplicated([self.aCol[1], self.aCol[0]], keep=False), :]
            sapspica.drop([spica.columns[0], spica.columns[1]], axis=1, inplace=True)
            sapspica['Diff_In'] = to_datetime(sapspica[self.aCol[4]].astype('str')) - to_datetime(sapspica['Arrival'].astype('str'))

            self.twoshifts = sapspica.query(f"((Diff_In >= '-01:00:00') and (Diff_In <= '01:00:00')) and ({self.aCol[4]} in {self.nSh_In} and {self.aCol[5]} in {self.nSh_Out})")
            for row, col in self.twoshifts.iterrows():
                sap.drop(sap.loc[(sap[self.aCol[1]] == col[self.aCol[1]]) & (sap[self.aCol[0]] == col[self.aCol[0]]), :].index, inplace=True)

            sfil = [self.nSh_In[0], self.nSh_Out[0]]
            self.twoshifts1 = sapspica.query(f"(Diff_In > '01:00:00' and Diff_In <= '06:00:00') and {self.aCol[4]} in {sfil}")
            for row, col in self.twoshifts1.iterrows():
                sap.drop(sap[(sap[self.aCol[1]] == col[self.aCol[1]]) & (sap[self.aCol[0]] == col[self.aCol[0]])].index, inplace=True)

            nfil = [self.nSh_In[3], self.nSh_Out[2]]
            self.twoshifts2 = sapspica.query(f"(Diff_In > '01:00:00' and Diff_In < '03:00:00') and {self.aCol[4]} in {nfil}")
            for row, col in self.twoshifts2.iterrows():
                sap.drop(sap[(sap[self.aCol[1]] == col[self.aCol[1]]) & (sap[self.aCol[0]] == col[self.aCol[0]])].index, inplace=True)

            return self.postDataOperation(sap, sfil, nfil)
        except Exception as e:
            return f"Error when merging sap & spica datas \n {e}"

    def postDataOperation(self, sap, sfil, nfil):
        self.exc1 = sap.query(f"(Diff_In > '01:00:00' and Diff_In <= '06:00:00') and {self.aCol[4]} in {sfil}")
        sap.drop(self.exc1.index, inplace=True)

        self.exc2 = sap.query(f"(Diff_In > '01:00:00' and Diff_In < '03:00:00') and {self.aCol[4]} in {nfil}")
        sap.drop(self.exc2.index, inplace=True)

        sap[self.aCol[0]] = sap[self.aCol[0]].dt.date
        sap[self.aCol[4]] = sap[self.aCol[4]].dt.time
        sap[self.aCol[5]] = sap[self.aCol[5]].dt.time
        sap[self.aCol[6]] = sap[self.aCol[6]].dt.time
        sap[self.aCol[7]] = sap[self.aCol[7]].dt.time
        print(sap.shape)
        return sap

    def loadProperties(self):
        self.fData = self.conn.readFile(self.sFilePath)
        if type(self.fData) is list:
            try:
                self.propHead = self.fData[-1].split(",")
                self.fData.pop()
                if len(self.propHead) == len(self.fData):
                    for heads in self.propHead:
                        if len([dts for dts in self.fData if dts.split(",")[0] == heads]) == 0:
                            return "Properties not in correct format"
                    self.nSh_In = [i for i in self.fData[0].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.nSh_Out = [i for i in self.fData[1].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.rSh_In = [i for i in self.fData[2].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.rSh_Out = [i for i in self.fData[3].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.lCol = [i for i in self.fData[4].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.aCol = [i for i in self.fData[5].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.pCol = [i for i in self.fData[6].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.eCode = [i for i in self.fData[7].split(",") if (i not in self.propHead) & (i != "\n")]
                    self.cSec = [i for i in self.fData[8].split(",") if (i not in self.propHead) & (i != "\n")]
                    return True
                else:
                    return "Properties Mismatch"
            except Exception as e:
                return f"Error reading file \n {e}"
        else:
            return f"Error reading file or file not exists \n {self.sFilePath}"

    def exportData(self, data):
        try:
            sec = list(data[self.aCol[3]].drop_duplicates())  # remove duplicates from the list
            rdFile = join(dirname(self.sorFile), "Shift Changes " + str(data[self.aCol[0]].min().day) + " to " + str(data[self.aCol[0]].max().day))
            typ = self.getExportType(self.cSec, sec)
            if exists(rdFile): shutil.rmtree(rdFile)
            mkdir(rdFile)

            if typ:
                a = self.getAsDict(self.cSec)
                for i, v in a.items():
                    fitm = [ct for ct in sec if ct in v]
                    if len(fitm) != 0:
                        wri = ExcelWriter(join(rdFile, i + ".xlsx"))
                        for j in fitm:
                            edata = data.query(f"{self.aCol[3]} == '{j}'")
                            edata.to_excel(wri, sheet_name=j, columns=self.aCol, index=False)
                            wri.sheets[j].column_dimensions['A'].width = 11
                            wri.sheets[j].column_dimensions['C'].width = len(max(list(edata[self.aCol[2]]), key=len))
                            wri.sheets[j].column_dimensions['D'].width = len(j)
                        wri.save()
            else:
                for i in sec:
                    wri = ExcelWriter(join(rdFile, i + ".xlsx"))
                    edata = data.query(f"{self.aCol[3]} == '{i}'")
                    edata.to_excel(wri, sheet_name=i, columns=self.aCol, index=False)
                    wri.sheets[i].column_dimensions['A'].width = 11
                    wri.sheets[i].column_dimensions['C'].width = len(max(list(edata[self.aCol[2]]), key=len))
                    wri.sheets[i].column_dimensions['D'].width = len(i)
                    wri.save()

            other = ExcelWriter(join(rdFile, "Others.xlsx"))
            if type(self.spicaEve) is DataFrame:
                if not self.twoshifts.empty: self.twoshifts.to_excel(other, sheet_name="Two Shifts", columns=self.aCol, index=False)
                if not self.twoshifts1.empty: self.twoshifts1.to_excel(other, sheet_name="Two Shifts 1", columns=self.aCol, index=False)
                if not self.twoshifts2.empty: self.twoshifts2.to_excel(other, sheet_name="Two Shifts", columns=self.aCol, index=False)
            if not self.exc1.empty: self.exc1.to_excel(other, sheet_name="Check 1", columns=self.aCol, index=False)
            if not self.exc2.empty: self.exc2.to_excel(other, sheet_name="Check 2", columns=self.aCol, index=False)
            other.save()

            return True

        except Exception as e:
            return f"Error when trying to export file \n {e}"

    #  method to determine the export type
    def getExportType(self,filedata, dropdata):
        a = self.getAsDict(filedata)
        l = self.dictvaluesList(a)
        ls = [x for x in dropdata if x in l]
        if ls == dropdata:
            return True
        else:
            return False

    # method to convert read data as dictionary
    def getAsDict(self, filedata):
        dct = dict()
        for i in filedata:
            a = i.split("-")
            dct[a[0]] = a[1].split(";")
        return dct

    # method to return dict keys
    def dictheadersList(self, dta):
        return [k for k in dta.keys()]

    # method to return values from dict by splitting
    def dictvaluesList(self, dta):
        lst = list()
        for i in [v for v in dta.values()]:
            for j in i:
                lst.append(j)
        return lst


sChanges = ShiftChanges()
result = sChanges.findShiftChanges()
if type(result) is bool:
    print("Sucessfully Exported")
else:
    print(result)