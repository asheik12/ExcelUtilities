import os
import shutil
import pandas as p
import datetime

def readRequire(sFile, cFile=None, oMon='Normal'):
    errs = "Source excel"       # Error with source file
    errr = "Read csv"           # Error with req file
    errf = "File error"         # Error when reading the source file
    msgs = "Sucess"             # Success Operation
    if cFile == None:           # Checking for req file path validation
        rFile = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        cFile = rFile + "\\Req.csv"
    if os.path.exists(sFile):   # Checking for source file path validation
        res = preRead(cFile)    # Reading the data with req file
        if res != None:
            col = res[4]        # Columns for dataframe
            pcol = res[6]       # Parse Columns for dataframe
            acol = res[5]       # Altered Column names for dataframe
            ecod = res[7]       # Excluded code numbers
            if oMon == 'Normal':    # Checking for type
                shifti = res[0]     # Loading available shift in
                shifto = res[1]     # Loading available shift out
                try:
                    data = p.read_excel(sFile, usecols=col, parse_dates=pcol)   # Reading excel file with settings
                    data[col[3]] = data[col[3]].astype("category")              # Compressing memory
                    data.columns = acol#[header.replace(" ","_") for header in col] # Loading columns for dataframe
                    data['Diff_In'] = data[acol[4]].sub(data[acol[6]])          # Deriving Diff In
                    data['Diff_Out'] = data[acol[5]].sub(data[acol[7]])         # Deriving Diff Out
                    data['SDiff'] = data[acol[6]] - data[acol[7]]               # Deriving SDiff
                    shftto = (datetime.datetime.strptime(shifto[1], "%H:%M:%S") + datetime.timedelta(hours=1)).time()
                    data.query(f"({acol[4]} != '{shifti[1]}' or {acol[5]} != '{shftto}') and (Code not in {ecod})", inplace = True)
                    ind = data.query(f"((Diff_In >= '-01:00:00') and (Diff_In <= '01:00:00')) and ({acol[4]} in {shifti} and {acol[5]} in {shifto})")
                    data.drop(ind.index, inplace=True)
                    sam = data.query("SDiff > '-01:00:00' and SDiff < '01:00:00'")
                    data.drop(sam.index, inplace=True)
                    sfil = [shifti[0], shifto[0]]
                    ibet = data.query(f"(Diff_In > '01:00:00' and Diff_In <= '06:00:00') and {acol[4]} in {sfil}")
                    data.drop(ibet.index, inplace=True)
                    data.sort_values([acol[1], acol[0]], inplace=True)
                    data[acol[0]] = data[acol[0]].dt.date               # Converts to date
                    data[acol[4]] = data[acol[4]].dt.time               # Converts to time
                    data[acol[5]] = data[acol[5]].dt.time
                    data[acol[6]] = data[acol[6]].dt.time
                    data[acol[7]] = data[acol[7]].dt.time
                    sec = list(data[acol[3]].drop_duplicates())         # remove duplicates from the list
                    rdFile = rFile + "\\Shift Changes " + str(data[acol[0]].min().day) + " to " + str(data[acol[0]].max().day)
                    typ = getExportType(res[-2], sec)
                    if os.path.exists(rdFile): shutil.rmtree(rdFile)
                    os.mkdir(rdFile)
                    exportData(data, rdFile, acol ,sec ,res[-2] , typ)
                    #['Date'0, 'Code'1, 'Name'2, 'Section'3, 'Shift_In'4, 'Shift_Out'5, 'Time_In'6, 'Time_Out'7]
                    print(data.shape)
                except Exception as e:
                    print(e)
                    return errf
            elif oMon == 'Ramadan':
                shifti = res[2]
                shifto = res[3]
        else:
            return errr         # Error thrown by req file
    else:
        return errs             # Error thrown by source file


# Reading the requirements from the file *.csv
def preRead(paths):
    vlist = ['NShiftI', 'NShiftO', 'RShiftI', 'RShiftO', 'LColumns', 'AColumns', 'PColumns', 'ECodes', 'CSection']
    if os.path.exists(paths):
        l = list()
        h = list()
        f = open(paths)
        for i in f:
            a = i.split(",")
            h.append(a[0])
            if len(a) > 1:
                a.pop(0)
                a.pop()
            l.append(a)
        f.close()
        l.append(h)
        if len(l) == 0 or vlist != l[-1]: return None
        else: return l
    else:
        return None

# Export to excel
def exportData(data, fName, cols, sec, res, typ):
    if typ:
        a = getAsDict(res)
        for i,v in a.items():
            fitm = [ct for ct in sec if ct in v]
            if len(fitm) != 0:
                wri = p.ExcelWriter(fName + "\\" + i + ".xlsx")
                for j in fitm:
                    edata = data.query(f"{cols[3]} == '{j}'")
                    edata.to_excel(wri, sheet_name=j, columns=cols, index=False)
                    wri.sheets[j].column_dimensions['A'].width = 11
                    wri.sheets[j].column_dimensions['C'].width = len(max(list(edata[cols[2]]), key = len))
                    wri.sheets[j].column_dimensions['D'].width = len(j)
                wri.save()
    else:
        for i in sec:
            wri = p.ExcelWriter(fName+"\\"+i+".xlsx")
            edata = data.query(f"{cols[3]} == '{i}'")
            edata.to_excel(wri, sheet_name=i, columns=cols, index=False)
            wri.sheets[i].column_dimensions['A'].width = 11
            wri.sheets[i].column_dimensions['C'].width = len(max(list(edata[cols[2]]), key = len))
            wri.sheets[i].column_dimensions['D'].width = len(i)
            wri.save()


#  method to determine the export type
def getExportType(filedata, dropdata):
    a = getAsDict(filedata)
    l = dictvaluesList(a)
    ls = [x for x in dropdata if x in l]
    if ls == dropdata:return True
    else: return False

# method to convert read data as dictionary
def getAsDict(filedata):
    dct = dict()
    for i in filedata:
        a = i.split("-")
        dct[a[0]] = a[1].split(";")
    return dct

# method to return dict keys
def dictheadersList(dta):
    return [k for k in dta.keys()]

# method to return values from dict by splitting
def dictvaluesList(dta):
    lst = list()
    for i in [v for v in dta.values()]:
        for j in i:
            lst.append(j)
    return lst

t1 = datetime.datetime.now()
print(readRequire("C:\\Users\\m.azad\\Desktop\\export.xlsx", oMon='Normal'))
t2 = datetime.datetime.now()
print(t2-t1)