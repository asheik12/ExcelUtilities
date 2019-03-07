import os
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
                    ind = data.query(f"((Diff_In > '-01:00:00') and (Diff_In < '01:00:00')) and ({acol[4]} in {shifti} and {acol[5]} in {shifto})")
                    data.drop(ind.index, inplace=True)
                    sam = data.query("SDiff > '-01:00:00' and SDiff < '01:00:00'")
                    data.drop(sam.index, inplace=True)
                    sfil = [shifti[0], shifto[0]]
                    ibet = data.query(f"(Diff_In > '01:00:00' and Diff_In <= '06:00:00') and {acol[4]} in {sfil}")
                    data.drop(ibet.index, inplace=True)
                    data.sort_values([acol[1], acol[0]], inplace=True)
                    data[acol[0]] = data[acol[0]].dt.date
                    data[acol[4]] = data[acol[4]].dt.time
                    data[acol[5]] = data[acol[5]].dt.time
                    data[acol[6]] = data[acol[6]].dt.time
                    data[acol[7]] = data[acol[7]].dt.time
                    sec = list(data[acol[3]].drop_duplicates())
                    rdFile = rFile + "\\Shift Changes " + str(data[acol[0]].min().day) + " to " + str(data[acol[0]].max().day)
                    if not os.path.exists(rdFile): os.mkdir(rdFile)
                    for i in sec:
                        senFile = rdFile+"\\"+i+".xlsx"
                        exportData(data.query(f"{acol[3]} == '{i}'"), senFile, acol)
                    #['Date'0, 'Code'1, 'Name'2, 'Section'3, 'Shift_In'4, 'Shift_Out'5, 'Time_In'6, 'Time_Out'7]
                    print(data.shape)
                except Exception as e:
                    return errf
            elif oMon == 'Ramadan':
                shifti = res[2]
                shifto = res[3]
        else:
            return errr         # Error thrown by req file
    else:
        return errs             # Error thrown by source file



def preRead(paths):
    vlist = ['NShiftI', 'NShiftO', 'RShiftI', 'RShiftO', 'LColumns', 'AColumns', 'PColumns', 'ECodes']
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

def exportData(data, fName, cols):
    wri = p.ExcelWriter(fName)
    data.to_excel(wri, sheet_name="Shift_Difference", columns = cols,index=False)
    wri.sheets['Shift_Difference'].column_dimensions['A'].width = 12
    wri.sheets['Shift_Difference'].column_dimensions['C'].width = 25
    wri.sheets['Shift_Difference'].column_dimensions['D'].width = 20
    #wri.sheets['Shift_Difference'].
    wri.save()

readRequire("C:\\Users\\Sheik\\Desktop\\Test.xlsx", oMon='Normal')