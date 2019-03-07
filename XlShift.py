import os
import pandas as p

def readRequire(sFile, cFile=None, oMon='Normal'):
    errs = "Source excel"
    errr = "Read csv"
    errf = "File error"
    msgs = "Sucess"
    rFile = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    cFile = rFile + "\\Req.csv"
    if os.path.exists(sFile):
        res = preRead(cFile)
        if res != None:
            col = res[4]
            pcol = res[6]
            acol = res[5]
            ecod = res[7]
            if oMon == 'Normal':
                shifti = res[0]
                shifto = res[1]
                try:
                    data = p.read_excel(sFile, usecols=col, parse_dates=pcol)
                    data[col[3]] = data[col[3]].astype("category")
                    data.columns = acol#[header.replace(" ","_") for header in col]
                    data['Diff_In'] = data[acol[4]].sub(data[acol[6]])
                    data['Diff_Out'] = data[acol[5]].sub(data[acol[7]])
                    data['SDiff'] = data[acol[6]] - data[acol[7]]
                    data.query(f"({acol[4]} != '{shifti[1]}' or {acol[5]} != '{shifto[1]}') and (Code not in {ecod})", inplace = True)
                    ind = data.query(f"((Diff_In > '-01:00:00') and (Diff_In < '01:00:00')) and ({acol[4]} in {shifti} and {acol[5]} in {shifto})")
                    data.drop(ind.index, inplace=True)
                    sam = data.query("SDiff > '-01:00:00' and SDiff < '01:00:00'")
                    data.drop(sam.index, inplace=True)
                    sfil = [shifti[0], shifto[0]]
                    ibet = data.query(f"(Diff_In > '01:00:00' and Diff_In <= '06:00:00') and {acol[4]} in {sfil}")
                    data.drop(ibet.index, inplace=True)
                    data.sort_values([acol[1], acol[0]], inplace=True)
                    sec = list(data[acol[3]].drop_duplicates())
                    rdFile = rFile + "\\Shift_Changes"
                    if not os.path.exists(rdFile): os.mkdir(rdFile)
                    for i in sec:
                        senFile = rdFile+"\\"+i+".xlsx"
                        exportData(data.query(f"{acol[3]} == '{i}'"), senFile)
                    #['Date'0, 'Code'1, 'Name'2, 'Section'3, 'Shift_In'4, 'Shift_Out'5, 'Time_In'6, 'Time_Out'7]
                except Exception as e:
                    print(e)
                    return e
            elif oMon == 'Ramadan':
                shifti = res[2]
                shifto = res[3]
        else:
            return errr
    else:
        return errs



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

def exportData(data, fName):
    wri = p.ExcelWriter(fName)
    data.to_excel(wri, sheet_name="Shift_Difference", index=False)
    wri.save()

readRequire("C:\\Users\\m.azad\\Desktop\\Test.xlsx", oMon='Normal')