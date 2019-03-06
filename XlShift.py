import os
import pandas as p

def readRequire(sFile, rFile=None, oMon='Normal'):
    errs = "Source excel"
    errr = "Read csv"
    errf = "File error"
    msgs = "Sucess"
    rFile = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')+"\\Req.csv"
    if os.path.exists(sFile):
        res = preRead(rFile)
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
                    data['Department'] = data['Department'].astype("category")
                    data.columns = acol#[header.replace(" ","_") for header in col]
                    data['Diff_In'] = data[acol[4]].sub(data[acol[6]])
                    data['Diff_Out'] = data[acol[5]].sub(data[acol[7]])
                    data['SDiff'] = data[acol[6]] - data[acol[7]]
                    #r = data.query(f"(Shift_In != {shifti[1]} or Shift_Out != {shifto[1]}) and (Code in {ecod})")
                    #print(r)
                except Exception as e:
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


print(readRequire("C:\\Users\\Sheik\\Desktop\\Test.xlsx", oMon='Normal'))