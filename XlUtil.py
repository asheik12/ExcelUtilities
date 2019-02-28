from os import path
from openpyxl import load_workbook

# function to read excel file and to return as list of dictionary
# arguments passed fName as String & headers as boolean

## If the headers is True considers the first record as header
## If the headers is False creates the own header by default

def retAsList(fName, headers):

    # Checking for file exist
    if path.exists(str(fName)):
        wb = load_workbook(fName, read_only=True)
        ws = wb.active
        ln = list()

        # Reading all the records from excel file
        for rs in ws.values:
            if len(rs) != rs.count(None) and len(rs) != rs.count(None)+1:
                ln.append(rs)

        # Checking number of records
        if len(ln) < 2: return None

        # Checking for headers & Naming headers that have None values
        if headers:
                header = ln[0]
                if None in header:
                    status = True
                    cheader = list(header)
                    while(status):
                       if None in cheader: cheader[cheader.index(None)] = "Column " + str(cheader.index(None)+1)
                       else : status = False
                    header = tuple(cheader)
        else:
                header = tuple(["Column "+str(i+1) for i in range(len(ln[0]))])

        # Deleting header from list
        ln.pop(0)

        # Converting datas to list of dictionary
        result = list()
        for data in ln:
            result.append(dict(zip(header, data)))

        return result
    else:
        return None