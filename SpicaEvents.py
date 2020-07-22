from DataFetch import DataFetch
from pandas import DataFrame, to_datetime, DateOffset
from datetime import time, datetime, date
from os.path import join



class SpicaEvents:

    def __init__(self):
        self.dataFetch = DataFetch()

    def spicaPunchings(self, start, end):
        self.data = self.dataFetch.fetchAllDB(start, end)
        if type(self.data) is DataFrame:
            lcol = self.loadColumnsNames()
            if type(lcol) is bool:
                try:
                    self.data.query(f"({self.event} == 268436546) or ({self.event} == 268436547) or ({self.event} == 1090) or ({self.event} == 1091)", inplace=True)
                    self.data.sort_values([self.code, self.date], inplace=True)
                    self.data['TIME'] = to_datetime(self.data[self.date]).dt.time
                    self.data[self.date] = to_datetime(to_datetime(self.data[self.date]).dt.date)
                    self.data.drop_duplicates(keep='last', inplace=True)
                    # Start - Updated Code on 22.07.2020
                    # mask = (self.data[self.time] < time(6, 10, 0)) & (self.data[self.status] == self.dep)  // Old Working Code
                    # self.data.loc[mask, self.date] = self.data[self.date] - DateOffset(days=1)             // Old Working Code   
                    def func1(sel):
                        if ((sel[self.time] < time(6,10,0)) & (sel[self.status] == self.dep)):
                            dval = self.data[(self.data[self.date] == sel[self.date]) & (self.data[self.code] == sel[self.code])]
                            if dval[(dval[self.status] == self.arr) & (dval[self.time] <= sel[self.time])].empty:
                                sel[self.date] = sel[self.date] - DateOffset(days=1)
                        return sel     
                    self.data = self.data.apply(func1, axis=1)
                    self.data[self.date] = self.data[self.date].dt.date 
                    self.data = self.data[self.data[self.date] != (start - DateOffset(days=1)).date()]
                    # self.data = self.data[self.data[self.date] != datetime(2019, 3, 14)]                     // Old Code  
                    # End - Updated Code on 22.07.2020
                    self.dup = self.data[self.data[[self.code, self.date, self.status]].duplicated()]

                    for i, col in (self.dup[~self.dup[[self.code, self.date]].duplicated(keep=False)]).iterrows():
                        lst = [col[self.code], col[self.date]]
                        # Muliple Punches
                        if (len(self.data.loc[(self.data[self.code] == lst[0]) & (self.data[self.date] == lst[1]), :][self.status].value_counts().to_list()) > 1):
                            self.data.drop(self.data.loc[(self.data[self.code] == lst[0]) & (self.data[self.date] == lst[1]) & (self.data[self.status] == col[self.status]), :].drop_duplicates([self.code, self.date, self.status]).index, inplace=True)
                        # 2 Same Punches
                        samePun = self.data.loc[(self.data[self.code] == lst[0]) & (self.data[self.date] == lst[1]), :][self.status].value_counts()
                        if (len(samePun.to_list()) == 1):
                            if samePun.keys()[0] == self.arr:
                                ind = self.data.loc[(self.data[self.code] == lst[0]) & (self.data[self.date] == lst[1]), :self.time].max()
                                self.data.loc[(self.data[self.code] == ind[self.code]) & (self.data[self.date] == ind[self.date]) & (self.data[self.status] == ind[self.status]) & (self.data[self.time] == ind[self.time]), [self.event, self.status]] = [268436547, self.dep]
                            elif samePun.keys()[0] == self.dep:
                                ind = self.data.loc[(self.data[self.code] == lst[0]) & (self.data[self.date] == lst[1]), :self.time].min()
                                self.data.loc[(self.data[self.code] == ind[self.code]) & (self.data[self.date] == ind[self.date]) & (self.data[self.status] == ind[self.status]) & (self.data[self.time] == ind[self.time]), [self.event, self.status]] = [268436546, self.arr]

                    self.data.drop([self.name, self.event], axis=1, inplace=True)
                    d1 = self.data.drop_duplicates([self.code, self.date, self.status], keep='last')
                    d2 = self.data.drop_duplicates([self.code, self.date, self.status])
                    f1 = d1.pivot_table(self.time, [self.code, self.date], self.status, aggfunc="sum")
                    f2 = d2.pivot_table(self.time, [self.code, self.date], self.status, aggfunc="sum")
                    self.workdata = f1.append(f2)
                    self.workdata.reset_index(inplace=True)
                    self.workdata.drop_duplicates([self.code, self.date, self.arr, self.dep], inplace=True)
                    self.workdata[self.code] = self.workdata[self.code].astype('int')
                    #for i in self.workdata[self.workdata.duplicated([self.code, self.date])][[self.code, self.date]].iterrows():
                        #print(self.workdata[(self.workdata[self.code] == i[1].values[0]) & (self.workdata[self.date] == i[1].values[1])])

                    self.workdata.sort_values([self.code, self.date, self.arr])
                    return self.workdata
                except Exception as e:
                    return f"Error while working with datas \n {e}"
            else:
                return lcol
        else:
            return self.data

    def getIncorrectPunches(self, start, end):
        self.data = self.dataFetch.fetchAllDB(start, end)
        if type(self.data) is DataFrame:
            lcol = self.loadColumnsNames()
            if type(lcol) is bool:
                try:
                    self.data.insert(3, self.time, self.data[self.date].dt.time)
                    self.data[self.date] = self.data[self.date].dt.date
                    self.data.sort_values([self.code, self.date], inplace=True)
                    self.data.query(f"({self.event} == 268436546) or ({self.event} == 268436547) or ({self.event} == 1090) or ({self.event} == 1091)", inplace=True)
                    # Start - Updated Code on 22.07.2020
                    # mask = (self.data[self.time] < time(6, 10, 0)) & (self.data[self.status] == self.dep)             // Old Working Code
                    # self.data.loc[mask, self.date] = to_datetime(self.data[self.date] - DateOffset(days=1)).dt.date   // Old Working Code
                    def func1(sel):
                        if ((sel[self.time] < time(6,10,0)) & (sel[self.status] == self.dep)):
                            dval = self.data[(self.data[self.date] == sel[self.date]) & (self.data[self.code] == sel[self.code])]
                            if dval[(dval[self.status] == self.arr) & (dval[self.time] <= sel[self.time])].empty:
                                sel[self.date] = sel[self.date] - DateOffset(days=1)
                        return sel     
                    self.data = self.data.apply(func1, axis=1)
                    self.data[self.date] = self.data[self.date].dt.date
                    # End - Updated Code on 22.07.2020
                    self.data = self.data[(self.data[self.date] != (start - DateOffset(days=1)).date()) & (self.data[self.date] != (end + DateOffset(days=1)).date())]
                    
                    # Filtering punches starts
                    self.data = self.data.pivot_table(values=self.time, index=[self.code, self.date], columns=self.status, aggfunc=lambda x: " ".join(str(i) for i in x))
                    self.data.reset_index(inplace=True)
                    self.data.fillna(" ", inplace=True)

                    def checkForNoValue(x):
                        if x != " ":
                            return int(len(str(x).split(" ")))
                        else:
                            return int(0)
                    self.data[self.arr+self.count] = self.data[self.arr].apply(checkForNoValue)
                    self.data[self.dep+self.count] = self.data[self.dep].apply(checkForNoValue)
                    self.data = self.data[(self.data[self.arr+self.count] != 1) | (self.data[self.dep+self.count] != 1)]
                    return self.data
                except Exception as e:
                    return f"Error While performing operation \n {e}"
            else:
                return lcol
        else:
            return self.data

    def getMultiplePunches(self, df):
        try:
            return df.query(f"({self.arr + self.count} != 0 and {self.dep + self.count} != 0) and ({self.arr + self.count} != 2 or {self.dep + self.count} !=2)")
        except Exception as e:
            return f"Error while finding multiple punches \n {e}"

    def getSamePunches(self, df):
        try:
            return df.query(f"({self.arr + self.count} == 0 or {self.dep + self.count} == 0) and ({self.arr + self.count} > 1 or {self.dep + self.count}  > 1)")
        except Exception as e:
            return f"Error while finding same punches \n {e}"

    def getSinglePunches(self, df):
        try:
            return df.query(f"({self.arr + self.count} == 0 or {self.dep + self.count} == 0) and ({self.arr + self.count} == 1 or {self.dep + self.count}  == 1)")
        except Exception as e:
            return f"Error while finding single punches \n {e}"

    def exportIncorrectPunches(self, start, end):
        inpun = self.getIncorrectPunches(start, end)
        if type(inpun) is DataFrame:
            mulpun = self.getMultiplePunches(inpun)
            sampun = self.getSamePunches(inpun)
            sinpun = self.getSinglePunches(inpun)
            if (type(mulpun) is DataFrame) and (type(sampun) is DataFrame) and (type(sinpun) is DataFrame):
                try:
                    colName = f"{'{:^12}'.format(self.date)} | {'{:^6}'.format(self.code)} | {'{:^3}'.format(self.arr[:1] + self.count[1:2])} | {'{:^3}'.format(self.dep[:1] + self.count[1:2])} | {'{:^17}'.format(self.arr)} | {'{:^17}'.format(self.dep)}\n"
                    fpunch = open(join(self.dataFetch.con.getDefaultPath(), "Punches.txt"), 'w')
                    if not mulpun.empty:
                        fpunch.write("Multiple Punches \n")
                        fpunch.write(colName)
                        fpunch.write('-' * len(colName) + "\n")
                        for i, k in mulpun.iterrows():
                            fpunch.write(f"{'{:^12}'.format(str(k[self.date]))} | {'{:^6}'.format(k[self.code])} | {'{:^3}'.format(k[self.arr + self.count])} | {'{:^3}'.format(k[self.dep + self.count])} | {'{:^17}'.format(k[self.arr])} | {'{:^17}'.format(k[self.dep])} \n")
                        fpunch.write("\n")
                    if not sampun.empty:
                        fpunch.write("Same Punches \n")
                        fpunch.write(colName)
                        fpunch.write('-' * len(colName) + "\n")
                        for i, k in sampun.iterrows():
                            fpunch.write(f"{'{:^12}'.format(str(k[self.date]))} | {'{:^6}'.format(k[self.code])} | {'{:^3}'.format(k[self.arr + self.count])} | {'{:^3}'.format(k[self.dep + self.count])} | {'{:^17}'.format(k[self.arr])} | {'{:^17}'.format(k[self.dep])} \n")
                        fpunch.write("\n")
                    if not sinpun.empty:
                        fpunch.write("Single Punches \n")
                        fpunch.write(colName)
                        fpunch.write('-' * len(colName) + "\n")
                        for i, k in sinpun.iterrows():
                            fpunch.write(f"{'{:^12}'.format(str(k[self.date]))} | {'{:^6}'.format(k[self.code])} | {'{:^3}'.format(k[self.arr + self.count])} | {'{:^3}'.format(k[self.dep + self.count])} | {'{:^17}'.format(k[self.arr])} | {'{:^17}'.format(k[self.dep])} \n")
                        fpunch.write("\n")

                    fpunch.close()
                    return True
                except Exception as e:
                    return f"Error exporting file \n {e}"
            else:
                return "Error calculating punches multiple or same or single"
        else:
            return inpun

    def checkForNoValue(self, x):
        if x != " ":
            return int(len(str(x).split(" ")))
        else:
            return int(0)

    def loadColumnsNames(self):
        try:
            self.query = self.dataFetch.queries
            for i in self.query:
                if i.split('#')[0] == self.dataFetch.qType:
                    dte = i.split("as ")
                    self.columns = [j.split(",")[0] for j in dte if ('#' not in j)]
                    self.columns.insert(-1, self.columns[-1].split(" ")[0])
                    self.columns.pop()
                    if len(self.columns) == 5:
                        self.code = self.columns[0]
                        self.name = self.columns[1]
                        self.date = self.columns[2]
                        self.event = self.columns[3]
                        self.status = self.columns[4]
                        self.time = 'TIME'
                        self.arr = 'Arrival'
                        self.dep = 'Departure'
                        self.count = '_Count'
                    else:
                        return "Problem loading columns names"
            return True
        except Exception as e:
            return f"Error while loading columns \n {e}"



# spiEve = SpicaEvents()
# res = spiEve.exportIncorrectPunches(date(2020,6,1), date(2020,6,30))
# if type(res) is bool:
#    print("Sucessfully Completed")
# else:
#    print(res)