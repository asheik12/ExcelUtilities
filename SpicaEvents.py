from DataFetch import DataFetch
from pandas import DataFrame, to_datetime, DateOffset
from datetime import time, datetime, date



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
                    mask = (self.data[self.time] < time(6, 10, 0)) & (self.data[self.status] == self.dep)
                    self.data.loc[mask, self.date] = self.data[self.date] - DateOffset(days=1)
                    self.data = self.data[self.data[self.date] != datetime(2019, 3, 14)]
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
                    else:
                        return "Problem loading columns names"
            return True
        except Exception as e:
            return f"Error while loading columns \n {e}"

