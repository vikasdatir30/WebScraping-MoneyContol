###################################################################
# This class performs cleaning and formatting on stage file,
# adds year, month and unique id
###################################################################

import csv
from datetime import datetime as dt

class Transformsite:
    def __init__(self, stgloc, tgtloc='NA'):
        try:
            self.dstmp = dt.now().strftime("%Y-%m-%d_%H:%M:%S_%p")
            self.stginfile=stgloc
            #self.tgtoutfile=tgtloc+'\\IndexData_'+(str(self.dstmp).replace(':',''))+'.txt'
            self.tgtoutfile=tgtloc+'\\Tgt_IndexData.raw'
            self.rowid=0
            self.rowlst=['INDEX_ID','INDEX_NAME','PRICE','CHANGE_AMT','CHANGE_PERCENT','TIMESTAMP','YEAR','MONTH','DAY']
        except Exception as error:
            print ('Error in Transsite Module - INIT ' +str(error))

    def transdata(self):
        try:
            print(self.stginfile)
            with open (self.stginfile,'r', encoding='UTF-8') as self.finptr:
                with open(self.tgtoutfile,'w', encoding='UTF-8', newline='') as self.foutptr:
                    self.fin  = csv.reader(self.finptr, delimiter='|')
                    self.fout = csv.writer(self.foutptr, delimiter='|', quotechar=None)
                    next(self.fin)
                    self.fout.writerow(self.rowlst)
                    self.rowlst.clear()
                    for self.row in self.fin:
                        if self.row[0].isspace() == False:
                            self.rowid=self.rowid+1
                            self.rowlst.append(self.row[4][0:4]+self.row[4][5:7]+self.row[4][8:10]+self.row[4][11:13]+str(self.rowid))
                            self.rowlst.append(self.row[0])
                            self.rowlst.append(float(self.row[1]))
                            self.rowlst.append(float(self.row[2]))
                            self.rowlst.append(float(self.row[3]))
                            self.rowlst.append(self.row[4].replace('_',' '))
                            self.rowlst.append(self.row[4][0:4])
                            self.rowlst.append((dt.strptime(self.row[4][0:10],'%Y-%m-%d')).strftime("%B"))
                            self.rowlst.append(self.row[5])
                            self.fout.writerow(self.rowlst)
                            self.rowlst.clear()
                        #print(self.row[4][0:4]+self.row[4][5:7]+self.row[4][8:10]+self.row[4][11:13]+str(self.rowid))
                        #print(self.row[0])
                        #print(float(self.row[1]))
                        #print(float(self.row[2]))
                        #print(float(self.row[3]))
                        #print(self.row[4].replace('_',' '))
                        #print(self.row[4][0:4])
                        #print((dt.strptime(self.row[4][0:10],'%Y-%m-%d')).strftime("%B"))
                        #print(self.row[5])
        except Exception as error:
            print('Error in Transsite Module - TRANSDATA '+str(error))
    def gettgtfile(self):
        return self.tgtoutfile
