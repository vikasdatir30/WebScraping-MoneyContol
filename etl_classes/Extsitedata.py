##################################################
# This class use to extract data from the main
# page of the moneycontrol website
# This will create a file to store raw output data
##################################################

import requests as req
import pandas as pd
from datetime import datetime as dt
from datetime import date as d

class Extractsite:
    def __init__(self, stgloc):
        try:
            self.URL= 'https://www.moneycontrol.com/'
            self.dttime=dt.now().strftime("%Y-%m-%d_%H:%M:%S")
            self.dtday =d.today().strftime("%a")
            self.stgfile =stgloc+'Stg_IndexData.raw'
        except Exception as error:
            print('Error in Extractsite - INIT '+ str(error))

    def getdata(self):
        try:
            #getting data from the web site
            self.rawdata = req.get(self.URL)
            self.dflist = pd.read_html(self.rawdata.text)
            self.rawdf = self.dflist[1]
            #print(self.rawdf)

            #adding timestamp part
            self.moddf=pd.DataFrame(self.rawdf.values.tolist(),columns=['Index', 'Price', 'ChgAmt', 'ChgPer'])
            self.moddf['Timestamp']=self.dttime
            self.moddf['Day']=self.dtday

            #writing data to text file
            self.moddf.to_csv(self.stgfile, mode='w',sep='|', encoding='UTF-8',na_rep='NULL', index=False, header=True)
        except Exception as error:
            print('Error in Extractsite - STOREDATA '+ str(error))
