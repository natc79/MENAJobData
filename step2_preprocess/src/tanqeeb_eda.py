import re
import string
import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#import seaborn as sns
import os, json
from collections import Counter
from config import FileConfig
import pprint
import datetime
import sqlite3
from pymongo import MongoClient
import datetime
from bson.objectid import ObjectId
currdate = datetime.datetime.now()
import sqlite3
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup


class TanqeebEDA(BasePreprocessor):
    """Code for analyzing tanqeeb cvs."""
    
    
    def __init__(self):
        """Initialize"""
        
        self.extdir = os.path.join(FileConfig.EXTDIR,'tanqeeb')
        self.outdir = os.path.join(FileConfig.EXTDIR,'tanqeeb')
        self.figdir = os.path.join(FileConfig.FIGDIR,'tanqeeb')
        self.conn = sqlite3.connect(os.path.join(self.outdir,"tanqeebcv.db"), timeout=10)
        self.cursor = self.conn.cursor()
        
        
        client = MongoClient('localhost', 27017)
        self.db = client['tanqeeb']
        #self.set_mappings()
    
    def compute_stats(self, df):
        """Compute stats on information filled out and available in the CVs"""
        
        df['age'] = [np.floor((datetime.datetime.now()-row['Birth Date']).days/365) for i, row in df.iterrows()]
        countries = list(set(df['Nationality']))
        
        # document non-missing
        avail = df.notnull().sum()/len(df)*100
        avail.sort_values(inplace=True)
        params = {
            "xtitle":"Percent Available", 
            "ytitle":"Resume Field", 
            "title":"Availability of Resume Fields\nData: %s (%d Observations)" % ('Tanqeeb', len(df)),
            "filename": "percent_resumes_available_all.png" 
         }
        self._graph_bar(avail.index, avail.values, params)
        
        for cty in countries:
            tempdf = df[df['Nationality']==cty]
            if len(tempdf) > 100:
                print(tempdf['Gender'].value_counts()/len(df))
        
                # document non-missing
                avail = tempdf.notnull().sum()/len(tempdf)*100
                avail.sort_values(inplace=True)
                params = {
                    "xtitle":"Percent Available", 
                    "ytitle":"Resume Field", 
                    "title":"Availability of Resume Fields\nCountry: %s\nData: %s (%d Observations)" % (cty, 'Tanqeeb', len(tempdf)),
                    "filename": "percent_resumes_available_%s.png" % (cty) 
                }
                self._graph_bar(avail.index, avail.values, params)
                
        # Months last active
        df['months_since_last_active'] = [month_diff(row['last_active'], datetime.datetime.now()) if pd.isnull(row['last_active']) is False else -1 for i, row in df.iterrows()]
        # generate percent active over different time periods 
        temp = df['months_since_last_active'].value_counts()
        print(temp.values)
        last_active = []
        for i in [1, 3, 6, 12]:
            last_active.append([i, np.sum(temp[(temp.index <= i) & (temp.index >= 0)])/np.sum(temp)])
        temp = pd.DataFrame(last_active, columns=['Active in Last Months', 'Number of Users']) 
        params = {
                    "xtitle":"Percent Active", 
                    "ytitle":"In Last Months", 
                    "title":"Percent of Active Users\nData: %s" % ('Tanqeeb'),
                    "filename": "percent_active_users_%s.png"
        }
        self._graph_bar(temp['Active in Last Months'], temp['Number of Users'], params)
        return(df)
    
    def document_users(self, df):
        """Get general statistics on users"""
        
        query = """SELECT DISTINCT country, id FROM resumelinks;"""
        tempdf = pd.read_sql(query, self.conn)
        print(tempdf.head())
        df['id'] = df['_id']
        df = tempdf.merge(df, how='left', on=['id'])
        print(len(df))
        df['female'] = [1 if row['Gender'] == 'Female' else 0 for i, row in df.iterrows()]
        df['employed'] = [1 if ~pd.isnull(row['jobstatus']) and str(row['jobstatus']).strip() == 'Working but looking for new opportunities' else 0 for i, row in df.iterrows()]
        
        aggregation = {
            'age':{
                'age_mean':'mean',
                'age_std':'std'
            },
            'female':'mean',
            'employed':'mean'
        }
        
        stats = df.groupby(['country']).agg(aggregation)
        stats.to_csv(os.path.join(FileConfig.INTDIR,'tanqeeb','resume_stats.csv'))
        print(stats)
    
    def extract_from_mongodb(self):
        """Extract data from mongodb and format it for analysis."""
        
        resumes = self.db['resumes']

        results = self.db.resumes.find({"error":{"$exists": False}})
        df = pd.DataFrame(list(results))
        print(df.head())
        re1 = re.compile('(\d+)\-(\d+)\-(\d+)')
        df['Birth Date'] = [row['Birth Date'] if re1.match(str(row['Birth Date'])) is not None else np.nan for i, row in df.iterrows()]
        df['Birth Date'] = pd.to_datetime(df['Birth Date'], errors='ignore')
        df['Marital Status'] = [np.nan if row['Marital Status'] == '-' else row['Marital Status'] for i, row in df.iterrows()]
        print(len(df))
        print(df.head())
        print(df.dtypes)
        df.to_csv(os.path.join(self.extdir,'csv','resumes.csv'), index=False)
        return(df)
    
    
    def get_valid_resumes(self, dfmd):
        """Get valid resumes and cross-join with the other data."""
        
        # check the data in the db
        query = """SELECT country, srchtitle, COUNT(id) as cnt
            FROM resumelinks 
            WHERE srchtitle IN ('Engineering','Marketing','Sales')
            GROUP BY country, srchtitle
            ORDER BY country, cnt DESC
            ;"""
        df = pd.read_sql(query, self.conn)
        
        query = """SELECT DISTINCT id, country, srchtitle FROM resumelinks WHERE srchtitle IN ('Engineering','Marketing','Sales');"""
        df = pd.read_sql(query, self.conn)
        print(len(df))
        
        dfmd['id'] = dfmd['_id']
        df = df.merge(dfmd, how='left', on=['id'])
        print(len(df))
        df = df[(~pd.isnull(df['education'])) & (~pd.isnull(df['experiences'])) & (~pd.isnull(df['skills']))]
        print(len(df))
        df.to_csv(os.path.join(FileConfig.INTDIR,'tanqeeb','resumes.csv'))

        
    def run_all():
        df = self.extract_from_mongodb()
        df = self.compute_stats(df)
        
if __name__ == "__main__":

    te = TanqeebEDA()
    te.run_all()