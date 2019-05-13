"""
# Analyze OLX Data Scraped from Website
# This was created for the paper "Improving Labor Market Matching in Egypt".

#The data is subsequently output into a csv file and processed via google translate
#NOTE:  This version is linked to the the data scraped in the cloud (as it was hard to figure out how to install googletrans on the cloud)

Created: Natalie Chun (December 2017)
Updated: May 2019
"""

from pytz import timezone
import sqlite3
import pandas as pd
import numpy as np
import re
import os
import datetime
import time
import csv
from config import FileConfig
from basepreprocess import BasePreprocessor

class OLXPreprocessor(BasePreprocessor):

    def __init__(self):
        self.extdir = os.path.join(FileConfig.EXTDIR, 'olx')
        self.conn = sqlite3.connect(os.path.join(self.extdir, 'OLX.db'))
        self.cursor = self.conn.cursor()
        #self.unprocdata = self._combine_data()
        self.tz = timezone('Africa/Cairo')
        self.datecur = datetime.datetime.now(self.tz)
        self.figdir = os.path.join(FileConfig.FIGDIR, 'olx')
        self.datasrc = 'OLX'
        
    def _combine_data(self):
        
        #first combine the relevant datasets
        query = '''SELECT a.downloaddate, a.downloadtime, b.region, b.subregion, b.jobsector, a.postdate, a.posttime, a.uniqueadid, b.i_photo, b.i_featured, a.pageviews, a.title, a.experiencelevel, a.educationlevel, a.type, a.employtype, a.compensation, a.description, a.textlanguage, a.userhref, a.username, a.userjoinyear, a.userjoinmt, a.emailavail, a.phoneavail, a.adstatus FROM jobadpagedata a LEFT JOIN jobadpageurls b ON a.uniqueadid = b.uniqueadid AND a.postdate = b.postdate;'''
        unprocdata = pd.read_sql(query,conn,parse_dates=['postdate','downloaddate'])

        #now get any data that might exists in the various csv files that have been archived
        archivedfiles = []
        if len(archivedfiles) > 0:
            for date in archivedfiles:
                filename = 'archivedpagedata_'+date+'.csv'
                tempdata = pd.read_csv(filename,parse_dates=['postdate','downloaddate'])
                # NOTE:  append tempdata to the unprocdata file maybe have to check that the data is in same format
                unprocdata = unprocdata.append(tempdata,ignore_index=True)
        
        c.close()
        print("Number of distinct entries: {}".format(len(unprocdata)))
        print(unprocdata.dtypes)
        print(unprocdata.head())
        print(unprocdata['postdate'].value_counts())
        print(unprocdata['educationlevel'].value_counts())
        print(unprocdata['experiencelevel'].value_counts())
        print(unprocdata['type'].value_counts())
        print(unprocdata['employtype'].value_counts())
        print(unprocdata['userjoinyear'].value_counts())
        print(unprocdata['adstatus'].value_counts())
        print(unprocdata['i_photo'].value_counts())
        print(unprocdata['i_featured'].value_counts())
        print("Memory Usage (mb): {}".format(round(unprocdata.memory_usage(deep=True).sum()/1048576,2)))
        return(unprocdata)
     
    def _create_features(self, df):
        """Create general features for data"""
        
        # Eliminate people who are posting job wanted ads
        df = df[(data['jobsector'] != 'Jobs Wanted') & (data['type'] != 'Job Seeker')]
        
        # date operations
        data['daysposted'] = [(row['downloaddate']-row['postdate']).days for i, row in df.iterrows()] 
        data['month'] = [datetime.date(val.year,val.month,1) for val in data['postdate']]
        
        # Fix unreasonable compensations
        df['compensation'] = [float(str(row['compensation']).replace(',','')) if row != np.nan else np.nan for i, row in df.iterrows()]
        catvars = ['experiencelevel', 'employtype', 'type', 'educationlevel']
        df, dcols = self._create_dummies(df, catvars)
        cols = ['daysposted', 'compensation','i_featured','i_photo','pageviews','phoneavail','emailavail'] + dcols
        return(df, catvars)
    
    def clean_data(self, data):
        translator = googletrans.Translator()
        #print(data['description'].value_counts())
        # TODO:  See if can correct this so can translate the description data into English
        #data['description_english'] = [translator.translate(desc).text if translator.translate(desc) is not None else '' for desc in data['description']]
        #print(data['description_english'].head(50))
        regex1= re.compile(r'[^0-9a-zA-Z]+')
        data['educationlevel'] = [regex1.sub('',row['educationlevel']) if type(row['educationlevel']) == str else row['educationlevel'] for i, row in data.iterrows()]
        data['bachelor_degree'] = [1 if row in ['BachelorsDegree','MastersDegree','PhD'] else 0 for row in data['educationlevel']]
        data['fulltime'] = [1 if row in ['Full-time'] else 0 for row in data['employtype']]
        data['comp'] = [float(str(row).replace(',','')) if row != np.nan else np.nan for row in data['compensation']]
        data['exp_management'] = [1 if row in ['Management','Executive/Director','Senior Executive (President, CEO)'] else 0 for row in data['experiencelevel']]
        data['exp_entrylevel'] = [1 if row in ['Entry level'] else 0 for row in data['experiencelevel']]
        data['has_comp'] = [1 if np.isnan(row['comp']) == False else 0 for i, row in data.iterrows()]
        data['has_credible_comp'] = [1 if row['has_comp'] == 1 and row['comp'] >= 100 else 0 for i, row in data.iterrows()]
        # a number of the compensations are below zero which seems to indicate incorrect or non-credible information has been entered
        # after indicating credibility of compensation values we replace these with zero
        data['comp'] = [np.nan if row['has_credible_comp'] == 0 else row['comp'] for i,row in data.iterrows()]
        
        # export data into csv filter
        data.to_csv(os.path.join(self.intdir, 'olx_jobads_clean.csv'), index=False)
        
        #print(data['fulltime'].value_counts())
        #print(keeprows[0:10])
        jobvacancies = data[keeprows == True]
        summarystats = jobvacancies.groupby('jobsector').mean()
        tempstats = jobvacancies.groupby('jobsector')['uniqueadid'].count()
        print(tempstats)
        print(summarystats)
        summarystats.to_csv('summary_statistics_OLX.csv')
        jobvacancies.reset_index(inplace=True)
        # add in summary stats over time
        return(jobvacancies)
    
    
    def generate_stats(self, jobvacancies):

        # Keep unique job vacancies only for main statistics
        temp = jobvacancies.groupby(['uniqueadid','postdate'])['downloaddate'].max()
        temp = pd.DataFrame(temp)
        temp.columns = ['maxdownloaddate']
        temp.reset_index(inplace=True)
        jobvacancies1 = jobvacancies.merge(temp,on=['uniqueadid','postdate'])
        jobvacancies2 = jobvacancies1.loc[jobvacancies1['downloaddate'] == jobvacancies1['maxdownloaddate'],]

        summarystats = jobvacancies2.groupby(['jobsector'])['i_photo','i_featured','daysposted','pageviews','has_comp','has_credible_comp','comp','emailavail','phoneavail','bachelor_degree','fulltime','exp_management','exp_entrylevel'].mean()
        summarystats.reset_index(inplace=True)
        tempstats = jobvacancies2.groupby(['jobsector'])['uniqueadid'].count()
        tempstats = pd.DataFrame(tempstats)
        tempstats.reset_index(inplace=True)
        statsmerged = tempstats.merge(summarystats,on=['jobsector'])
        statsmerged.to_csv('summary_statistics_OLX.csv')
    
        # job postings by month
        tempstats = pd.DataFrame(tempstats)
        tempstats.reset_index(inplace=True)
    
        tempstats1 = tempstats.pivot(index='month', columns='jobsector', values='uniqueadid')
        tempstats1.reset_index(inplace=True)
        print(tempstats1.head())
        tempstats1.to_csv('OLX_timeseries_sector_counts.csv')
    
        # average page views over time
    
    def generate_stats(self):
        """Create additional variables that are useful for generating statistics."""
    
        df = pd.read_sql(query, self.conn)
        df, cols = self._create_features(df)
        statcols = cols
        self._create_stats(df, 'industry', statcols, 'uid')
        
    def run_all(self):
        print("Running OLXPreprocessor on date (%s)" % (self.datecur))
        print("="*100)
        self._document_missing()
        self._document_jobads('jobadpageurls','uid','jobsector')
        
        #self.generate_stats()
        #jobvacancies = self.clean_data(self.unprocdata)
        #create_statistics(jobvacancies)
        self.conn.close()

if __name__ == "__main__":

    op = OLXPreprocessor()
    op.run_all()
