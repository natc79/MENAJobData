"""
Purpose:  This class consists of general functions that are useful 
in processing data from various websites containing job advertisement
data that was scraped from the web.

Author:  Natalie Chun
Created: 23 November 2018
"""

from pytz import timezone
import csv
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time
import random
import re
import os
from config import FileConfig
from google.cloud import translate
import sqlite3
from basepreprocess import BasePreprocessor
from googletrans import Translator
import html2text

class TanQeebPreprocessor(BasePreprocessor):

    def __init__(self):
        self.extdir = os.path.join(FileConfig.EXTDIR, 'tanqeeb')
        self.conn = sqlite3.connect(os.path.join(self.extdir, 'tanqeeb.db'), timeout=10)
        self.cursor = self.conn.cursor()
        #self.unprocdata = self._combine_data()
        self.figdir = os.path.join(FileConfig.FIGDIR, 'tanqeeb')
        self.outdir = os.path.join(FileConfig.INTDIR, 'tanqeeb')
        self.tz = timezone('Africa/Cairo')
        self.datecur = datetime.now(self.tz)
        self.datasrc = 'TanQeeb'

    def _translate_to_english(self):
        """Translate the arabic description into english and insert into sql table."""
    
        query = "SELECT DISTINCT * FROM jobadpage WHERE postdate IS NOT NULL;"
        results = pd.read_sql(query, tp.conn)
        trans = Translator()
        h = html2text.HTML2Text()
        h.ignore_links = True
        description_en = []
        query = """SELECT DISTINCT uniqueid FROM translation;"""
        trans_ids = list(pd.read_sql(query, tp.conn)['uniqueid'])
    
        for i, row in results[0:10].iterrows():
            if row['uniqueid'] not in trans_ids:
                temp = h.handle(row['description'].decode('utf-8'))
                query = """INSERT OR IGNORE INTO translation (country, uniqueid, description_en) VALUES (?,?,?);"""
                entry = [row['country'],row['uniqueid'],trans.translate(temp).text]
                tp.cursor.execute(query,entry)
                tp.conn.commit()
                time.sleep(random.randint(3,6))
        
    def set_search_terms(self):
        """Set search terms."""
        
        searcheduc = {}
        searcheduc['diploma'] = ['diploma','high school', 'secondary education']
        searcheduc['bachelors'] = ['higher education','baccalaureate','university','college','university (graduates|degree)','bachelor(s){0,1}','b(\.){0,1}[as](\.){0,1}','b(\.){0,1}sc(\.){0,1}','college degree','engineering','computer science']
        searcheduc['masters'] = ['masters','m(\.){0,1}[as](\.){0,1}','m(\.){0,1}b(\.){0,1}a(\.){0,1}']
        searcheduc['doctorate'] = ['doctorate','phd','ph(\.){0,1}d(\.){0,1}','doctor of philosophy','faculty']
        searcheduc['md'] = ['m(\.){0,1}d(\.){0,1}','medical doctor', 'medical degree']
        searcheduc['rn'] = ['r(\.){0,1}n(\.){0,1}','registered nurse', 'nursing degree','nursing']
        searcheduc['jd'] = ['j(\.){0,1}d(\.){0,1}','law degree']
        self.searcheduc = searcheduc
        
    def get_education_map(self, educseries):
        """Get education map for arabic to english."""
    
        values = educseries.value_counts()
        translist = []
        for index in values.index:
            translation = client.translate(
            index,
            target_language='en')
            translist.append(translation['translatedText'].lower())
        
        df = pd.DataFrame({'arabic':values.index,'english':translist})
        # create map for bachelors degree
        df['english'] = [row['english'].strip().replace('&#39;',"'") for i, row in df.iterrows()]
        df['education'] = ''
        for ed, srchterm in searcheduc.items():
            re1 = re.compile(r'\b(%s)\b' % ('|'.join(srchterm)))
            df['education'] = [ed if re1.match(row['english']) is not None else row['education'] for i, row in df.iterrows()]
        educmap = {row['arabic']: row['education'] for i, row in df.iterrows() if row['education']!=''}
        with open(os.path.join(FileConfig.INTDIR,'tanqeeb','educmap.pickle'), 'wb') as f:
            pickle.dump(educmap, f)
        
        return(df)
        
    def fillin_education(self):
        """Fill in education column based on content description."""
        
        query = "SELECT DISTINCT * FROM jobadpage WHERE postdate IS NOT NULL;"
        results = pd.read_sql(query, self.conn)
        
        if os.isfile(os.path.join(FileConfig.INTDIR,'tanqeeb','educmap.pickle')) is False:
            self.get_education_map(results['education'])
        
        with open(os.path.join(FileConfig.INTDIR,'tanqeeb','educmap.pickle'), 'rb') as f:
            educmap = pickle.load(f)
            
        results['f_education'] = results['education'].replace(educmap)
        results['description'] = [row['description'].decode('utf-8') for i, row in results.iterrows()]
        results['f_education'] = ['' if row['f_education'] not in list(searcheduc.keys()) else row['f_education'] for i, row in results.iterrows()]

        for ed, srchterm in self.searcheduc.items():
            re1 = re.compile(r'\b(%s)\b' % ('|'.join(srchterm)))
            results['f_education'] = [ed if row['f_education']=='' and row['description'] is not None and re1.search(row['description'].lower()) is not None else row['f_education'] for i, row in results.iterrows()]
    
        searcheducar = {}
        for key, ed in educmap.items():
            if ed not in searcheducar:
                searcheducar[ed] = []
                searcheducar[ed].append(key)
    
        for ed, srchterm in searcheducar.items():
            re1 = re.compile(r'\b(%s)\b' % ('|'.join(srchterm)))
            results['f_education'] = [ed if row['f_education']=='' and row['description'] is not None and re1.search(row['description'].lower()) is not None else row['f_education'] for i, row in results.iterrows()]
        
        # Translate description into english

        return(results)

    def _get_top_skills(self):
        """Identify top skills based on job descriptions.
        Right now assume time series data is not that interesting.
        NOTE:  Not that efficient if dataset large...think about how to better do this.
        """
        
        query = """SELECT DISTINCT country, uniqueid, description
        FROM jobadpage
        """
        #chunk_iter = pd.read_sql(query, self.conn, chunksize=opt_chunk)
        #for chunk in chunk_iter:
        df= pd.read_sql(query, self.conn)
        # STEP 1: Clean the text of extraneous words
        df['description'] = [self._translate_text(self._clean_text(row['description'])) for i, row in df.iterrows()]
        # STEP 2:  Extract key words from database
        df['vocab_words'] = [kw['master'].extract_keywords(row['description']) for i, row in df.iterrows()]
        # STEP 3:  Figure out total counts of keywords
        
    def _create_time_series(self):
        """Function to develop time series data and key variables that are useful
        in developing predictions.
        """
        raise NotImplementedError
        
        
    def _combine_data(self):
        
        #extract the relevant set of data
        query = '''SELECT * FROM pagedata;'''
        unprocdata = pd.read_sql(query, self.conn,parse_dates=['postdate','downloaddate'])
        query = '''SELECT * FROM archivedpagedata;'''
        unprocarchiveddata = pd.read_sql(query, self.conn, parse_dates=['postdate','downloaddate'])
        unprocdata = unprocdata.append(unprocarchiveddata,ignore_index=True)

        #now get any data that might exists in the various csv files that have been archived
        archivedfiles = []
        if len(archivedfiles) > 0:
            for date in archivedfiles:
                filename = 'archivedpagedata_'+date+'.csv'
                tempdata = pd.read_csv(filename,parse_dates=['postdate','downloaddate'])
                #NOTE:  append tempdata to the unprocdata file maybe have to check that the data is in same format
                unprocdata = unprocdata.append(tempdata,ignore_index=True)
        
        c.close()
        print("Number of distinct entries: {}".format(len(unprocdata)))
        print(unprocdata.dtypes)
        print(unprocdata.head())
        print("Memory Usage (mb): {}".format(round(unprocdata.memory_usage(deep=True).sum()/1048576,2)))
        return(unprocdata)
        
    def generate_stats(self):
        """Create additional variables that are useful for generating statistics."""
    
        query = """SELECT u.country, u.cat, u.uniqueid, u.i_featured, u.postdate,
                            j.jobtype, j.company, j.reqexp, j.education, j.title, j.pubimg
            FROM jobadpageurls AS u
            LEFT JOIN jobadpage AS j 
                ON u.uniqueid = j.uniqueid AND u.country = j.country
        """
   
        df = pd.read_sql(query, self.conn)
        statcols = ['i_featured']
        self._create_stats(df, 'cat', statcols, 'uniqueid')
        
    def run_all(self):
        print("Running TanqeebPreprocessor on date (%s)" % (self.datecur))
        print("="*100)
        self._document_missing()
        self._document_jobads('jobadpageurls','uniqueid','cat')
        self.generate_stats()
        self.conn.close()
        
if __name__ == "__main__":

    tp = TanQeebPreprocessor()
    tp.run_all()