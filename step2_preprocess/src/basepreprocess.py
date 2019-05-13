"""
Purpose:  This class consists of general functions that are useful 
in processing data from various websites containing job advertisement
data that was scraped from the web.

Author:  Natalie Chun
Created: 23 November 2018
"""

import sqlite3
import pandas as pd
import numpy as np
import os
#import googletrans
import html
#from sklearn.feature_extraction import ENGLISH_STOP_WORDS
import matplotlib.pyplot as plt
import datetime

class BasePreprocessor(object):
    """Base preprocessor for data scraped from web."""
    
    def __init__(self, db):
        super(BasePreprocessor, self).__init__()
        self.conn = sqlite3.connect(db)
        self.cursor = conn.cursor()
        self.datecur = datetime.datetime.now()
        self.figdir = FileConfig.FIGDIR
        self.outdir = FileConfig.OUTDIR
        
    def _graph_bar(self, cat, values, params):
        """Horizontal bar char"""
        
        plt.style.use('classic')
        catpos = np.arange(len(cat))
        plt.barh(catpos, values, align='center', alpha=0.5)
        plt.yticks(catpos, cat)
        plt.ylabel(params['ytitle'])
        plt.xlabel(params['xtitle'])
        plt.title(params['title'])
        if 'note' in params:
            plt.annotate(params['note'], (0,0), (0, -50), xycoords='axes fraction', textcoords='offset points', va='top')
        plt.savefig(os.path.join(self.figdir, params['filename']), bbox_inches='tight', dpi=200)
        plt.gcf().clear()
        
    def create_education_feature(self):
        """Create education variable."""
        raise NotImplementedError
    
    def _create_dummies(self, df, catvars):
        """Create dummy variables from categorical variables."""
    
        # format categorical variables so they are a bit cleaner
        for cat in catvars:
            temp = df[cat].value_counts()
            print("Categories for %s" % (cat))
            print(temp)
            total = sum(temp.values)
            # create indicator variable only if 5% or greater of total sample, if not replace
            keys = {var: var if value/len(df) >= 0.05 else 'other' for var, value in temp.items()}
            df[cat] = df[cat].replace(keys) 
        
        # Generate dummies for categorical variables
        dummies = pd.get_dummies(catvars, dummy_na=False)
        df = pd.concat([df, pd.get_dummies(df[catvars], dummy_na=False)], axis=1)
        return(df, list(dummies.columns))
    
    def _create_stats(self, df, cat, statcols, uid):
        """Create table of basic statistics based on data."""
               
        summarystats = df[statcols + [cat, 'country']].groupby(['country', cat]).mean()
        summarystats.reset_index(inplace=True)
        tempstats = df.groupby(['country', cat])[uid].count()
        tempstats = pd.DataFrame(tempstats)
        tempstats.reset_index(inplace=True)
        statsmerged = tempstats.merge(summarystats,on=['country', cat])
        statsmerged.to_csv(os.path.join(self.outdir,'summary_statistics_%s.csv' % (self.datasrc)), index=False)

    def _graph_line(self, df, params):
        """Time series line charts.  To make subplots: https://matplotlib.org/gallery/lines_bars_and_markers/spectrum_demo.html#sphx-glr-gallery-lines-bars-and-markers-spectrum-demo-py
        """

        #plt.style.use('classic')
        plt.figure(figsize=(16,8))
        #df[datecol] = [datetime.strptime(row[datecol], '%Y-%m-%d') for i, row in df.iterrows()]
        #df.set_index(pd.to_datetime(df[datecol].date), drop=True)
        df.plot(linewidth=3, figsize=(16,8))
        plt.title(params['title'])
        plt.xlabel('Date')
        plt.ylabel(params['ytitle'])
        plt.legend(loc='best')
        plt.annotate(params['note'], (0,0), (0, -50), xycoords='axes fraction', textcoords='offset points', va='top')
        plt.savefig(os.path.join(self.figdir, params['filename']), bbox_inches='tight', dpi=200)
        plt.gcf().clear()
        
    def _document_missing(self):
        """Document missing data, by pulling in data from sqlite 
        and graphing it."""
        
        query = """SELECT name FROM sqlite_master WHERE type='table';"""
        tablenames = self.cursor.execute(query).fetchall()
        for name in tablenames:
            query = """SELECT * FROM %s""" % (name[0])
            df = pd.read_sql(query, self.conn)
            miss = df.isnull().sum()/len(df)*100
            miss.sort_values(inplace=True)
            params = {
                        "xtitle":"Percent missing", 
                        "ytitle":"Variable", 
                        "title":"Table (%s) Missing Variables\nData: %s" % (name[0], self.datasrc),
                        "filename": "missing_%s.png" % (name[0])
                     }
            
            self._graph_bar(miss.index, miss.values, params)
        
    def _document_jobads(self, table, uid, cat):
        """General function to document job ad counts and most frequent job ad counts over time.
        Args:  
        table (string) - reference table in sqlite db
        uid (string) - references column in sqlite table
        cat (string) - references job advertisement category in sqlite table
        """
        print(table, uid, cat)
        query = """CREATE TEMP TABLE t1 AS
            SELECT DISTINCT CAST(strftime('%Y',t3.postdate) AS INT) as year, CAST(strftime('%m',t3.postdate) AS INT) as month, t3.country, t3.{} AS uid, t3.{} AS cat, 1/t2.cnt AS weight
                FROM {} AS t3
                LEFT JOIN 
                    (   SELECT DISTINCT country, uid, COUNT(cnt) as cnt
                        FROM
                        (   SELECT DISTINCT country, {} as uid, {} as cat, 1 as cnt
                            FROM {}
                        ) AS t1
                        GROUP BY country, uid
                    ) AS t2
                    ON t3.country = t2.country AND t3.{} = t2.uid;
            """.format(uid, cat, table, uid, cat, table, uid)
        self.cursor.execute(query)
        
        query = """SELECT DISTINCT country, cat, SUM(weight) AS total
                FROM t1
                GROUP BY country, cat;
            """
            
        # STEP 1:  Graph aggregate job ad counts by category
        df1 = pd.read_sql(query, self.conn)
        df1.sort_values(['total'], ascending=[True], inplace=True)
        countries = list(set(df1['country']))
        for cty in countries:
            temp = df1[df1['country'] == cty]
            note = "Total unique job ads: %d" % (np.sum(temp['total']))
            params = {'xtitle':'Category', 'ytitle':'Total', 'title':'Job Ad Counts by Category\nCountry: %s\nData: %s' % (cty.title(), self.datasrc), 'filename':'jobad_counts_%s.png' % (cty), 'note':note}
            self._graph_bar(temp['cat'], temp['total'], params)
        
        # STEP 2:  Graph aggregate job ad counts by category over time for top 5
        query = """SELECT DISTINCT year, month, country, cat, SUM(weight) AS total
                FROM t1
                WHERE year IS NOT NULL AND month IS NOT NULL
                GROUP BY country, year, month, cat
            """
        df2 = pd.read_sql(query, self.conn)
        print(df2.head())
        df2['date'] = [datetime.date(int(row['year']), int(row['month']), 1) for i, row in df2.iterrows()]
        # take only top 5 categories in each country to graph
        for cty in countries:
            temp = df1[df1['country'] == cty].copy()
            temp.sort_values(['total'], ascending=[False], inplace=True)
            totalads = np.sum(temp['total'])
            top5 = temp['cat'].head(5)
            temp = df2[(df2['country'] == cty) & (df2['cat'].isin(top5))]
            top5pct = int(100*np.sum(temp['total'])/totalads)
            mindate = np.min(temp['date'])
            maxdate = np.max(temp['date'])
            # pivot df
            gdf = temp.pivot(index='date', columns='cat', values='total')
            #gdf.columns = ['_'.join(col).strip('_') for col in gdf.columns]
            note = 'Top 5 accounts for %d percent of all job ads between %s and %s' % (top5pct, mindate, maxdate)
            params = { 'ytitle':'Total Job Ads', 'title':'Job Ads by Month\nCountry: %s\nData: %s' % (cty.title(), self.datasrc), 'note':note, 'filename':'jobad_timecounts_%s.png' % (cty)}
            self._graph_line(gdf, params)
        
        # STEP 3:  Graph aggregate changes in job ad counts by category for top/bottom categories
        df3 = df2[df2['date'].isin([mindate,maxdate])].copy()
        df3.reset_index(inplace=True)
        df3['datetype'] = ['mindate' if row['date'] == mindate else 'maxdate' if row['date'] == maxdate else '' for i, row in df3.iterrows()]
        for cty in countries:
            temp = df3[df3['country'] == cty]
            gdf = temp.pivot(index='cat', columns='datetype', values='total')
            #print(gdf.head())
            gdf['change'] = gdf['maxdate'] - gdf['mindate']
            gdf.sort_values(['change'], ascending=[True], inplace=True)
            temp = gdf.head(20)
            params = {'xtitle':'Category', 'ytitle':'Change in Job Ads', 'title':'Top Category Changes Between (%s) and (%s)\nCountry: %s\nData: %s' % (mindate, maxdate, cty.title(), self.datasrc), 'filename':'jobad_changes_top_%s.png' % (cty)}
            self._graph_bar(temp.index, temp['change'], params)
            temp = gdf.tail(20)
            params = {'xtitle':'Category', 'ytitle':'Change in Job Ads', 'title':'Bottom Category Changes Between (%s) and (%s)\nCountry: %s\nData: %s' % (mindate, maxdate, cty.title(), self.datasrc), 'filename':'jobad_changes_bottom_%s.png' % (cty)}
            self._graph_bar(temp.index, temp['change'], params)
            
    def _translate_text(self, text):
        """Translate foreign text to English"""
        translator = googletrans.Translator()
        text = translator.translate(text).text.replace('\r',' ').replace('\n','>').encode('utf-8')
        return(text)
        
    def _clean_text(self, text):
        """Light cleaning of text that can be used for analysis."""
        text = html.unescape(text)
        return(text)
        
    def extract_data_to_csv(self):
        """Extract raw data from sql databases and export to csv"""
        
        query = "SELECT * FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql(query, self.conn)
 
        for i, row in tables.iterrows():
            # query table data and do a dump into bigquery
            query = "SELECT * FROM %s;" % row['name']
            data = pd.read_sql(query, self.conn)
            data.to_csv(os.path.join(self.extdir, 'csv', '%s.csv' % (row['name'])), index=False)
            
    def create_graphs():
        raise NotImplementedError
        
    def extract_kws():
        """Extract keywords that are used for building a dictionary and modeling."""
        raise NotImplementedError