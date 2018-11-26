"""
Purpose:  This class consists of general functions that are useful 
in downloading and storing data from various websites that 
are continually scraped through a crontab.

Author:  Natalie Chun
Created: 22 November 2018
"""

import urllib
import urllib.request
from bs4 import BeautifulSoup
import sys
import pandas as pd
import re
import datetime
import time
import random
import csv
import sqlite3


class BaseDownloader(object):
    """Base code for downloading data from various websites.  Specific application
    is for the download of job advertisement data, but can be applied more generally.
    """
    
    def __init__(self, db):
        super(BaseDownloader, self).__init__(db)
        self.conn = sqlite3.connect(db)
        self.cursor = conn.cursor()
        self.datecur = datetime.datetime.now()
                
    def _display_db_tables(self):
        query = """SELECT name FROM sqlite_master WHERE type='table';"""
        tablenames = self.cursor.execute(query).fetchall()
        for name in tablenames:
            query = """SELECT * FROM %s""" % (name)
            temp = self.cursor.execute(query).fetchall()
            print("Entries in table %s: %d" % (name[0], len(temp)))
            query = """SELECT DISTINCT * FROM %s WHERE country = '%s' LIMIT 5;""" % (name[0], self.country)
            temp = self.cursor.execute(query).fetchall()
            print(temp)
                
    def _request_until_succeed(self, url):
        """URL request helper, set to only request a url 5 times before giving up."""
        
        req = urllib.request.Request(url)
        count = 1
        while count <= 5:
            try: 
                response = urllib.request.urlopen(req)
                if response.getcode() == 200:
                    return(response)
            except Exception:
                print("Error for URL %s : %s" % (url, datetime.datetime.now()))
            time.sleep(random.randint(1,5))
            count+=1
        return(None)
        
    def _last_download_date(self, table):
        """Query the latest data that will inform the scraping tool.  If data
        exists insert from 29 days ago, otherwise only insert data posted after last date downloaded.
        """
        
        query = """SELECT MAX(postdate) FROM %s""" % (table)
        lastdate = self.cursor.execute(query).fetchall()[0][0]
        print("Last Date Downloaded: {}".format(lastdate))
        if lastdate is None:
            newdate = self.datecur - datetime.timedelta(days=29)
            lastdate = datetime.date(newdate.year,newdate.month,newdate.day)
        else:
            temp = lastdate.split('-')
            lastdate = datetime.date(int(temp[0]),int(temp[1]),int(temp[2]))
        return(lastdate)
        urldata = get_WuzuffJobUrls(datetime.date(int(temp[0]),int(temp[1]),int(temp[2])))
        
    def _archive_database(self, table, urltable, maxdays=90):
        """Create an archive database where information is selected from main database and placed into 
        stored archive.  This helps limit the amount of data stored on the cloud system.
        """

        query = '''INSERT OR IGNORE INTO archived%s SELECT * FROM %s WHERE uid in (SELECT DISTINCT uid FROM %s WHERE stat == 'CLOSED' OR DATE(postdate) < DATE('{}','-{} days'));''' % (table, table, table) 
        temp1 = self.cursor.execute(query.format(self.datecur,maxdays))

        query = '''DELETE FROM %s WHERE uid in (SELECT DISTINCT uid FROM archived%s);''' % (urltable, table)
        self.cursor.execute(query)
        temp2 = self.cursor.execute(query)

        query = '''DELETE FROM %s WHERE uid in (SELECT DISTINCT uid FROM archived%s);''' % (table, table)
        self.cursor.execute(query)
        temp3 = self.cursor.execute(query)
        self.conn.commit() 

        #want to extract data from both page data and archived page data to place in csv file
        #occassionally clean up the archived page data file so that there is no data in it any longer
        #lets set this to be done on the first day of each month so we just store this data (which is unique)
        if self.datecur.day == 1:
            datenow = self.datecur.strftime("%m%d%Y")
            query = '''SELECT * FROM archived%s;''' % (table)
            df = pd.read_sql(query, self.conn)
            df.to_csv('archivedpagedata_%s.csv' % (datenow), mode='w', index=False)
            print("Number of archived page entries: {}".format(len(df)))

        #clear information from the archivedpagedata
        query = '''DELETE FROM archived%s WHERE uid in (SELECT uid FROM archived%s);''' % (table, table)
        self.cursor.execute(query)
        self.conn.commit()
        self.cursor.close()
     
    def _create_table_schema(self, tables):
        """Generate table schema for database.  If it doesn't currently exist"""

        for t, query in tables.items():
            self.cursor.execute(query)
            # create archived databases
            aquery = """CREATE TABLE IF NOT EXISTS archived%s AS SELECT *
                    FROM %s WHERE 0""" % (t, t)
            self.cursor.execute(aquery)
            self.conn.commit()
        
        # Print out tables in database
        query = """SELECT name FROM sqlite_master WHERE type='table';"""
        tablenames = self.cursor.execute(query).fetchall()
        print(tablenames)
            
    def run_all(self):
        raise NotImplementedError