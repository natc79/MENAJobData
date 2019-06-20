"""
Purpose:  This class consists of functions to scrape job advertisement data
from tanqeeb websites.  It is developed to store information in a SQL database.

Author:  Natalie Chun
Created: November 2018
Updated: May 2019
"""

from pytz import timezone
import datetime
import time
import random
import sqlite3
import csv
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os
from googletrans import Translator
import html2text
from basedownloader import BaseDownloader
from config import FileConfig
from create_databases import get_tanqeeb_table_schema


countries = ['algeria','egypt','jordan','morocco','tunisia']

class TanQeebDownloader(BaseDownloader):
    
    def __init__(self, params):
        #super(TanQeebDownloader, self).__init__()
        self.extdir = os.path.join(FileConfig.EXTDIR,'tanqeeb')
        self.outdir = os.path.join(FileConfig.EXTDIR,'tanqeeb')
        self.conn = sqlite3.connect(os.path.join(self.outdir,"tanqeeb.db"), timeout=3)
        self.cursor = self.conn.cursor()
        self._create_table_schema(get_tanqeeb_table_schema())
        self.country = params["country"]
        self.tz = timezone(params["timezone"])
        self.url = 'https://%s.tanqeeb.com/' % (params['webname'])
        self.datecur = datetime.datetime.now(self.tz)
        print("Start Time: {}".format(self.datecur))
        self.datemap = ['NULL', 'January','February','March','April','May',
                    'June','July','August','September',
                   'October','November','December']
        
    def get_page_urls(self, url, classvar):
        """General function for extracting page urls for different categories."""
        
        response = self._request_until_succeed(url)
        if response is None:
            return([])
        soup = BeautifulSoup(response, 'html.parser')
        #print(soup)
        temp1 = soup.find('div', {'class':classvar})
        temp2 = temp1.find_all('ul', {'class':'row'})
        
        category_hrefs = []
        for t2 in temp2:
            temp3 = t2.find_all('a')
            for t3 in temp3:
                temp = t3.find('img')
                img = temp['src'] if temp is not None else ''
                if re.match(r'\w+', t3.text.strip()) is not None:
                    category_hrefs.append([self.country, t3.text.strip(), t3['href'], img])
        return(category_hrefs)
        
    def get_urls(self):
        """Get all the urls associated with different category topics."""
        print("Getting urls")
        
        # if getting new url data delete all of previous entries
        query = """DELETE FROM mainurls WHERE cat IS NOT NULL AND country = '%s';""" % (self.country)
        self.cursor.execute(query)
        self.conn.commit()
        query = """DELETE FROM categoryurls WHERE cat IS NOT NULL AND country = '%s';""" % (self.country)
        self.cursor.execute(query)
        self.conn.commit()
        
        # STEP 1:  get main page urls
        url = self.url + '/en'
        all_hrefs = []
        category_hrefs = self.get_page_urls(url, "tab-content")
        for row in category_hrefs:
            country, catname, href, img = row
            if re.match(r'\w+ Website Jobs', catname) or re.match(r'Tanqeeb', catname) is not None:
                cattype = 'publisher'
            elif re.match(r'\w+ Jobs', catname) is not None:
                cattype = 'occupation'
            elif re.match(r'Jobs in \w+', catname) is not None:
                cattype = 'location'
            catname = re.sub(r'(Jobs in |( Website)* Jobs)','',catname)
            query = """INSERT OR IGNORE INTO mainurls (downloaddate, country, topic, cat, href, img) VALUES(?, ?,?,?,?,?);"""
            row = [self.datecur.date(), country, cattype, catname, href, img]
            self.cursor.execute(query,row)
        self.conn.commit()
        time.sleep(random.randint(1,5))
        
        
        # STEP 2:  get category urls
        all_hrefs = []
        i = 0
        query = """SELECT DISTINCT cat, href FROM mainurls WHERE href LIKE '%category%' AND country = '{}';""".format(self.country)
        df = pd.read_sql(query, self.conn)
        for i, row in df.iterrows():
            cat, href = row['cat'], row['href']
            url = self.url + href
            print(url)
            temp_hrefs = self.get_page_urls(url, "panel panel-default")
            cat = cat.replace(' Jobs','')
            query = """INSERT OR IGNORE INTO categoryurls 
            (downloaddate, country, cat, subcat, href) VALUES(?,?,?,?,?);"""
            for rowh in temp_hrefs:
                row = [self.datecur.date(), country, cat, rowh[1].replace(' Jobs','').strip(), rowh[2]]
                self.cursor.execute(query,row)
            time.sleep(random.randint(2,5))
        self.conn.commit()
        
    def _clean_description(self, description):
        """Clean description of extraneous characters."""
        words = re.split(r'[\n\t\r]',description)
        words = [word.strip() for word in words if re.search(r'\w+',word) is not None]
        description = '\n'.join(words)
        description = re.sub(r'<!--(.*?)-->', '', description)
        description = description.replace(u"\xa0", u"\n")
        description = description.encode('utf-8')
        # replace any comments
        return(description)
        
    def get_jobad_summary_page(self, cat, subcat, href, pagetype='first'):
        """Get summary page of job advertisements.
        TODO:  Generalize to move onto next page.
        """
        print("Scraping page for category (%s) subcat (%s)" % (cat, subcat))
        if pagetype == 'first':
            url = self.url + href
        else:
            url = href
        response = self._request_until_succeed(url)
        if response is None:
            return 
        soup = BeautifulSoup(response, 'html.parser')
        temp1 = soup.find('div', {'id':'jobs_list'})
        if temp1 is None:
            return
        temp2 = temp1.find_all('div', {'id':True})
        data = {}
        cols = ['country', 'cat', 'subcat', 'uniqueid', 'dataid', 
                    'i_featured', 'date', 'title', 'href', 'description']
        data['country'] = self.country
        data['cat'] = cat
        data['subcat'] = subcat
        for t2 in temp2:
            data['uniqueid'] = t2['data-id']
            data['dataid'] = t2['id']
            data['i_featured'] = 1 if "featured_job" in t2['class'] else 0
            t3 = t2.find('a',{'href':True})
            data['title'] = t3.text.strip()
            data['href'] = t3['href']
            temp4 = t2.find('div', {'class':'meta-desc'})
            temp5 = temp4.find('a')
            if temp5 is not None:
                data['company'] = temp5.text
            temp5a = re.search(r'\b((\d+) (\w+) (\d+))\b', temp4.text)
            if temp5a is not None:
                data['date'] = datetime.date(int(temp5a.group(4)),self.datemap.index(temp5a.group(3)),int(temp5a.group(2))).strftime('%Y-%m-%d')
                pagedate = datetime.datetime.strptime(data['date'], '%Y-%m-%d')
            else:
                pagedate = None
            t6 = t2.find('p')
            data['description'] = self._clean_description(t6.text)
            query = """INSERT OR IGNORE INTO jobadpageurls (country, cat, subcat, uniqueid, dataid, 
                    i_featured, postdate, title, href, description) VALUES(?,?,?,?,?,?,?,?,?,?);"""
            row = [data[col] if col in data else np.nan for col in cols]
            #print(row)
            self.cursor.execute(query,row)
            self.conn.commit()
     
        nextpage = soup.find('link',{'rel':'next'})
        # only scrape next summary page if date is greater than lastdownloaddate
        if nextpage is not None and pagedate is not None:
            if pagedate.date() >= self.lastdownloaddate:
                print("Getting next page", nextpage['href'])
                self.get_jobad_summary_page(cat, subcat, nextpage['href'], pagetype='next')
        
    def get_jobad_page(self, uid, href):
        """Get individual job ad pages."""
        data = {}
        url = self.url + href
        print(url)
        # potential cols 
        cols = ['country','uid','Posted date','Location','Job Type',
                'Company','Required Experience','Salary','Education','title','Publisher','description']
        response = self._request_until_succeed(url)
        soup = BeautifulSoup(response, 'html.parser')
        data['country'] = self.country
        data['uid'] = uid
        temp = soup.find('h1', {'class':""})
        if temp is not None:
            data['title'] = temp.text.strip()
        tableinfo = soup.find('table', {'class':"table job-details-table"})

        if tableinfo is not None:
            for t in tableinfo.find_all('tr'):
                varname = t.find('th').text.strip()
                vardata = t.find('td').text.strip()
                if varname == 'Posted date':
                    temp = re.search(r'((\d+) (%s) (\d+))' % ('|'.join(self.datemap)),vardata)
                    if temp is not None:
                        vardata = datetime.date(int(temp.group(4)),self.datemap.index(temp.group(3)),int(temp.group(2))).strftime('%Y-%m-%d')
                elif varname == 'Publisher':
                    temp = t.find('img')
                    t1 = re.search(r'(thumb.*?\.(png|jpeg))$',temp['src'])
                    if t1 is not None:
                        vardata = t1.group(1)
                elif varname == 'Location':
                    vardata = re.sub(r'\t', '', vardata)
                data[varname] = vardata
            data['description'] = self._clean_description(str(soup.find('div', {'class':'job-details'})))
            query = """INSERT OR IGNORE INTO jobadpage 
                (country, uniqueid, postdate, location, jobtype, company, reqexp, salary, education, title, 
                pubimg, description)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?);"""
            row = [data[col] if col in data else np.nan for col in cols]
            #print(row)
            self.cursor.execute(query,row)
            self.conn.commit()
        else:
            temp = soup.find('div',{'class':"alert alert-warning"})
            if temp is not None:
                # Delete the job ad number from href as it is no longer relevant (and we will not find it)
                query = """DELETE FROM jobadpageurls
                WHERE country = '%s' AND uniqueid = '%s' AND href = '%s';"""  % (self.country, uid, href)
                self.cursor.execute(query)
                self.conn.commit()
        
    def get_new_jobad_pages(self):
        """Query data to get new job ad pages that have not been posted.
        Time series data is irrelevant for Tanqeeb since there is nothing to capture.
        """

        query = """SELECT DISTINCT uniqueid, href FROM jobadpageurls 
        WHERE country = '%s' AND uniqueid NOT IN
        (SELECT uniqueid FROM jobadpage WHERE country = '%s')""" % (self.country, self.country)
        jobadpages = self.cursor.execute(query).fetchall()
        print("Downloading %d pages" % (len(jobadpages)))
        for uid, href in jobadpages:
            self.get_jobad_page(uid,href)
        
    def translate_descriptions(self):
        """Translate description from arabic to english"""
        
        print("Starting to Translate")
        trans = Translator()
        h = html2text.HTML2Text()
        h.ignore_links = True
        
        query = """SELECT DISTINCT country from jobadpage;"""
        countries = self.conn.execute(query)
        
        for country in countries:
            query = """SELECT DISTINCT j.country, j.uniqueid, j.description FROM jobadpage j 
            WHERE j.country='%s' AND NOT EXISTS (SELECT t.country, t.uniqueid FROM translation t WHERE j.country=t.country AND j.uniqueid=t.uniqueid);""" % (country[0]) 
            results = pd.read_sql(query, self.conn)
            print("Translating %s for %d descriptions" % (country[0], len(results)))
            for i, row in results.iterrows():
                temp = h.handle(row['description'].decode('utf-8'))
                query = """INSERT OR IGNORE INTO translation (country, uniqueid, description_en) VALUES (?,?,?);"""
                try:
                    entry = [row['country'],row['uniqueid'],trans.translate(temp).text]
                except:
                    print("Error: %s" % (row['uniqueid']))
                    entry = [row['country'],row['uniqueid'],'Error']
                self.cursor.execute(query,entry)
                self.conn.commit()
                time.sleep(random.randint(1,3))
                if i % 1000 == 0:
                    print("Translating %d" % (i))
              
    def run_all(self, debug=False):
        """Download all relevant data"""
        print("Running TanqeebDownloader for %s on date (%s)" % (self.country, self.datecur))
        print("="*100)
        
        self._display_db_tables()
        # only download new links if we have not looked at it in more than a week
        linkdate1 = self._last_download_date('mainurls', 'downloaddate')
        linkdate2 = self._last_download_date('categoryurls', 'downloaddate')
        if linkdate1 is None or linkdate2 is None or (self.datecur.date() - linkdate2).days >= 7:
            print("Getting urls")
            self.get_urls()
        
        # download newly listed job ads
        self.lastdownloaddate = self._last_download_date('jobadpageurls', 'postdate')
        query = """SELECT DISTINCT cat, subcat, href FROM categoryurls WHERE country='%s';""" % (self.country)
        df = pd.read_sql(query, self.conn)
        for i, row in df.iterrows():
            self.get_jobad_summary_page(row['cat'],row['subcat'],row['href'], pagetype='first')
            if debug and i > 1:
                break
        self.get_new_jobad_pages()
        if self.country == 'tunisia':
            self.translate_descriptions()
        self._display_db_tables()
        self.conn.close()
       
if __name__ == "__main__":

    countryparams = [
        {"country":"algeria", "webname":"algerie", "timezone":"Africa/Algiers"},
        {"country":"egypt", "webname":"egypt", "timezone":"Africa/Cairo"},
        {"country":"jordan", "webname":"jordan", "timezone":"Asia/Amman"},
        {"country":"morocco", "webname":"morocco", "timezone":"Africa/Casablanca"},
        {"country":"tunisia", "webname":"tunisia", "timezone":"Africa/Tunis"}
    ]
    for i, params in enumerate(countryparams):
        td = TanQeebDownloader(params)
        td.run_all()
    
