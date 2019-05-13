"""
Purpose:  This class consists of functions to scrape job advertisement data
from Wuzzuf's website.  It is developed to store information in a SQL database
that contains historical data on number of job applicants, reviews and selections.

Author:  Natalie Chun
Created: November 2018
"""

from pytz import timezone
import datetime
import time
import random
import sqlite3
import urllib.request
import re
import os
import pandas as pd
import numpy as np
from config import FileConfig
from bs4 import BeautifulSoup
from basedownloader import BaseDownloader
from create_databases import get_wuzzuf_table_schema

class WuzzufDownloader(BaseDownloader):
    
    def __init__(self):
        #super(WuzzufDownloader, self).__init__()
        self.conn = sqlite3.connect(os.path.join(FileConfig.EXTDIR, 'wuzzuf', "wuzzuf_new.db"))
        self.cursor = self.conn.cursor()
        self.country = 'egypt'
        self.tz = timezone('Africa/Cairo')
        self.datecur = datetime.datetime.now(self.tz)
        self._create_table_schema(get_wuzzuf_table_schema())
        print("Start Time: {}".format(self.datecur))
        
        
    def get_job_urls(self, lastdownloaddate, debug=False):
        """Get urls for each job in the database."""
    
        url = 'https://wuzzuf.net/search/jobs?start=0&filters%5Bcountry%5D%5B0%5D=Egypt'
        nextpage = True

        #check the dates of the pages that are listed
        while nextpage:
    
            req = urllib.request.Request(url)
            response = urllib.request.urlopen(req)
            soup = BeautifulSoup(response, 'html.parser')
    
            # objective is to get the links from the page and put it in a list to call and run through
            name_box = soup.find('div', attrs={'class': 'content-card card-has-jobs'})
            #print(name_box)

            #obtain all of the urls and dates associated with different jobs listed on the website (this only needs to be called once)
            for d in name_box.find_all('div', attrs={'class':'new-time'}):
                #print(d)
                temp= d.find('a',href=True)
                temptime =  d.find('time',title=True)
                dateval = datetime.datetime.strptime(temptime['title'],'%A, %B %d, %Y at %H:%M%p')
                url = temp['href'].split("?")[0]
                #print(url)
                temp = re.search(r'[jobs/p/|internship/](\d+)-',url)
                uniqueid = temp.group(1)
                query = '''INSERT OR IGNORE INTO jobadpageurls (country, uid, postdate, postdatetime, href) VALUES (?,?,?,?,?);'''
                row = [self.country, uniqueid,dateval.strftime('%Y-%m-%d'),temptime['title'],url]
                self.cursor.execute(query, row)
            self.conn.commit()
        
            # get the next set of job listings for this classification only if we have not already collected the data
            if dateval.date() >= lastdownloaddate and not debug:
                nextpg = name_box.find('li', attrs={'class': 'pag-next'})
                try:
                    url = nextpg.find_all('a', href=True)[0]['href']
                    #Print out length to track number of urls retrieved
                    # sleep to make sure not too many requests are being made
                    time.sleep(random.randint(1,5))
                except AttributeError:
                    nextpage = False
            else:
                print(row)
                nextpage = False
        

    def get_job_page(self, uid, urlname, postdate):
        """Scrapes individual job advertisement pages and return the row of relevant data."""
        #print(urlname)
        punctuation = [";",",","'","&"]
        data = {}
        cols = ['country','uid', 'postdate', 'posttime', 'downloaddate', 'downloadtime', 'stat',    'job-title',
                'job-company-name', 'job-company-location', 'num_applicants', 'num_vacancies', 'num_seen', 'num_shortlisted',
                'num_rejected', 'experience_needed', 'career_level', 'job_type', 'salary', 'education_level', 'gender', 'travel_frequency', 'languages', 'vacancies',
                'roles', 'keywords', 'requirements', 'industries']
                
        # actual time downloaded to use the page views as proxy       
        datetimecur = datetime.datetime.now(self.tz)
        data['country'] = self.country
        data['uid'] = uid
        data['downloaddate'] = datetimecur.strftime('%Y-%m-%d')
        data['downloadtime'] = datetimecur.strftime('%H:%M')
        data['postdate'] = postdate
        data['stat'] = 'OPEN'
        
        #STEP 1:  request the url page
        response = self._request_until_succeed(urlname)
        if response is None:
            urlname = urlname.split('-')[0]
            response = self._request_until_succeed(urlname)
        
        # No data retrieve return empty data
        if response is None:
            data['stat'] = 'NOT FOUND'
        else:
            #check job status and see if it is open or closed and it contains content
            soup = BeautifulSoup(response, 'html.parser')
            status = soup.find('div',attrs={'class':"alert alert-danger alert-job col-sm-12"})
            mainjobdata = soup.find('div', attrs={'class': 'job-main-card content-card'})
            if status is not None:
                data['stat'] = 'CLOSED'
            
        if response is None or mainjobdata is None:
            return([data[col] if col in data else np.nan for col in cols])
        
        #STEP 2:  parse main job data

        jobdata = mainjobdata.find_all(['h1','a','span'], {'class':True})
        #print(jobdata)
        for d in jobdata:
            if d['class'][0] in ['job-title','job-company-name','job-company-location']:
                data[d['class'][0]] = d.get_text().strip().encode('utf-8')

        #get stats on applicants
        temp = mainjobdata.find('div', attrs={'class': 'applicants-num'})
        data['num_applicants'] = int(temp.get_text()) if temp is not None else 0
        
        temp = mainjobdata.find('span', attrs={'class': 'vacancies-num'})
        data['num_vacancies'] = int(temp.get_text()) if temp is not None else 1
        
        temp = mainjobdata.find_all('div', attrs={'class': 'applicants-stat-num'})
        #print(temp)
        data['num_seen'] = int(temp[0].get_text()) if temp is not None and len(temp) > 0 else 0
        data['num_shortlisted'] = int(temp[1].get_text()) if temp is not None and len(temp) > 1 else 0
        data['num_rejected'] = int(temp[2].get_text()) if temp is not None and len(temp) > 2 else 0

        #get date when posted and download date
        postdate = mainjobdata.find('p', attrs={'class': 'job-post-date'})

        try:
            pdate = datetime.datetime.strptime(postdate['title'],'%A, %B %d, %Y at %I:%M%p')
        except ValueError:
            pdate = datetime.datetime.strptime(postdate['title'],'%A, %B %d, %Y at%I:%M%p')
        data['postdate'] = pdate.strftime('%Y-%m-%d')
        data['posttime'] = pdate.strftime('%H:%M') # best time format for spreadsheet programs
    
        #obtain job summary information
        jobsumm = soup.find('div', attrs={'class': 'row job-summary'})
        jobsummdata = jobsumm.find_all(['dl'])
        if jobsummdata is not None:
            for d in jobsummdata:
                temp = re.sub('\s+',' ',d.get_text()).strip().split(":")
                name = re.sub('\s',"_",temp[0].lower())
                if name in ['languages']:
                    data['languages'] = temp[1].strip().split(',')
                    data['languages'] = '>'.join(data['languages'])
                elif name in ['salary']:
                    if 'Negotiable' in temp[1].strip().split(','):
                        data[name] = temp[1].strip().split(',')
                    else:
                        newtemp = temp[1].strip().replace(',','')
                        data[name] = [newtemp]
                    data['salary']='>'.join(data['salary'])
                else:
                    data[name] = temp[1].strip()
            #print(data)
        
        # Job roles
        jobcard = soup.find('div', attrs={'class': "about-job content-card"})
        temp = jobcard.find_all('div', attrs={'class': "labels-wrapper"})
        if temp is not None:
            jobroles = []
            for d in temp:
                jobroles += [role.get_text().strip() for role in d.find_all(['a'])]
            data['roles'] = '>'.join(jobroles)
        
        #obtain job requirements, key words, and industry indicators
        jobreqs = soup.find('div', attrs={'class': "job-requirements content-card"})
        #print(jobreqs)
        if jobreqs is not None:
            temp = jobreqs.find_all('meta', content=True)
            keywords = []
            try:
                temp1 = temp[0]['content']
                for t in temp1.split(', '):
                    keywords.append(t)
                data['keywords'] = keywords
            except IndexError:
                data['keywords'] = []
        else:
            data['keywords'] = []
        data['keywords']='>'.join(data['keywords']).encode('utf-8')
    
        try:
            temp = jobreqs.find_all('li')
            reqs = []
            for d in temp:
                temp = d.get_text().lower().strip('.')
                for p in punctuation:
                    temp = temp.replace(';','')
                reqs.append(temp)
            data['requirements'] = reqs
            #print(reqs)
        except:
            data['requirements'] = []
        data['requirements']='>'.join(data['requirements']).encode('utf-8')
    
        ind1 = soup.find('div', attrs={"class": "industries labels-wrapper"})
        if ind1 is not None:
            ind2 = ind1.find_all(['a'])
            if ind2 is not None:
                data['industries'] = '>'.join([ind.get_text().strip() for ind in ind2]).encode('utf-8')
        
        jobinfo = [data[col] if col in data else np.nan for col in cols]
        return(jobinfo)
        
    def get_new_page_data(self, debug):
        """Obtain all job advertisement pages from urltable which we want to recollect data for insertion into
        database.  This helps with tracking changes in applicants over time.  It focuses on job ads where status
        is open (not closed).
        """
    
        jobpageurlquerylist = []

        for i in range(0,8):
            d = i*7
            temp = self.cursor.execute('''SELECT DISTINCT uid, href, postdate, country FROM jobadpageurls WHERE country = '{}' AND DATE(postdate) == DATE('{}','-{} days');'''.format(self.country, self.datecur.strftime('%Y-%m-%d'),d)).fetchall()
            jobpageurlquerylist = jobpageurlquerylist + temp
            if debug and i > 1:
                print(jobpageurlquerylist)
                break

        print("Number of pages to query: {}".format(len(jobpageurlquerylist)))

        #retrieve information for insertion into database
        for i, urlinfo in enumerate(jobpageurlquerylist):
            query = '''INSERT OR IGNORE INTO jobadpage (country, uid, postdate, posttime, downloaddate, downloadtime, stat, jobtitle, company, location, num_applicants, num_vacancies, num_seen, num_shortlisted, num_rejected, experience_needed, career_level, job_type, salary, education_level, gender, travel_frequency, languages, vacancies, roles, keywords, requirements, industries)
            VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
            rowvalues = self.get_job_page(urlinfo[0],urlinfo[1],urlinfo[2])
            self.cursor.execute(query, rowvalues)
            self.conn.commit()
            if debug and i > 2:
                print(rowvalues)
                break
        
    def run_all(self, debug=False):
        """Run key operations to update database."""
        print("Running WuzzufDownloader for %s on date (%s)" % (self.country, self.datecur))
        print("="*100)
        starttime = time.time()
        lastdownloaddate = self._last_download_date('jobadpageurls','postdate')
        self.get_job_urls(lastdownloaddate, debug=debug)
        print("Time to get new urls: {}".format(time.time()-starttime))
        self.get_new_page_data(debug=debug)
        print("Time to get scrape pages: {}".format(time.time()-starttime))
        self._display_db_tables()
        self._archive_database('jobadpage', 'jobadpageurls',maxdays=90)
        print("Total run time: {}".format(time.time()-starttime))
        
if __name__ == "__main__":
    wd = WuzzufDownloader()
    wd.run_all()