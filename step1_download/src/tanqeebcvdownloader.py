"""
Purpose:  This class consists of functions to scrape resume data
from tanqeeb websites.  It requires creating an employer account and logging in.
It currently works best by operating the code interactively in a jupyter notebook
as the login-process is not currently automated.  There are two stages to gathering resumes:
1) Search resume database and download all of the unique ids associated with that search
2) Get all unique ids and check which are not in the mongoDB for resumes or were last downloaded/active recently
and should be updated.  Download new resume page.
Check if the resume page exists in the database, if it does not then

Author:  Natalie Chun
Created: 3 March 2019
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

class TanQeebCVDownloader(BaseDownloader):
    """Class for downloading tanqeeb CVs.  Probably need to use selenium"""
    
    def __init__(self, params, loginparams, driver=None):
        #super(TanQeebDownloader, self).__init__()
        self.outdir = os.path.join(FileConfig.EXTDIR,'tanqeeb')
        self.conn = sqlite3.connect(os.path.join(self.outdir,"tanqeebcv.db"), timeout=10)
        self.cursor = self.conn.cursor()
        self._create_table_schema(get_tanqeebcv_table_schema())
        self.country = params["country"]
        self.tz = timezone(params["timezone"])
        self.url = 'https://%s.tanqeeb.com/' % (params['webname'])
        self.datecur = datetime.datetime.now(self.tz)
        print("Start Time: {}".format(self.datecur))
        
        # set terms for mongodb as better for storing resume type data
        client = MongoClient('localhost', 27017)
        db = client['tanqeeb']
        resumes = db['resumes']
        self.client = client
        self.db = db
        
        self.datemap = ['NULL', 'Jan','Feb','Mar','Apr','May',
                    'Jun','Jul','Aug','Sep',
                   'Oct','Nov','Dec']
        self.datefullmap = ['NULL', 'January','February','March','April','May',
                    'June','July','August','September',
                   'October','November','December']
        self.loginparams = loginparams
        
        # declare the driver
        self.driver = driver if driver is not None else self.login()
         
    def login(self):
        """Login to Indeed (if necessary)"""
        
        params = self.loginparams
        
        # specifies the path to the chromedriver.exe
        driver = webdriver.Chrome(params['chromedriverpath'])
        # driver.get method() will navigate to a page given by the URL address
        driver.get('https://www.tanqeeb.com/employers/login')
        
        username = driver.find_element_by_id('LoginEmployerEmail').send_keys(params['useremail'])
        time.sleep(random.randint(2,4))

        # locate password form by_class_name and enter password
        username = driver.find_element_by_id('LoginEmployerPassword').send_keys(params['userpassword'])
        time.sleep(random.randint(2,4))

        # locate submit button by_xpath
        log_in_button = driver.find_element_by_xpath('//*[@type="submit"]')

        # .click() to mimic button click
        log_in_button.click()
        time.sleep(random.randint(1,5))
        
        return driver
            
    def parse_resume_page(self, soup, uid):
        """Parse the resume page."""
        
        # set mappings for page
        mapper = {}
        mapper['Education'] = {'date':'date','subject':'school','item1':'degree','item2':'major','description':'description'}
        mapper['Experiences'] = {'date':'date','date_start':'date_start','date_end':'date_end','subject':'jobtitle','item1':'company','item2':'location','description':'description'}
        mapper['Courses'] = mapper['Education']
        mapper['Certificates'] = mapper['Education']
        mapper['Projects'] = {'date':'date', 'subject':'name','item1':'item1', 'item2':'item2', 'description':'description'}
        
        # date values
        re_date = {}
        re_date[0] = re.compile(r'\b(\d+) (\w+) (\d+)$')
        re_date[1] = re.compile(r'^(\d+)$')
        re_date[2] = re.compile(r'^(\w+) (\d+)$')
        re_date[3] = re.compile(r'^(\w+) (\d+)\s+\-\s+(\w+) (\d+)$')
        re_date[4] = re.compile(r'^(\w+) (\d+)\s+\-\s+Present$')
        re_date[5] = re.compile(r'Last Active:\s+(\w+.*?)\b')
        
        data = {}
        data['_id'] = uid
        data['downloaddate'] = datetime.datetime.now()
        
        temp = soup.find('title')
        if temp is not None:
            data['name'] = temp.text.split('|')[0].strip().encode('utf-8')
        
        # Examine whether person has uploaded photo -- more valid (?)
        temp = soup.find('div', {'class':"media-left biothumb"})
        if temp is not None:
            data['has_photo'] = 1
            if 'img/avatar.png' in temp.find('img')['src']:
                data['has_photo'] = 0
        
        temp = soup.find('div', {'class':"media profile-header"})
        if temp is None:
            data['error'] = 'profile is deactivated'
            return(data)
        
        data['location'] = temp.find("h4", {'class':"no-margin"}).text.strip().encode('utf-8')
        data['title'] = temp.find('h4', {'class':None}).text.strip().encode('utf-8')
        stat = temp.find_all('p')
        if len(stat) > 0:
            data['jobstatus'] = stat[0].text
        if len(stat) == 2:
            temp2 = re_date[0].search(stat[1].text.strip().replace(u'\xa0', u' '))
            if temp2 is not None:
                print(temp2.group(3), temp2.group(2), temp2.group(1))
                data['last_active'] = datetime.datetime(int(temp2.group(3)), self.datefullmap.index(temp2.group(2)), int(temp2.group(1)))
            else:
                temp3 = re_date[5].search(stat[1].text.strip().replace(u'\xa0', u' '))
                if temp3 is not None:
                    print(temp3.group(1))
                    if temp3.group(1) == 'Yesterday':
                        data['last_active'] = datetime.datetime.now(self.tz) - datetime.timedelta(days=1)
                    elif temp3.group(1) in ['Today','Hours Ago','hours ago']:
                        data['last_active'] = datetime.datetime.now(self.tz)
                    else:
                        data['last_active'] = datetime.datetime.now(self.tz) - datetime.timedelta(days=7)
                        data['last_active_other'] =  temp3.group(1)
        
        cards = soup.find_all('div', {'class':'section-block'})
        #print(cards)
        #print(data)
        for card in cards:
            temp = card.find('div', {'class':"panel-header listing-header"})
            key = temp.find('h2').text
            content = card.find('div', {'class':'row'})
            if key == 'Personal Information':
                # need to figure out how to replace break with \n
                info = content.find_all('div', {'class':"col-md-6 col-sm-6"})
                for i in info:
                    i1 = i.text.split(':')
                    temp = re_date[0].search(i1[1].strip()) 
                    if temp is not None:
                        data[i1[0].strip()] = datetime.datetime(int(temp.group(3)), self.datefullmap.index(temp.group(2)), int(temp.group(1)))
                    else:
                        data[i1[0].strip()] = i1[1].strip()
            elif key in ['Education','Experiences','Courses','Certificates','Projects']:
                info = content.find_all('li', {'style':"position:relative;"})
                itemlist = []
                for i in info:
                    idata = {}
                    idate = i.find('div', {'class':'date'}).text.replace(u'\xa0', u' ')
                    temp = {}
                    for k, re1 in re_date.items():
                        temp[k] = re1.search(idate)
                    if temp[1] is not None:
                        idata['date'] = int(temp[1].group(1))
                    elif temp[2] is not None:
                        idata['date'] = datetime.datetime(int(temp[2].group(2)), self.datemap.index(temp[2].group(1)), 1)
                    elif temp[3] is not None:
                        idata['date_start'] = datetime.datetime(int(temp[3].group(2)), self.datemap.index(temp[3].group(1)), 1)
                        idata['date_end'] = datetime.datetime(int(temp[3].group(4)), self.datemap.index(temp[3].group(3)), 1)
                    elif temp[4] is not None:
                        idata['date_start'] = datetime.datetime(int(temp[4].group(2)), self.datemap.index(temp[4].group(1)), 1)
                        idata['date_end'] = datetime.datetime(data['downloaddate'].year, data['downloaddate'].month, data['downloaddate'].day)
                    else:
                        idata['date'] = idate
                    idata['subject'] = i.find('div', {'class':'subject'}).text.strip().encode('utf-8')
                    i1 = i.find('p').text
                    idata['item1'] = i1.split('|')[0].strip().encode('utf-8')
                    if '|' in i1:
                        idata['item2'] = i1.split('|')[1].strip().encode('utf-8')
                    i2 = i.find('div',{'class':"col-md-10"})
                    if i2 is not None and i2.text.strip() != '':
                        idata['description'] = i2.text.strip().encode('utf-8')
                        if idata['description'] == idata['item1']:
                            idata['item1'] = ''
                    newidata = {mapper[key][k]:value for k, value in idata.items() if value != ''}
                    itemlist.append(newidata)
                data[key.lower()] = itemlist
            elif key in ['Skills','Interests']:
                info = content.find_all('button')
                itemlist = []
                for i in info:
                    itemlist.append(i.text.strip().encode('utf-8'))
                data[key.lower()] = itemlist
            elif key in ['Languages']:
                info = content.find_all('li')
                itemlist = []
                for i in info:
                    idata = {}
                    idata['level'] = i.find('div', {'class':'dates'}).text.strip()
                    idata['type'] = i.find('div', {'class':'lang'}).text.strip()
                    itemlist.append(idata)
                data[key.lower()] = itemlist
            else:
                # should capture summary section
                data[key.lower()] = content.text.strip().encode('utf-8')
            #data[key] = content
        
        # save the raw unprocessed data so no need to download and can reparse if needed
        with open(FileConfig.EXTDIR +  "/tanqeeb/resumes/uid_%s.html" % (uid), "w") as file:
            file.write(str(soup.encode('utf-8')))
        
        return data
        
    def parse_resume_links(self, country, title, startpage=1):
        
        self.driver = driver
        countryidmap = {'Algeria':31, 'Egypt':213, 'Jordan':24, 'Morocco':50, 'Tunisia':99}
        srchtitle = title.replace(' ','+')
        url = "https://www.tanqeeb.com/usersearch/search?url=cv-search&keywords={}&country_id={}&nationality_id={}".format(srchtitle, countryidmap[country], countryidmap[country])
        driver.get(url)
        # sleep before trying to extract the page links...need some time lapse to download
        time.sleep(random.randint(3,6))
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
        except:
            return([])
        
        # get number of page results
        temp = soup.find('div', {'class':"pull-right text-primary result-num"}).text
        temp1 = re.search(r'(\d+) Results Found', temp)
        num_results = int(temp1.group(1))
        num_pages = num_results//20+1
    
        query = """SELECT DISTINCT * FROM jobstatmap;"""
        df = pd.read_sql(query, self.conn)
        jobstatmap = {row['jobstat']:row['id'] for i, row in df.iterrows()}
        cols = ['id','srchtitle','srchcountry','name','jobtitle','jobstatid','country','region']
        print("Querying %d pages for country (%s), title (%s)" % (num_pages, country, title))
        for page in range(startpage, num_pages+1):
            print("Downloading page %d" % (page))
            url = "https://www.tanqeeb.com/usersearch/search?url=cv-search&keywords={}&country_id={}&nationality_id={}&page={}".format(srchtitle, countryidmap[country],countryidmap[country],page)
            print(url)
            driver.get(url)
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
            except:
                return([])
            cards = soup.find_all('div', {'class':"job-box panel-content clearfix"})
            datalist = []
            for card in cards:
                data = {}
                data['srchtitle'] = title
                data['srchcountry'] = country
                link = card.find('a')['href']
                # parse out uniqueid 
                temp = re.search(r'/profile/(\d+)\?', link)
                data['id'] = temp.group(1)
                data['name'] = card.find('h2',{'class':"media-heading"}).text.strip().encode('utf-8')
                jobstat = card.find('p').text.strip()
                if jobstat not in jobstatmap:
                    query = """INSERT OR IGNORE INTO jobstatmap (id, jobstat) VALUES(?,?);""" 
                    row = [len(list(jobstatmap.keys())), jobstat]
                    self.conn.execute(query, row)
                    self.conn.commit()
                    query = """SELECT DISTINCT * FROM jobstatmap;"""
                    df = pd.read_sql(query, self.conn)
                    jobstatmap = {row['jobstat']:row['id'] for i, row in df.iterrows()}
                    
                data['jobstatid'] = jobstatmap[jobstat]
                data['jobtitle'] = card.find('h4').text.encode('utf-8')
                location = card.find('h5').text
                data['country'] = location.split('-')[0].strip()
                if '-' in location:
                    data['region'] = location.split('-')[1].strip()
                row = [data[col] if col in data else np.nan for col in cols]
                #print(row)
                query = """INSERT OR IGNORE INTO resumelinks (id, srchtitle, srchcountry, name, jobtitle, jobstatid, country, region) VALUES (?,?,?,?,?,?,?,?)"""
                self.conn.execute(query, row)
                self.conn.commit()
    
    def get_resume_links(self):
                
        countrylist = ['Algeria', 'Egypt', 'Jordan', 'Morocco', 'Tunisia']
        
        for country in countrylist[1:]:
            startpage = 785 if country == 'Egypt' else 1
            self.parse_resume_links(country, country, startpage)
               
    def clear_mongodb(self):
        """Clear database of fields that have errors to re-check and ensure scraper was working."""
        
        resumes = self.db.resumes
        temp = list(resumes.find({"error":{"$exists": True}},{'_id':1}))
        print(len(temp))
        print(temp[0:5])
        for t in temp:
            if db.resumes.delete_one(t)
        temp = list(resumes.find({"error":{"$exists": True}},{'_id':1}))
        assert len(temp) == 0, "Error deletion did not work"
            
    def get_resume_pages(self):
        """Get resume pages.  Store in mongodb.  TODO: FIXXX"""
        
        db = self.db
        resumes = self.db.resumes
        driver = self.driver
        
        query = """SELECT DISTINCT id FROM resumelinks ORDER BY RANDOM() ;"""
        df = pd.read_sql(query, self.conn)
        # get all the ids in mongod db
        py_ids = [int(i) for i in db.resumes.find().distinct('_id')]
        print("Number of entries in mongo DB: %d" % (len(py_ids)))
        df = df[~df['id'].isin(py_ids)]
        
        print("Trying to retrive %s pages" % (len(df)))

        
        for uid in df['id']:
            # check if the id exists and/or it is greater than 
            criteria = {"$and": [{"_id": uid}, {"downloaddate": {'$lt': self.datecur - datetime.timedelta(days=30)}}]}
            cnt1 = db.resumes.count_documents(criteria)
            cnt2 = db.resumes.count_documents({'_id': uid})
            url = 'https://www.tanqeeb.com/profile/{}?open=1'.format(uid)
            #print(url)
            if cnt1 > 0 or cnt2 == 0:
                driver.get(url)
                # sleep before trying to extract the page links...need some time lapse to download
                time.sleep(random.randint(2,4))
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                data = self.parse_resume_page(soup, uid)
                if cnt1 > 0:
                    db.resumes.delete_one(criteria)
                try:
                    resumes.insert_one(data)
                    print("Inserted %s" % (uid))
                except Exception as e:
                    print(e)
                    data = {'_id':uid, 'error':e}
                    resumes.insert_one(data)

        
if __name__ == "__main__":
    params = {'chromedriverpath':'C:/Users/natal/Downloads/chromedriver.exe', 'useremail':'suclistejo@memsg.site', 'userpassword':'temporary97'}
    countryparams = [
    {"country":"algeria", "webname":"algerie", "timezone":"Africa/Algiers"},
    {"country":"egypt", "webname":"egypt", "timezone":"Africa/Cairo"},
    {"country":"jordan", "webname":"jordan", "timezone":"Asia/Amman"},
    {"country":"morocco", "webname":"morocco", "timezone":"Africa/Casablanca"},
    {"country":"tunisia", "webname":"tunisia", "timezone":"Africa/Tunis"},
    ]
    for params in countryparams():
        td = TanQeebCVDownloader(countryparams)
        td.parse_resume_links
