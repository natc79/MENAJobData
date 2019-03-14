"""
Purpose:  This class consists of functions to scrape job advertisement data
from OLX websites.  It is developed to store information in a SQL database
that contains historical data on page views and other information.

Author:  Natalie Chun
Created: 22 November 2018
"""


from pytz import timezone
import datetime
import time
import random
import sqlite3
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import os
from basedownloader import BaseDownloader
from config import FileConfig
from create_databases import get_olx_table_schema

class OLXDownloader(BaseDownloader):
    
    def __init__(self, params):
        #super(OLXDownloader, self).__init__(params)
        self.conn = sqlite3.connect(os.path.join(FileConfig.EXTDIR, 'olx', "OLX.db"))
        self.cursor = self.conn.cursor()
        self.country = params["country"]
        self.tz = timezone(params["timezone"])
        self.url = params["url"]
        self.datecur = datetime.datetime.now(self.tz).date()
        self._create_table_schema(get_olx_table_schema())
        print("Start Time: {}".format(self.datecur))
        
    def get_region_data(self, debug=False):
        """Gets aggregated counts of advertisements by different regions in given country."""
        
        datetimecur = datetime.datetime.now(self.tz)
        downloaddate = datetimecur.strftime('%Y-%m-%d')
        downloadtime = datetimecur.strftime('%H:%M')

        url = self.url + 'sitemap/regions/'
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        soup = BeautifulSoup(response, 'html.parser')

        name_box = soup.find('div', attrs={'class': 'content text'})
        regions = name_box.find_all('div', attrs={'class':'bgef pding5_10 marginbott10 margintop20 clr'})
        subregions = name_box.find_all('div',attrs={'class':"clr marginbott10"})

        regionname = []
        totalposts = []
        subregname = []
        subposts = []
        fregname = []
        fsubregname = []
        data = []
    
        for i, subreg in enumerate(subregions):
            if debug and i > 2:
                break
            #print(regions[i].get_text().strip())
            region = regions[i].get_text().strip()
            temp = region.split(' (')
            regionname.append(temp[0])
            totalposts.append(temp[1].strip(')')) 
            fregname.append(re.sub('\s+(\+\s)?','-',regionname[i].lower()))    
            #print(subreg)
            text = subreg.find_all('li')
            #print(text)
            for t in text:
                temp = t.get_text().strip()
                temp = temp.replace('\n','').split('(')
                #print(temp)
                subregname = temp[0].strip()
                subposts = temp[1].strip(')')
                #print(subregname,subposts)
                fsubregname = re.sub('''[\s(\+\s)?|\'|\.\s]''','-',subregname.lower())
                fsubregname = re.sub('[-](-)?(-)?','-',fsubregname)
                row = [downloaddate, downloadtime, self.country, regionname[i], fregname[i], subregname, fsubregname,totalposts[i], subposts]
                query = '''INSERT OR IGNORE INTO regionadcounts (downloaddate, downloadtime, country, region, freg, subregion, fsubreg, totalregposts, subposts) VALUES (?,?,?,?,?,?,?,?,?) ;'''
                self.cursor.execute(query, row)
    
        #commit entries to the database
        self.conn.commit()
        
    def get_region_jobdata(self, debug=True):
        """Loop through the key industries and regions to investigate the counts of postings
        under each heading"""  
    
        # SELECT ONLY MOST RECENT DOWNLOAD DATE OF DATA
        query = '''SELECT DISTINCT country, region, freg, subregion, fsubreg FROM regionadcounts WHERE country='{}' AND DATE(downloaddate) >= DATE('{}', '-2 days');'''
        data = pd.read_sql(query.format(self.country, self.datecur.strftime('%Y-%m-%d')),self.conn)
    
        #loop through the different geographic regions to get job data
        for i, reg in data.iterrows():
            if debug and i > 2:
                break
            datetimecur = datetime.datetime.now(self.tz)
            downloaddate = datetimecur.strftime('%Y-%m-%d')
            downloadtime = datetimecur.strftime('%H:%M')
            region = reg['region']
            freg = reg['freg']
            subregion = reg['subregion']
            fsubreg = reg['fsubreg']
            url = self.url + 'jobs-services/' + fsubreg + '/'
            subregsector, subreghref = self.get_job_urls(url)
        
            #now want to output this data into the SQL database
            for sector, numposts in subregsector.items():
                #print(subreghref[sector])
                rowvalues = [downloaddate,downloadtime,self.country,region,freg,subregion,fsubreg,sector,subreghref[sector],numposts]
                query = '''INSERT OR IGNORE INTO regionjobadcounts (downloaddate, downloadtime, country, region, freg, subregion, fsubreg, sector, urlregsector, totalposts) VALUES (?,?,?,?,?,?,?,?,?,?);'''
                self.cursor.execute(query,rowvalues)
         
                if i % 100 == 0:
                    print(i,rowvalues)
            # sleep to make sure not too many requests are being made
            time.sleep(random.randint(1,3))
        
        self.conn.commit()
         
    def get_job_urls(self, url):
        """Obtains all sector variables and associated reference links that will be input into our database"""
    
        sector = {}
        href = {}
    
        req = urllib.request.Request(url)
        try:
            response = urllib.request.urlopen(req)
        except:
            #certain regions have no job postings
            return([sector,href])
        
        soup = BeautifulSoup(response, 'html.parser')

        #get counts of number of jobs in different areas
        name_box = soup.find_all('div', attrs={'class': 'wrapper'})
    
        for name in name_box:
            #print(name)
            newnames = name.find_all('a', attrs={'class' : 'topLink tdnone '})
            if len(newnames) > 0:
                for i, n in enumerate(newnames):
                    sect = n.find('span', attrs='link').get_text().strip()
                    cnt = n.find('span', attrs='counter nowrap').get_text().strip().replace(',','')
                    #export a tuple rather than dictionary
                    sector[sect] = cnt
                    href[sect] = n['href']

        return([sector,href])
        

    def get_jobpage_urls(self, region, freg, subregion, fsubreg, jobsector, url, lastdownloaddate):
        """Get all of the job page urls from subregion-sector listings.  Download only based one
        last download date
        """
 
        urllist = []
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        soup = BeautifulSoup(response, 'html.parser')
    
        #now find out the total number of pages available
        try:
            nextpage = soup.find('div',attrs={'class':'pager rel clr'})
            temp = nextpage.find('input',attrs={'type':"submit"})['class']
            totalpages = re.search(r'(\d+)',str(temp[1])).group(1)
            #print(temp,totalpages)
        except:
            totalpages = 1
    
        #set the minimum addate to the current date
        datetimecur = datetime.datetime.now(self.tz)
        minaddate = datetimecur.date()
        cnt=1
        #check that the minumum ad date for a page is greater than the last downloaddate
        while(minaddate >= lastdownloaddate and cnt <= int(totalpages)):
                
            newurl = url
            if cnt > 1:
                newurl = url + '?page='+str(cnt)
            #print(cnt, newurl)
            req = urllib.request.Request(newurl)
            response = urllib.request.urlopen(req)
            soup = BeautifulSoup(response, 'html.parser')
        
            #get the current time
            datetimecur = datetime.datetime.now(self.tz)
            datecur = datetimecur.strftime("%Y-%m-%d")
    
            adlinks = soup.find_all('div',attrs={'class':'ads__item__info'})
            adphotos = soup.find_all('div',attrs={'class':"ads__item__photos-holder"})
        
            #now loop through all of the relevant data and grab the ad information
            for i, val1 in enumerate(adphotos):
                #print(i)
                val2 = adlinks[i]
                temp1 = val1.find('img')
                temp2 = val1.find('span',attrs={'class':"ads__item__paidicon icon paid"})
                temp3 = val1.find('a',attrs={'data-statkey':'ad.observed.list'})
                #there are some really old ads that are no longer active and do not have ids so we just skip over this
                if temp3 is not None:
                    temp3a = temp3['class'][2]
                    #print(temp3a)
                    temp3b = re.search(r'{id:(\d+)}',temp3a)
                    uid = temp3b.group(1)
                    #print(temp1['src'])
                    temp4 = val2.find('a',attrs={'class':"ads__item__title"})
                    #print(temp4['href'])
                    urllinkshort = temp4['href'].split('/ad/')[1]
                    temp5 = val2.find('p',attrs={'class':'ads__item__date'})
                    tempdate = temp5.get_text().strip()
                    yr = datetimecur.year
                    mt = datetimecur.month
                    day = datetimecur.day
                    if 'Today' in tempdate:
                        postdate = datecur
                    elif 'Yesterday' in tempdate:
                        tempdatetime = datetimecur - datetime.timedelta(days=1)
                        postdate = tempdatetime.strftime("%Y-%m-%d")
                    else:
                        #print(tempdate)
                        months = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                        tempdate2 = re.search(r'(\d+)\s+(\w+)',tempdate) 
                        day = tempdate2.group(1)
                        mt = months[tempdate2.group(2)]
                        #ads stay alive for three months tops need to catch cases when cross over between one year to next
                        if datetimecur.month - mt >= 0:
                            yr = datetimecur.year
                        else:
                            yr = datetimecur.year - 1
                        postdate = str(yr)+'-'+str(mt)+'-'+str(day)
        
                    i_photo = 0
                    i_featured = 0
                    if 'jobs-services-thumb.png' not in temp1['src']:
                        i_photo = 1
                    if temp2 is not None:
                        i_featured = 1
                    rowvalues = [self.country, region, freg, subregion, fsubreg, jobsector, postdate, uid, i_photo,i_featured, urllinkshort]
                    #print(rowvalues)
                    query = '''INSERT OR IGNORE INTO jobadpageurls (country, region, freg, subregion, fsubreg, jobsector, postdate, uid, i_photo, i_featured,
                    urllinkshort) VALUES(?, ?,?,?,?,?,?,?,?,?,?);'''
                    self.cursor.execute(query,rowvalues)
                    self.conn.commit()
    
            #now store the last date retrieved as the midaddate
            #print(yr,mt,day)
            try:
                minaddate = datetime.date(int(yr),int(mt),int(day))
            except:
                minaddate = datetimecur.date()
            cnt+=1
            time.sleep(random.randint(1,5))
        self.conn.commit()
        
    def get_jobpage(self, uid, postdate, url, translation=False):
        """Get the data from each job page and insert into database"""
    
        data = {}
        cols = ['downloaddate', 'downloadtime', 'country', 'uid', 'postdate', 'posttime', 'pageviews', 'title',
            'Experience Level', 'Education Level', 'Type', 'Employment Type', 'Compensation', 'content', 'texttype',
            'userhref', 'username', 'userjoinmonth', 'userjoinyear', 'emailavail', 'phoneavail', 'stat']
        
        fields = ['Experience Level','Employment Type','Education Level','Type','Compensation']
        
        ### note want to add in the actual time download if we are to use the page views as proxy    
        datetimecur = datetime.datetime.now(self.tz)
        data['country'] = self.country
        data['uid'] = uid
        data['postdate'] = postdate
        data['downloaddate'] = datetimecur.strftime('%Y-%m-%d')
        data['downloadtime'] = datetimecur.strftime('%H:%M')
        
        response = self._request_until_succeed(url)
        if response is not None:
            #get content for ad posting data and check if available as some are no longer available
            soup = BeautifulSoup(response, 'html.parser')
            addata = soup.find('span',attrs={'class':'pdingleft10 brlefte5'})
            
        data['stat'] = 'NOT FOUND' if response is None else 'CLOSED' if addata is None else 'OPEN' 
  
        if data['stat'] in ['NOT FOUND','CLOSED']:
            row = [data[col] if col in data else np.nan for col in cols]
            return(row)
        
        #scrape the page if it does exist
        addata = addata.get_text().strip()
        #print(addata)
        m = re.search(r"at (\d+:\d+, \d+ \w+ \d+), Ad ID: (\d+)",addata)
        date = m.group(1)
        adid = m.group(2)
        dateval = datetime.datetime.strptime(date,'%H:%M, %d %B %Y')
        data['postdate'] = dateval.strftime('%Y-%m-%d') # best time format for spreadsheet programs
        data['posttime'] = dateval.strftime('%H:%M')
    
        #get title of job advertisement
        temptitle = soup.find('div',attrs={'class':"clr offerheadinner pding15 pdingright20"})
        title = temptitle.find('h1').get_text()
        content = soup.find('div', attrs={'class':"clr", 'id':'textContent'}).get_text().strip()

        #translate the text as needed
        if translation:
            import googletrans
            translator = googletrans.Translator()
            data['content'] = translator.translate(content).text.replace('\r',' ').replace('\n','>').encode('utf-8')
            data['title'] = translator.translate(title).text.encode('utf-8')
            data['texttype'] = 'EN'
        else:
            data['content'] = content.replace('\r',' ').replace('\n','>').encode('utf-8')
            data['title'] = title.encode('utf-8')
            data['texttype'] = 'AR'
    
        #get main content related to job
        name_box = soup.find_all('div', attrs={'class': "clr descriptioncontent marginbott20"})
    
        for name in name_box:
            #print(name)
            newnames = name.find_all('td', attrs={'class' : 'col'})
            #print(newnames)
            for name in newnames:
                cat = name.find('th').get_text().strip()
                catval = name.find('td').get_text().strip()
                data[cat] = catval
            
        views = soup.find_all('div',attrs={'class':'pdingtop10'})
        for v in views:
            if 'Views' in str(v):
                m = re.search(r"Views:<strong>(\d+)</strong>", str(v))
                data['pageviews'] = int(m.group(1))
                #print(num_views)
            
        #get content related to compensation/price
        comp = soup.find('div', attrs={'class': "pricelabel tcenter"})
        if comp is not None:
            data['Compensation'] = comp.get_text().strip()
        if 'Compensation' in data:
            data['Compensation'] = re.sub(r'(\d+.*?\d+)(.*?)', lambda m: m.group(1), data['Compensation'])
            data['Compensation'] = re.sub(r'[^0-9]','', data['Compensation'])
            try:
                data['Compensation'] = int(data['Compensation'])
            except:
                data['Compensation'] = ''
                
        #get content related to identity of user/poster of ad
        user = soup.find('div', attrs={'class':'user-box'})
        if user is not None:
            userhref = user.find('a')['href']
            data['username']=user.find('p', attrs={'class':'user-box__info__name'}).get_text().strip().encode('utf-8')
            data['userjoindate']=user.find('p', attrs={'class':'user-box__info__age'}).get_text().strip()
            m=re.search(r'On site since\s+(\w+)\s+(\d+)',data['userjoindate'])
            data['userjoinmonth'] = m.group(1)
            data['userjoinyear'] = int(m.group(2))
    
        #email available?
        emailinfo = soup.find('div', attrs={'class':"contactbox innerbox br3 bgfff rel"})
        data['emailavail'] = 1 if emailinfo is not None and 'Email Seller' in emailinfo.get_text() else 0

        #phone available?
        phoneinfo = soup.find('div', attrs={'class':"contactbox-indent rel brkword"})
        data['phoneavail'] = 1 if phoneinfo is not None and 'Show phone' in phoneinfo.get_text().strip() else 0

        row = [data[col] if col in data else np.nan for col in cols]
        #OLX field for compensation just got changed (17-Dec-2017)
        return(row)
    
    def check_changes_region(self, datast=0, debug=True):
        """Select only region sectors where total posts have changed at least once over the last 5 days"""
        
        c = self.cursor
        query = """SELECT DISTINCT a.country, a.region, a.freg, a.subregion, a.fsubreg, 
                    a.sector, a.urlregsector 
                FROM regionjobadcounts a 
                INNER JOIN regionjobadcounts b 
                    ON a.region = b.region AND a.subregion = b.subregion AND a.sector = b.sector 
                WHERE a.country = '%s' 
                    AND ((DATE(a.downloaddate,'+5 DAYS') >= (SELECT DISTINCT MAX(DATE(downloaddate)) FROM regionjobadcounts) 
                    AND (a.totalposts != b.totalposts) AND DATE(a.downloaddate,'-2 DAYS') == DATE(b.downloaddate)) OR 
                (a.fsubreg NOT IN (SELECT DISTINCT fsubreg FROM jobadpagedata
                WHERE a.country = country AND country = '%s' AND a.region = region AND a.freg = freg AND a.subregion = subregion AND a.fsubreg = fsubreg AND a.downloaddate = downloaddate)))
                ;""" % (self.country, self.country)
        regsector = c.execute(query).fetchall()
        print("Region-sectors to grab: {}".format(len(regsector)))
        cols = ['country','region','freg','subregion','fsubreg','sector','urlregsector']
        for i, reg in enumerate(regsector[datast:]):
            if debug and i > 2:
                break
            d = {col: reg[n] for n, col in enumerate(cols)}
            query = '''SELECT uid FROM jobadpageurls WHERE country='{}' AND fsubreg = '{}' AND jobsector = '{}';'''
            ids = c.execute(query.format(self.country, d['fsubreg'],d['sector'])).fetchall()
            query = '''SELECT MAX(DATE(downloaddate)) FROM jobadpagedata WHERE country='{}' AND uid IN (SELECT uid FROM jobadpageurls WHERE country='{}' AND fsubreg = '{}' AND jobsector = '{}');'''
            lastdate = c.execute(query.format(self.country, self.country, d['fsubreg'],d['sector'])).fetchall()[0][0]
            query = '''SELECT COUNT(*) FROM jobadpageurls WHERE country='{}' AND fsubreg == '{}' AND jobsector == '{}';'''
            oldnumentries = c.execute(query.format(self.country, d['fsubreg'],d['sector'])).fetchall()[0][0]
            #print("Old numentries: {}".format(oldnumentries))
            if lastdate is None:
                #if no data is in the database lets insert from X days ago
                newdate = self.datecur - datetime.timedelta(days=30)
                dateval = datetime.date(newdate.year,newdate.month,newdate.day)
            else:
                date = lastdate.split(' ')[0]
                temp = date.split('-')
                dateval = datetime.date(int(temp[0]),int(temp[1]),int(temp[2]))
            #if there is data in the database lets only insert data posted after the last date downloaded
            self.get_jobpage_urls(d['region'],d['freg'],d['subregion'],d['fsubreg'],d['sector'],d['urlregsector'],dateval)
            query = '''SELECT COUNT(*) FROM jobadpageurls WHERE country='{}' AND fsubreg == '{}' AND jobsector == '{}';'''
            newnumentries = c.execute(query.format(self.country, d['fsubreg'],d['sector'])).fetchall()[0][0]
            if i % 1000 == 0:
                print("Last Download Date for sub-region {} and sector {}: {}".format(d['fsubreg'],d['sector'],lastdate))
                print("Number new pages entered into jobadpageurls for subregion {} and sector {}: {}".format(d['fsubreg'],d['sector'],newnumentries-oldnumentries))
        self.conn.commit()
  
    def get_new_page_data(self, debug=False):
        """Only get new data where it is the most recent and status is open.  Ads in
        OLX are live for 3 months so if we rotate through for at least 15 weeks this should cover
        everything.
        """
        
        jobpageurllist = []
        for i in range(0,15):
            d = i*7
            query = '''SELECT DISTINCT uid, postdate, urllinkshort, country
                        FROM jobadpageurls 
                        WHERE country = '%s' AND DATE(postdate) = DATE('%s','-%d days') 
                        AND uid NOT IN 
                        (SELECT DISTINCT uid 
                        FROM jobadpagedata WHERE country='%s' 
                        AND (stat = 'CLOSED' OR DATE(postdate) <= DATE('%s','-93 days')));''' % (self.country, self.datecur.strftime('%Y-%m-%d'), d, self.country, self.datecur)
            temp = self.cursor.execute(query).fetchall()
            jobpageurllist = jobpageurllist + temp
        print("Number of pages to query: {}".format(len(jobpageurllist)))
        #print(jobpageurllist[0:10])
        
        for i, urlinfo in enumerate(jobpageurllist):
            if debug and i > 2:
                break
            print(urlinfo)
            query = '''INSERT OR IGNORE INTO jobadpagedata (downloaddate, downloadtime, country, uid, postdate, posttime, pageviews, title, experiencelevel, educationlevel, type, employtype, compensation, description, textlanguage, userhref, username, userjoinmt, userjoinyear, emailavail, phoneavail, stat)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'''
            url = self.url + 'ad/'+urlinfo[2]
            rowvalues = self.get_jobpage(urlinfo[0], urlinfo[1], url, translation=False)
            self.cursor.execute(query, rowvalues)
            self.conn.commit() 

    def run_all(self, debug=False):
        """Run key operations to update database."""
        print("Running OLXDownloader for %s on date (%s)" % (self.country, self.datecur))
        print("="*100)
        starttime = time.time()
        #write one entry per day to the OLXregiondata job database
        lastdownloaddate = self._last_download_date('regionadcounts','downloaddate')
        if lastdownloaddate < self.datecur:
            self.get_region_data(debug=debug)
        lastdownloaddate = self._last_download_date('regionjobadcounts','downloaddate')
        if lastdownloaddate < self.datecur:
            self.get_region_jobdata(debug=debug)
            print("Run time for get_region_jobdata: {}".format(time.time()-starttime))
        regsector = self.check_changes_region(debug=debug)
        self.get_new_page_data(debug=debug)
        print("Run time for get_new_page_data: {}".format(time.time()-starttime))
        self._display_db_tables()
        self._archive_database('jobadpagedata', 'jobadpageurls',maxdays=90)
        print("Total run time: {}".format(time.time()-starttime))
 
if __name__ == "__main__":
    countryparams = [
        {"country":"jordan", "url":"https://olx.jo/en/", "timezone":"Asia/Amman"},
        {"country":"egypt", "url":"https://olx.com.eg/en/", "timezone":"Africa/Cairo"}
    ]
    for params in countryparams:
        od = OLXDownloader(params)
        od.run_all()