"""
# Analyze Wuzzuf Data
# This data draws on data scraped from Wuzzuf.com related to job advertisement in Egypt to:

# 1. cleans the data so it is useful for regression analysis 
# 2. select out key nouns and words from the requirements section of the job advertisements
# 3. produce word clouds that highlight key skills and qualifications desired for different occupations

# Code was developed for paper "Improving Labor Market Matching in Egypt"
# Code was updated to extract information from the SQl database
# And the archivedpagedata that is stored in various csv files

#### Created by Natalie Chun 
Created:  20 September 2017
Updated:  11 May 2019
"""

# import relevant packages for processing data
from pytz import timezone
import csv
import pandas as pd
import numpy as np
import re
import datetime
from datetime import datetime
import time
import re
import os

import sqlite3
from basepreprocess import BasePreprocessor
from config import FileConfig


class WuzzufPreprocessor(BasePreprocessor):

    def __init__(self):
        self.extdir = os.path.join(FileConfig.EXTDIR, 'wuzzuf')
        print(os.path.join(self.extdir, 'wuzzuf_new.db'))
        self.conn = sqlite3.connect(os.path.join(FileConfig.EXTDIR, 'wuzzuf', 'wuzzuf_new.db'))
        self.cursor = self.conn.cursor()
        self.tz = timezone('Africa/Cairo')
        self.datecur = datetime.now(self.tz)
        self.intdir = os.path.join(FileConfig.INTDIR, 'wuzzuf')
        self.figdir = os.path.join(FileConfig.FIGDIR, 'wuzzuf')
        #self._load_mappings()
        self.datasrc = 'Wuzzuf'

    def _load_mappings(self):
        """Read the following items into dictionary that will be used 
        for mapping industry and job data"""
        with open(os.path.join(self.intdir, 'wuzzuf_industry_list_mapping.csv'), mode='r') as infile:
            reader = csv.reader(infile)
            self.ind_mapping = {rows[0]:rows[1] for rows in reader}
            infile.close()

        with open(os.path.join(self.intdir, 'wuzzuf_job_list_mapping.csv'), mode='r') as infile:
            reader = csv.reader(infile)
            self.job_mapping = {rows[0]:rows[1] for rows in reader}
            infile.close()
        
    def _combine_data(self):
        
        #extract the relevant set of data
        query = '''SELECT * FROM pagedata;'''
        unprocdata = pd.read_sql(query,conn,parse_dates=['postdate','downloaddate'])
        query = '''SELECT * FROM archivedpagedata;'''
        unprocarchiveddata = pd.read_sql(query,conn, parse_dates=['postdate','downloaddate'])
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
        
    def clean_requirements(self, dataset):    
        """Check the text in the requirements section to identify key words to focus on
        it cleans the requirements section data that will be better for processing 
        and analysis"""
        keywords = []
        alltext = []
        notation = ['\"','''\'''',"-","(",")","[","]",";","."]
        for i, row in dataset.iterrows():
            # Replace any non-letter, space, or digit character in the headlines.
            linetext = []
            #note that a lot of the data is stored with '>' to denote different lines 
            text = row['requirements'].decode('utf-8').strip("[").strip("]").split('>')
            #print(text)
            for t in text:
                #print(t)
                t = t.strip()
                t = re.sub(r'^b[\'|\"]','',t)
                t = re.sub(r'\\x[\\x|\w+]','',t)
                t = re.sub(r'[\d]',"",t)
                t = re.sub('\s+'," ",t)
                for n in notation:
                    t = t.replace(n,"")
                t = re.sub(r'(bs/ba|university|bachelors|masters)','bachelor',t)
                t = re.sub(r'leaders[\s|$]','leadership',t)
                t = re.sub(r'[^|\s]ms\s','microsoft',t)
                t = re.sub(r'good looking','good-looking',t)
                t = re.sub(r'hard working','hard-working',t)
                t = re.sub(r'attention to detail','attention-to-detail',t)
                t = t.replace('\\','')
                linetext.append(t)
            alltext.append(' '.join(linetext))
        return(alltext)

    def clean_text(text, MOSTCOMMON = True):
        """Clean the requirements text by removing stop words and focusing on nouns"""
    
        #use the built in list of stop words 
        from nltk.corpus import stopwords
        stop = set(stopwords.words('english'))
    
        #place things into parts of speech and label as noun, verb etc.
        tokenized = nltk.tokenize.word_tokenize(text)
        pos_words = set(nltk.pos_tag(tokenized))
        #print(tokenized)
        #print(text.split())
    
        # eliminate
        morestop = ['experience','user','skills','skill','ability','excellent','years','year','relevant',
                'knowledge','work','minimum','time','command', 'field','degree','familiar','must','strong','good',
               'preferred','plus','able','least','must','using','understanding','background','presentable','candidate',
               'able','ability','least','working','including','related','required','solid','attention']

        clean_split = [w for w in text.split() if w not in stop and w not in morestop and len(w)>=4]
        merged_clean = ' '.join(clean_split)
    
        NOUNS = ['NN', 'NNS', 'NNP', 'NNPS']
        cnt_words = nltk.FreqDist(clean_split)
    
        bigrams = list(nltk.bigrams(clean_split))
        cnt_bigrams = nltk.FreqDist(bigrams)
    
        if MOSTCOMMON is True:
            most_freq_nouns = [w for w, cnt in cnt_words.most_common(30) if nltk.pos_tag([w])[0][1] in NOUNS]
            #print(bigrams)
            most_freq_bigrams = [w for w, cnt in cnt_bigrams.most_common(30) if w[0] in most_freq_nouns or w[1] in most_freq_nouns and w[0] != w[1]]
            #print(cnt_bigrams.most_common(100))
            return(merged_clean,most_freq_nouns,most_freq_bigrams)
        else:
        
            keyunigrams = ['emails','analytics','analytical','research','bachelor','leadership',
                       'interpersonal','communication','knowledge','marketing','management','business','computer','team','english','arabic',
                       'presenting','confident','travel','initiative',
                       'arabic','writing','reading','negotiation','excel','powerpoint', 'flexible','microsoft','persuade','strategic']
            keybigrams = [('meeting', 'clients'),('microsoft','office'),('organizational','ability'),
                      ('time','management'),('problem','solving'),('data','driven'),
                      ('sales','oriented')]
            other_languages = ['Chinese','Turkish','French','German','Russian','Spanish']
    
    def tag_skills(textline,word_tags,bigram_tags):
        """ extracts key skills"""
        cnt_tags = 0
        cleanline, words, bigrams = clean_text(textline)
        for word in words:
            #print(word)
            if word in word_tags:
                cnt_tags += 1
        for bigram in bigrams:
            if bigram in bigram_tags:
                cnt_tags += 1
        return(cnt_tags)
    
    def _clean_col(self, text):
        text = re.sub(r'[\[\]\']', '', text)
        return(text)
    
    def _analyze_requirements(self, df):
        #analyze requirements and count
        tags_nouns_hard = ['experience','knowledge','degree','computer','software','engineering','science','language','excel']
        tags_nouns_soft = ['communication','team','presentation','negotiation','leadership','interpersonal']
        tags_bigrams_soft = [('problem', 'solving'),('work', 'pressure'),('time', 'management'),('attention', 'detail')]
        tags_bigrams_hard = [('microsoft','office')]  
        jobdata['soft_skills'] = [tag_skills(row['clean_requirements'],tags_nouns_soft,tags_bigrams_soft) for i, row in jobdata.iterrows()]
        jobdata['hard_skills'] = [tag_skills(row['clean_requirements'],tags_nouns_hard,tags_bigrams_hard) for i, row in jobdata.iterrows()]
    
    
    def _create_features(self, df):
        """Function creates key features that can be used for analysis."""
        
        experience_min = []
        experience_max = []
        vacancies = []
        regex = {}
        regexterms = ['^(\d) to (\d) years', '^More than (\d) years', '^Less than (\d) years',
                        '^(\d) years', '^(\d) open position']
        
        rt = {cnt: re.compile(regex) for cnt, regex in enumerate(regexterms)}
        
        # Number of years of experience in tuples
        expyears = [
                rt[0].sub(lambda m: (m.group(1), m.group(2)), row['experience_needed']) 
                    if rt[0].search(row['experience_needed']) is not None
                else rt[1].sub(lambda m: (m.group(1), 30), row['experience_needed'])
                    if rt[1].search(row['experience_needed']) is not None
                else rt[2].sub(lambda m: (0, m.group(1)), row['experience_needed'])
                    if rt[2].search(row['experience_needed']) is not None
                else rt[3].sub(lambda m: (m.group(1), m.group(1)), row['experience_needed'])
                    if rt[3].search(row['experience_needed']) is not None
                else (np.nan, np.nan) for i, row in df.iterrows()]
        df['expmin'], df['expmax'] = zip(*df['expyears'])
            
            
        # Number of vacancies
        df['vacancies'] = [rt[4].sub(lambda m: m.group(1), row['vacancies']) 
                    if rt[4].search(row['vacancies']) is not None else 1 for i, row in df.iterrows()]

        # Number of requirements (line numbers)
        df['req_num'] = [len(row['requirements'].decode('utf-8').split('>')) for i, row in df.iterrows()]
                
        # Replace education level if it is found in requirements section
        df['req_bachelors'] = [1 if 'bachelor' in row['requirements'].decode('utf-8') else 0 for i, row in df.iterrows()]
        
        df['days_posted'] = df['downloaddate']-df['postdate']
                
        #clean industry and job values and map them into standard features          
        industries = [[self._clean_col(ind) for ind in row['industries'].decode('utf-8').split(">")] for i, row in df.iterrows()]
        job_roles = [[self._clean_col(role) for role in row['roles'].split(">")] for i, row in df.iterrows()]
        for i, v in enumerate(['jobrole0','jobrole1','jobrole2']):
            jobdata[v] = joblist[i]
            jobdata[v] = jobdata[v].replace(job_mapping)
        for i, v in enumerate(['indtype0','indtype1','indtype2']):
            jobdata[v] = indlist[i]
            jobdata[v] = jobdata[v].replace(ind_mapping)
         
        #create industry and job dummies based on content in multiple columns
        indkeys = set(jobdata['indtype0'])
        jobkeys = set(jobdata['jobrole0'])
    
        for i, ind in enumerate(indkeys):
            jobdata['ind'+str(i)] = [1 if ind in [row['indtype0'],row['indtype1'],row['indtype2']] else 0 for j, row in jobdata.iterrows()]
        for i, job in enumerate(jobkeys):
            jobdata['job'+str(i)] = [1 if job in [row['jobrole0'],row['jobrole1'],row['jobrole2']] else 0 for j, row in jobdata.iterrows()]
        
        # fix addresses
        df['address'] = [row['location'].decode('utf-8').strip("\'") for i, row in df.iterrows()]
        df['address'] = [row['address'] + ", Egypt" if "Egypt" not in row['address'] else row['address'] for i, row in df.iterrows()]
        #obtain location counts of our data
        newlocation = []
        location = ['Alexandria','Assuit','Cairo','Gharbia','Giza','Ismailia','Kafr Alsheikh','Matruh','Monufya','Port Said','Sharqia','Qalubia']
        re_loc = re.compile(r'.*?(%s).*?'% ('|'.join(location)))
        df['province'] = [re_loc.sub(lambda m: m.group(1), row['address']) if re_loc.match(row['address']) is not None else 'Other' for i, row in df.iterrows()]
        
        # pick categorical variables for which we want to create dummies
        catvars = ['job_type','gender','travel_frequency','languages','education_level','career_level','company',
        'province']
        df, dcols = self._create_dummies(self, df, catvars)
         
        #export the processed data to a csv file
        from pytz import timezone
        tz = timezone('Africa/Cairo')
        datecur = datetime.now(tz)
        datenow = datecur.strftime("%m%d%Y")
        df.to_csv('wuzzuf_jobdata_processed_'+datenow+'.csv')
        cols = ['days_posted', 'vacancies', 'expmin', 'expmax', 'req_num', 'req_bachelors'] + dcols
        return(df, cols)
        
    def generate_stats(self):
        """Create additional variables that are useful for generating statistics."""
    
        df = pd.read_sql(query, self.conn)
        df, cols = self._create_features(df)
        statcols = cols
        self._create_stats(df, 'industry', statcols, 'uid')
        
    def run_all(self):
    
        print("Running WuzzufPreprocessor on date (%s)" % (self.datecur))
        print("="*100)
    
        self._document_missing()
        self._document_jobads('jobadpage','uid','industries')
        #self.generate_stats()
        
        """
        unprocdata = self._combine_data()
        unprocdata['clean_requirements'] = clean_requirements(unprocdata)
        cleandata = clean_jobdata(unprocdata)
        print(cleandata.head())

        #obtain location counts of our data
        print(cleandata['newlocation'].value_counts())
        print(cleandata['jobrole0'].value_counts())
        print(cleandata['jobrole1'].value_counts())

        #STEP 3:  Print Out Key Summary Statistics of Our New Data
        countobs = cleandata.groupby('downloaddate').count()
        summarystats = cleandata.groupby('downloaddate').aggregate([np.mean, np.std])
        print(countobs)
        print(summarystats)
        countobs.to_csv("Wuzzuf_variablecounts.csv")
        summarystats.to_csv('Wuzzuf_summarystats.csv')

        #STEP 4:  START TAGGING KEY WORDS ASSOCIATED WITH DIFFERENT JOB TYPES
        #This can be important for creating word clouds, but also for coming up with measures on the relative complexity of jobs
        print("Run Time: {}".format(time.time()-start_time))
        """
        self.conn.close()
        
if __name__ == "__main__":

    wp = WuzzufPreprocessor()
    wp.run_all()