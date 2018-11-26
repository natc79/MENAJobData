"""
This code creates key databases for various data that is being downloaded.

Author: Natalie Chun
Created:  November 22, 2018

"""


import sqlite3

def update_table(tablename,newtablequery,insertstatement):
    """Update table"""
    
    conn = sqlite3.connect("egyptOLX.db")
    c = conn.cursor()

    query ='''PRAGMA table_info({});'''
    print(c.execute(query.format(tablename)).fetchall())    
    
    query = "ALTER TABLE {} RENAME TO {}_temp;"
    c.execute(query.format(tablename,tablename))
    
    c.execute(newtablequery)
    
    query = "INSERT INTO {} ({}) SELECT {} FROM {}_temp;"
    c.execute(query.format(tablename,insertstatement,insertstatement,tablename))
    
    query ='''PRAGMA table_info({});'''
    print(c.execute(query.format(tablename)).fetchall())
    
    query = '''DROP TABLE IF EXISTS {}_temp;'''
    c.execute(query.format(tablename))
    
    temp = c.execute('''SELECT * FROM {};'''.format(tablename)).fetchall()
    print("Number of entries in converted table {}: {}".format(tablename,len(temp)))
    
    conn.commit()
    conn.close()


def reset_tables():
    """Reset all of the tables."""

    for i, t in enumerate(tables):
        deletequery = '''DROP TABLE IF EXISTS {};'''
        c.execute(deletequery.format(table[i]))
        c.execute(querycreate[t])
        query ='''PRAGMA table_info({});'''
        print(c.execute(query.format(table[i])).fetchall())
    conn.commit()
    conn.close()


def get_olx_table_schema():
    """Set schema for relevant OLX tables."""
    
    # in sqlite the only option to convert table is to rename temporary table, create new table, and then drop old 
    tables = {}

    tables['regionadcounts'] = '''CREATE TABLE IF NOT EXISTS regionadcounts (
        downloaddate DATE,
        downloadtime VARCHAR(5),
        country VARCHAR(15),
        region VARCHAR(50),
        freg VARCHAR(50),
        subregion VARCHAR(50),
        fsubreg VARCHAR(50),
        totalregposts INTEGER,
        subposts INTEGER,
        PRIMARY KEY(downloaddate,region,subregion));'''
    
    tables['regionjobadcounts']= '''CREATE TABLE IF NOT EXISTS regionjobadcounts 
        (downloaddate DATE,
        downloadtime VARCHAR(5),
        country VARCHAR(15),
        region VARCHAR(50),
        freg VARCHAR(50),
        subregion VARCHAR(50),
        fsubreg VARCHAR(50),
        sector VARCHAR(50),
        urlregsector VARCHAR(50),
        totalposts INTEGER,
        PRIMARY KEY(downloaddate,region,subregion,sector));'''
    
    tables['jobadpageurls'] = '''CREATE TABLE IF NOT EXISTS jobadpageurls (
        country VARCHAR(15),
        region VARCHAR(50),
        freg VARCHAR(50),
        subregion VARCHAR(50),
        fsubreg VARCHAR(50),
        jobsector VARCHAR(50),
        postdate DATE,
        uid INTEGER,
        i_photo INTEGER,
        i_featured INTEGER,
        href VARCHAR(50),
        PRIMARY KEY(uid,postdate));'''

    tables['jobadpage'] = '''CREATE TABLE IF NOT EXISTS jobadpage (
        downloaddate DATE,
        downloadtime VARCHAR(5),
        country VARCHAR(15),
        uid INTEGER,
        postdate DATE,
        posttime VARCHAR(5),
        pageviews INTEGER,
        title VARCHAR(70),
        experiencelevel VARCHAR(50),
        educationlevel VARCHAR(50),
        type VARCHAR(50),
        employtype VARCHAR(50),
        compensation INTEGER,
        description VARCHAR(5000),
        textlanguage VARCHAR(2),
        userhref VARCHAR(30),
        username VARCHAR(50),
        userjoinyear INTEGER,
        userjoinmt VARCHAR(3),
        emailavail INTEGER,
        phoneavail INTEGER,
        stat VARCHAR(10),
        PRIMARY KEY(downloaddate,uid,postdate));'''
        
    return(tables)
        
def get_wuzzuf_table_schema():
    """Set schema for relevant Wuzzuf tables."""
    tables = {}

    tables['jobadpageurls']='''CREATE TABLE IF NOT EXISTS jobadpageurls (
        country VARCHAR(20),
        uid INTEGER,
        href VARCHAR(200),
        postdatetime VARCHAR(100),
        postdate DATE,
        PRIMARY KEY(uid,postdate));'''
	
    tables['jobadpage']='''CREATE TABLE IF NOT EXISTS jobadpage (
		country VARCHAR(20),
        uid INTEGER,
		postdate DATE,
		posttime VARCHAR(5),
		downloaddate DATE,
		downloadtime VARCHAR(5),
		stat VARCHAR(10),
		jobtitle VARCHAR(50),
		company VARCHAR(50),
		location VARCHAR(50),
		num_applicants INTEGER,
		num_vacancies INTEGER,
		num_seen INTEGER,
		num_shortlisted INTEGER,
		num_rejected INTEGER,
		experience_needed VARCHAR(50),
		career_level VARCHAR(50),
		job_type VARCHAR(50),
		salary VARCHAR(50),
		education_level VARCHAR(50),
		gender VARCHAR(10),
		travel_frequency VARCHAR(20),
		languages VARCHAR(30),
		vacancies VARCHAR(15),
		roles VARCHAR(300),
		keywords VARCHAR(100),
		requirements VARCHAR(5000),
		industries VARCHAR(100),
		PRIMARY KEY(uid,postdate,downloaddate));
		'''
    return(tables)   
     
def get_tanqeeb_table_schema():
    tables = {}
    tables['jobadpageurls'] = """CREATE TABLE IF NOT EXISTS jobadpageurls (
        country VARCHAR(15),
        cat VARCHAR(15),
        subcat VARCHAR(15),
        uniqueid VARCHAR(50),
        dataid INTEGER,
        i_featured INTEGER,
        postdate VARCHAR(10),
        title VARCHAR(25),
        href VARCHAR(100),
        description VARCHAR(500),
        PRIMARY KEY (uniqueid, postdate));"""
     
    tables['jobadpage'] = """CREATE TABLE IF NOT EXISTS jobadpage (
        country VARCHAR(15),
        uniqueid VARCHAR(50),
        postdate VARCHAR(10),
        location VARCHAR(20),
        jobtype VARCHAR(10),
        company VARCHAR(20),
        reqexp VARCHAR(10),
        salary VARCHAR(10),
        education VARCHAR(20),
        title VARCHAR(25),
        pubimg VARCHAR(50),
        description VARCHAR(5000),
        PRIMARY KEY (uniqueid, postdate));
    """     
    return(tables)
        
if __name__ == "__main__":
    pass