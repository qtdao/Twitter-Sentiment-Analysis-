#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 12:57:29 2018

@author: tuandao
"""

#%%
import sqlite3 as lite
import sys

try: 
    con = None
    
    con = lite.connect('/Users/tuandao/datascience/tweetdb.db')
    
    cur = con.cursor()
    
    cur.execute('SELECT SQLITE_VERSION()')
    
    data = cur.fetchone()
        
    print("SQLite version; %s" % data) 
    
    cur.execute('SELECT * FROM tweets')
    
    cur.fetchone()
    
    print(cur.fetchone())
  
except lite.Error as e:
    print("Error %s:" % e.args[0])
    sys.exit(1)
#%%
finally:
    if con:
        con.close()
#%%
        
'''    
    cur = con.cursor()    
    cur.execute("DROP TABLE IF EXISTS Cars")
    cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
    cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
    cur.execute("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
    cur.execute("INSERT INTO Cars VALUES(3,'Skoda',9000)")
    cur.execute("INSERT INTO Cars VALUES(4,'Volvo',29000)")
    cur.execute("INSERT INTO Cars VALUES(5,'Bentley',350000)")
    cur.execute("INSERT INTO Cars VALUES(6,'Citroen',21000)")
    cur.execute("INSERT INTO Cars VALUES(7,'Hummer',41400)")
    cur.execute("INSERT INTO Cars VALUES(8,'Volkswagen',21600)")
'''