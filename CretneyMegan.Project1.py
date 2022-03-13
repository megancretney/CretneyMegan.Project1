# -*- coding: utf-8 -*-
"""
Project One

@author: Megan Cretney
"""

################################ imports #####################################
import json
import requests

import os
import pymysql
import mysql.connector
from sqlalchemy import create_engine

import pandas as pd
import matplotlib.pyplot as plt

import csv
##############################################################################

### Will need this for later when using SQLite 
engine = create_engine('sqlite://', echo = False)

##############################################################################
######### 1.	Fetch / download / retrieve a remote data file by URL ##########
##############################################################################

try:
    
    ## Open the data using a URL API address and save it to a variable
    response = requests.get("https://opendata.arcgis.com/datasets/d1877e350fad45d192d233d2b2600156_6.geojson")

    data = response.json()
    
except:
    print("Unable to fetch data from URL. Please make sure address is correct")

##############################################################################
#### 2. Convert the general format and data structure of the data source ##
##############################################################################

### Get users input
outputSource = input("The Data Came in as JSON would you like to see the Orginal Data as a CSV or a SQL Database Table? (Enter CSV or SQL):  ")

try:
### First convert to dataframe in python
    justFeatures = data['features'] # We only need the features section on the data
    df = pd.json_normalize(justFeatures)

#### Dataframe to CSV ####
## Check to see if the user wants the data as a CSV 
## Convert to CSV and print the results
    if outputSource == 'CSV':
        df.to_csv('project1_csv.csv')
        file = open("project1_csv.csv")
        CSV_FILE = csv.reader(file)
        header = next(CSV_FILE)
        print(header)
        rows = []
        for row in CSV_FILE:
            rows.append(row)
        print(rows)
        file.close()

### To SQL ####
## Check to see if the user wants the data as a SQL Database
## Using Sqlite to create a database
    elif(outputSource == 'SQL'):
 
        df.to_sql('Crime_Data', con = engine)
        
        print(engine.execute("SELECT * FROM Crime_Data").fetchall())

  
except:
    print("Please Type 'CSV' or 'SQL' ! :)")
    
##############################################################################
##################   3.	Modify the number of columns   #######################
##############################################################################
try:
    # Get rid of unecassary columns and rename the rest of the columns
    df2 = df.drop('type', 1)
    df = df2.drop('geometry', 1)
    df2 = df2.rename(columns = {'properties.RecordID':'RecordID', 'properties.Offense':'Offense', 
                     'properties.IncidentID':'IncidentID', 'properties.BlockNumber':'BlockNumber',
                     'properties.StreetName':'StreetName', 'properties.Agency':'Agency', 
                     'properties.DateReported':'DateRep', 'properties.HourReported':'HoursRep',
                     'properties.ReportingOfficer':'Officer'})
except:
    print("Unable to drop extra columns... Please Try again")
    
##############################################################################   
###### 4.	The converted (new) file should be written to SQL database ###### 
##############################################################################

# Convert the new data to a sql database
df2.to_sql('New_Crime_Data', con = engine)
# Print out the new database
print(pd.read_sql("SELECT * FROM New_Crime_Data", engine))

##############################################################################
###                          5. Summary of Data                           ###
##############################################################################
print('Rows = ', pd.read_sql("SELECT COUNT(*) FROM New_Crime_Data", engine))
print('Columns = ', pd.read_sql("SELECT COUNT(*) FROM pragma_table_info('New_Crime_Data')", engine))
