# takes a folder of txt files for cabled receivers and extracts them into
# a csv file, and then loads them into a raw detection table called
# c_detections_yymm_to_yymm_txt

import os, sys
from StringIO import StringIO
import pandas as pd
import numpy as np
import re

if run_from_ipython():
    from ipywidgets import *
    from IPython.display import display, HTML

# Checks and formats the file to be uploaded
def check_detection_data_integrity(engine, filepath, datatype):
    if not os.path.isfile(filepath):
        print "The path provided does not point to a valid file.\n"

    #The expected columns from the VR2C data files
    #Detection data has fewer and different values, so the headers are
    #set accordingly
    if datatype is 'detection':
        header = ['receiver','sequence', 'datetime',
        'sensor_tag', 'A2D', 'hex']
    elif datatype is 'environment':
        header = ['receiver', 'sequence', 'datetime', 'info', 'detection_count',
        'ping_count', 'line_voltage', 'battery_voltage', 'battery_used',
        'current_consumption', 'temperature', 'detection_memory_used',
        'raw_memory_used', 'tilt_information', 'hex']

    #read into textblock and then replace delimiter values
    with open(filepath, "r") as workfile:
        textlist = workfile.readlines()

    insertion_list = []
    #Sometimes the data is sanitized in different formats. Most cases should
    #fall under the final else: case, which is the VR2C default output
    for line in textlist:
        if '|' in line:
            #trimline = re.sub(r'.*\|', '', line)
            splitline = line.split('|')
            trimline = splitline[-1]
        #Angled brackets are used in various system messages that do not relate
        #to detections or idle readings. They are currently not handled by this
        #script and are discarded outright.
        elif '<' in line:
            trimline = None
        elif '>' in line:
            trimline = None
        else:
            trimline = re.sub(r'[.*Z]?', '', line)
            trimline = trimline[19:]

        if datatype is 'detection':
            if not trimline == None:
                #Check to make sure it is a detection data row
                if trimline.count(',')==5:
                    #If the record has any unexpected symbols in it, don't add
                    #it to the dataframe
                    if not re.match('^.*[!<>/\\\]+.*$', trimline):
                        insertion = trimline.split(',')
                        #check to see if all entries are valid before appending
                        insertion_list.append(insertion)
                    else:
                        print 'The following record could not be parsed due to formatting: \n'
                        print trimline
        elif datatype is 'environment':
            if not trimline == None:
                #Only environmental data update rows will be of this length
                if trimline.count(',')>12:
                    if not re.match('^.*[!<>/\\\]+.*$', trimline):
                        insertion = trimline.strip().split(',')
                        #check to see if all entries are valid before appending
                        insertion_list.append(insertion)
                    else:
                        print 'The following record could not be parsed due to formatting: \n'
                        print trimline

    try:
        df = pd.DataFrame(data=insertion_list[0:], columns=header)
    except:
        print "The data is unable to be inserted into the dataframe."

    #Removing characters from numerical data
    df['hex'] = df['hex'].map(lambda x: x.lstrip('#').rstrip('\n'))
    if datatype is 'environment':
        df['detection_count'] = df['detection_count'].map(lambda x: x.lstrip('DC='))
        df['ping_count'] = df['ping_count'].map(lambda x: x.lstrip('PC='))
        df['line_voltage'] = df['line_voltage'].map(lambda x: x.lstrip('LV='))
        df['battery_voltage'] = df['battery_voltage'].map(lambda x: x.lstrip('BV='))
        df['battery_used'] = df['battery_used'].map(lambda x: x.lstrip('BU='))
        df['current_consumption'] = df['current_consumption'].map(lambda x: x.lstrip('I='))
        df['temperature'] = df['temperature'].map(lambda x: x.lstrip('T='))
        df['detection_memory_used'] = df['detection_memory_used'].map(lambda x: x.lstrip('DU='))
        df['raw_memory_used'] = df['raw_memory_used'].map(lambda x: x.lstrip('RU='))
        df['tilt_information'] = df['tilt_information'].map(lambda x: x.lstrip('XYZ='))
    #Specify dataframe column types where needed
    df['datetime'] = pd.to_datetime(df['datetime'])
    #This is inefficient, but the entries of the dataframe have to be sorted
    #in order for the name of the database table to be appropriate.
    df = df.sort_values(by='datetime', ascending=True)
    return df

# Insert the generated detection dataframe into the database
def insert_detections_into_database(df, engine, tablename, schemaname, chunksize=10000):
    print "Loading data file(s)."
    print "Please wait until the operation is complete."
    sys.stdout.flush()
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        # SQL commands for creating the table and copying over the data
        createtable = ('CREATE TABLE IF NOT EXISTS {0}.{1}( '
                       'receiver VARCHAR, sequence VARCHAR, '
                       'datetime timestamp, sensor_tag VARCHAR, a2d INT, hex VARCHAR)').format(schemaname, tablename)
        copyrecords = 'COPY {0}.{1} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'.format(schemaname, tablename)

        engine.execute(createtable)
        fh = StringIO()
        df.to_csv(fh, index=False, encoding='utf-8')

        fh.seek(0)
        cursor.copy_expert(copyrecords, fh)
        conn.commit()
        sys.stdout.write('\r')

        sys.stdout.write('\n')
        print "File transfer complete.\n"
        total_count = engine.execute('SELECT * FROM {0}.{1}'.format(schemaname, tablename))
        row_count = 0
        for row in total_count:
            row_count = row_count+1
        print "{} total records transferred.".format(row_count)
    except ValueError as e:
        print "Error:", e
        return False
    except:
        print 'Unexpected Error:', sys.exc_info()
        return False
    return True

def insert_environmental_data_into_database(df, engine, tablename, schemaname, chunksize=10000):
    print "Loading data file(s)."
    print "Please wait until the operation is complete."
    sys.stdout.flush()
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        # SQL commands for creating the table and copying over the data
        createschema = ('CREATE SCHEMA IF NOT EXISTS {0}').format(schemaname)
        createtable = ('CREATE TABLE IF NOT EXISTS {0}.{1}( '
                       'receiver VARCHAR, sequence VARCHAR, '
                       'datetime timestamp, info VARCHAR, detection_count INT, '
                       'ping_count INT, line_voltage DECIMAL(4,1), '
                       'battery_voltage DECIMAL(4,1), battery_used DECIMAL(4,1), '
                       'current_consumption DECIMAL(3,1), temperature DECIMAL(4,1), '
                       'detection_memory_used DECIMAL(4,1), raw_memory_used DECIMAL(4,1), '
                       'tilt_information VARCHAR, hex VARCHAR)').format(schemaname, tablename)
        copyrecords = 'COPY {0}.{1} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'.format(schemaname, tablename)

        engine.execute(createschema)
        engine.execute(createtable)
        fh = StringIO()
        df.to_csv(fh, index=False, encoding='utf-8')

        fh.seek(0)
        cursor.copy_expert(copyrecords, fh)
        conn.commit()
        sys.stdout.write('\r')

        sys.stdout.write('\n')
        print "File transfer complete.\n"
        total_count = engine.execute('SELECT * FROM {0}.{1}'.format(schemaname, tablename))
        row_count = 0
        for row in total_count:
            row_count = row_count+1
        print "{} total records transferred.".format(row_count)
    except ValueError as e:
        print "Error:", e
        return False
    except:
        print 'Unexpected Error:', sys.exc_info()
        return False
    return True

# Create new table name for the signified data
def create_new_detection_tablename(engine, tabledata, datatype):
    if not tabledata.empty:
        date1 = tabledata['datetime'].iloc[0]
        date2 = tabledata['datetime'].iloc[-1]
        #The table name will have the date range in YYMM format
        formatteddate1 = str(date1)[2:4]+str(date1)[5:7]
        formatteddate2 = str(date2)[2:4]+str(date2)[5:7]
        if datatype is 'detection':
            tablename = "c_detections_{0}_to_{1}_txt".format(formatteddate1, formatteddate2)
        elif datatype is 'environment':
            tablename = "c_environmental_data_{0}_to_{1}_txt".format(formatteddate1, formatteddate2)
        return tablename
    else:
        print "There are no records, so no tablename is to be generated"
        return "dummy_detection_table_name"

# Returns True if function was called form ipython, used in pretty ipython notebook printing
def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False
