import pandas as pd
import geopandas as gpd
#from nyc_geoclient import Geoclient #python wrapper for geoclient
import sys
import warnings
import subprocess, os, re
from itertools import product
import numpy as np
import matplotlib.pyplot as plt

from urllib.parse import urlencode
from requests import get

'''
include these lines at the begining of program to import this file:
import sys
sys.path.insert(0,'/home/deena/Documents/data_munge/ModaCode/')
import moda
'''

import cx_Oracle
import databridge as db

def databridge(sqlquery,encrypted='no'):
    '''
    pass an sql query to databridge and return the output in a dataframe
    encrypted = yes, no, or oca
    '''
    
    if encrypted =='no':
        host = db.host
        port = db.port
        service = db.service
        user = db.user
        pswd = db.pswd
        
    elif encrypted =='yes':
        host = db.encr_host
        port = db.encr_port
        service = db.encr_service
        user = db.user
        pswd = db.pswd
        
    elif encrypted =='oca':
        host = db.hostOCA
        port = db.portOCA
        service = db.serviceOCA
        user = db.userOCA
        pswd = db.pswdOCA
     
    
    # open databridge connection
    con = cx_Oracle.connect(user+'/'+pswd+'@'+host+':'+port+'/'+service)
    cur = con.cursor()
    # query
    cur.execute(sqlquery)
    # save query to dataframe
    df = pd.DataFrame(cur.fetchall())
    df.columns = [rec[0] for rec in cur.description]
    # close connection
    con.close()
    return df

from sodapy import Socrata
import opendata as od

def socrata():
    '''
    return an open data dataset given by uid
    needed for datasets that are not public
    '''
    request = Socrata('nycopendata.socrata.com', od.socrata_key,
               username = od.socrata_username,
               password = od.socrata_pswd)

    return request
    

def readPluto():
    '''read in PLUTO data from all boros into one dataframe'''
    warnings.filterwarnings('ignore')
    path = "/home/deena/Documents/data_munge/pluto/nyc_pluto_17v1/"
    #print 'PLUTO path:',path
    pluto = pd.DataFrame()

    borolist = ['MN','BX','BK','QN','SI']
    for boro in borolist:
        df = pd.read_csv(path+boro+'2017v1.csv')
        pluto = pd.concat([pluto,df],ignore_index=True)
    return pluto

def readMapPluto():
    '''read in MapPLUTO data from all boros into one geodataframe'''
    path = '/home/deena/Documents/data_munge/mappluto16v1/'
    #print 'MapPLUTO path:',path
    mp=gpd.GeoDataFrame()
    
    borolist = ['mn','bx','bk','qn','si']
    for boro in borolist:
        df = gpd.read_file(path+boro+'_mappluto_16v1')
        mp = gpd.GeoDataFrame(pd.concat([mp,df],ignore_index=True))
        mp.crs = df.crs #adding in the projection manually
    return mp

def readMapPlutoCSV():
    '''read in MapPLUTO csv files from all boros into one dataframe
    (used ogr2ogr to convert original shapefiles into csv)'''
    path = '/home/deena/Documents/data_munge/mappluto16v1/'
    #print 'MapPLUTO path:',path
    mp=pd.DataFrame()
    
    borolist = ['mn','bx','bk','qn','si']
    for boro in borolist:
        df = pd.read_csv(path+boro+'_mappluto_16v1.csv')
        mp = pd.concat([mp,df],ignore_index=True)
    mp.BBL = mp.BBL.round()
    return mp

def readPad(billbbl_flag=0):
    '''
    read in DCP's PAD and return two dataframes
    
    combines boro block lot fields into one bbl column
    
    if billbbl_flag is set to 1, then it replaces the bbl field in pad_bbl with
    the billbbl field where it exits.
     '''
    path = "/home/deena/Documents/data_munge/pad/pad19c/"
    #print 'PAD path', path
    warnings.filterwarnings('ignore')
    
    pad_adr = pd.read_csv(path+'bobaadr.txt')
    pad_adr.drop_duplicates(inplace=True)
    pad_adr['bbl'] = combineBBL(pad_adr,boro='boro',block='block',lot='lot')
    
    pad_bbl = pd.read_csv(path+'bobabbl.txt')
    pad_bbl['bbl'] = combineBBL(pad_bbl,boro='boro',block='block',lot='lot')
    pad_bbl['billbbl'] = pad_bbl.billboro + pad_bbl.billblock + pad_bbl.billlot
    pad_bbl['billbbl']= pd.to_numeric(pad_bbl.billbbl,errors='coerse')
    
    if(billbbl_flag == 1):
        pad_bbl.loc[pad_bbl.billbbl>0,'bbl'] = pad_bbl.billbbl
    pad_bbl.drop_duplicates(inplace=True)
    
    return pad_adr, pad_bbl

def combineBBL(df,boro='Borough',block='Block',lot='Lot'):
    '''
    in dataframes df where boro,block, and lot come in three separate columns,
    this returns them as one column with the correct BBL formatting.
    Returns pandas series
    '''
    df['bbl'] = df[boro].astype(int).apply(lambda x: '{:01.0f}'.format(x)).astype(str)\
    + df[block].astype(int).apply(lambda x: '{:05.0f}'.format(x)).astype(str)\
    + df[lot].astype(int).apply(lambda x: '{:04.0f}'.format(x)).astype(str)
    return df['bbl'].astype(int)

def extractAddress(series):
    '''
    in a dataframe series with an address, this function separates the address
    into the number and the street name.
    returns two pandas series
    '''
    number = series.str.extract('(^[0-9|-]*)',expand=False)
    street = series.str.extract('(\s.+$)',expand=False)
    return number,street


def geoclientBatch(df,address='address'):
    '''
    Uses DOITT's GeoClient (the web interface to DCP's GeoSupport)
    https://api.cityofnewyork.us/geoclient/v1/doc
    Single Field Search input type
    Returns the dataframe df with two additional columns: geocodedBBL and geocodedBIN
    '''
    path = 'https://api.cityofnewyork.us/geoclient/v1/search.json?app_id=fb9ad04a&app_key=051f93e4125df4bae4f7c57517e62344&'

    #warnings.filterwarnings('ignore') #do not display warnings
    
    def hitGeoC(df):
        try:
            query = {'input':df[address]}
            response = get(path+urlencode(query))
            results = response.json()['results'][0]['response']            
            BBL = results['bbl']
            BIN = results['buildingIdentificationNumber']
        except:
            e = sys.exc_info()[0]
            BBL = ( "Error: %s" % e )
            BIN = BBL
        return BBL,BIN
    
    df[['geocodedBBL','geocodedBIN']] = df.apply(hitGeoC,axis=1).apply(pd.Series)
    return df


def geoclientBatchCensus(df,address='address'):
    '''
    Uses DOITT's GeoClient (the web interface to DCP's GeoSupport)
    https://api.cityofnewyork.us/geoclient/v1/doc
    Single Field Search input type
    Returns the dataframe df with two additional columns: censustract and nta
    '''
    path = 'https://api.cityofnewyork.us/geoclient/v1/search.json?app_id=fb9ad04a&app_key=051f93e4125df4bae4f7c57517e62344&'

    #warnings.filterwarnings('ignore') #do not display warnings
    
    def hitGeoC(df):
        try:
            query = {'input':df[address]}
            response = get(path+urlencode(query))
            results = response.json()['results'][0]['response']
            m = results['censusTract2010']
            m = re.sub('^\s+','',m)
            censustract = re.sub('\s','0',m)
            nta = results['nta']
        except:
            e = sys.exc_info()[0]
            censustract = ( "Error: %s" % e )
            nta = censustract
        return censustract,nta
    
    df[['censustract','nta']] = df.apply(hitGeoC,axis=1).apply(pd.Series)
    return df

def geosupport(boro, houseNo, street,function = '1A', tpad='n', extend=''):
    '''
    Python wrapper for DCP's GeoSupport Desktop Edition, 
    1. Download the Linux version from DCP's website: 
    http://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page
    2. Set 'path' to point to where you downloaded the geosupport folder to
    
    GeoSupport User Guide: http://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/upg.pdf
    
    Input:  boro is the borough code, integer 1-5
            boro = 1 (Manhattan), 2 (Bronx), 3 (Brooklyn), 4 (Queens), 5 (Staten Island)
            houseNO is the address house number
            street is the address street name
            function = 1A, 1, or 1E (1A is the default)
              [function is the GeoSupport function that takes address or non-addressable
              place name as input. (see GeoSuport User Guide for more info)] 
            tpad: y if you want TPAD data, n otherwise (see GeoSupport User Guide)
            extend: y if you want Extended Work Area (see GeoSupport User Guide)
    
    Returns output from GeoSupport
    '''
    
    # path to geosupport files and executables
    # path = '/home/deena/geosupport/version-20d_20.4/'
    path = '/home/deena/geosupport/version-21b_21.2/'

    # set environment variables
    my_env = os.environ.copy()
    my_env["LD_LIBRARY_PATH"] = path+'lib'
    my_env["GEOFILES"] = path+'fls/'

    # path to geosupport executable
    geosupport = path+'bin/c_client'

    # open subprocess to run geosupport
    p = subprocess.Popen([geosupport],
                         env=my_env, 
                         stdout=subprocess.PIPE,
                         stdin=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    
    exit = 'x'
    
    inputstring = '{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(function,
                                                        boro,
                                                        houseNo,
                                                        street,
                                                        tpad,
                                                        extend,
                                                        exit).encode('utf-8')
    # read in input data
    stdout_data = p.communicate(input=inputstring)

    # output
    return stdout_data[0].splitlines(True)[14:]

def geosupportBatch(df,boro='boro',houseNo='houseNo',street='street'):
    ''' 
    Batch processing using GeoSupport function 1A, and returning BBL and BIN
    
    1. Download the Linux version from DCP's website: 
    http://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page
    2. Set 'path' to point to where you downloaded the geosupport folder to
    
    GeoSupport User Guide: http://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/upg.pdf
    
    input:  dataframe df, with column names containing information on 
            the borough (boro = 1-5), 
            address house number (houseNo),
            and address street name (street).
    
    returned dataframe will have two additional columns: geocodedBBL and geocodedBIN.
    '''
    # path to geosupport files and executables
    # path = '/home/deena/geosupport/version-20d_20.4/'
    path ='/home/deena/geosupport/version-21b_21.2/'


    # set environment variables
    my_env = os.environ.copy()
    my_env["LD_LIBRARY_PATH"] = path+'lib'
    my_env["GEOFILES"] = path+'fls/'

    # path to geosupport executable
    expath = path+'bin/c_client'

    def hitGeoS(df):
        # open subprocess to run geosupport
        p = subprocess.Popen([expath],
                             env=my_env, 
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        
        # this works with function 1A which takes address or non-addressable place name
        # as input and returns property related data including BIN, BBL
        inputstring = '{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format('1A',
                                                            df[boro],
                                                            df[houseNo],
                                                            df[street],
                                                            '','','x').encode('utf-8')
        try:
            # read input data to geosupport
            stdout_data = p.communicate(input=inputstring)
            output = stdout_data[0].decode('utf-8')
            # search for BBL data in output
            m = re.search('\[  6\]: Bbl .+[\n]',output)
            BBL = int(m.group()[-11:-1])
            # search for BIN data in output
            m = re.search('Bin Of Input Address .+[\n]',output)
            BIN = int(m.group()[-8:-1])
        except:
            m = re.search('Error Message .+[\n]',output)
            BBL = m.group()
            BIN = m.group()

        return BBL,BIN
    
    df[['geocodedBBL','geocodedBIN']] = df.apply(hitGeoS,axis=1).apply(pd.Series)
    return df
    
def geosupportBatchCensus(df,boro='boro',houseNo='houseNo',street='street'):
    ''' 
    Batch processing using GeoSupport function 1, 
    returns 2010 census tract and nta
    
    1. Download the Linux version from DCP's website: 
    http://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page
    2. Set 'path' to point to where you downloaded the geosupport folder to
    
    GeoSupport User Guide: http://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/upg.pdf
    
    input:  dataframe df, with column names containing information on 
            the borough (boro = 1-5), 
            address house number (houseNo),
            and address street name (street).
    
    returned dataframe will have two additional columns: censustract and nta.
    '''
    # path to geosupport files and executables
    # path = '/home/deena/geosupport/version-20d_20.4/'
    path ='/home/deena/geosupport/version-21b_21.2/'

    # set environment variables
    my_env = os.environ.copy()
    my_env["LD_LIBRARY_PATH"] = path+'lib'
    my_env["GEOFILES"] = path+'fls/'

    # path to geosupport executable
    expath = path+'bin/c_client'

    def hitGeoS(df):
        # open subprocess to run geosupport
        p = subprocess.Popen([expath],
                             env=my_env, 
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        # this works with function 1A which takes address or non-addressable place name
        # as input and returns property related data including BIN, BBL
        inputstring = '{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format('1',
                                                            df[boro],
                                                            df[houseNo],
                                                            df[street],
                                                            '','','x').encode('utf-8')
        try:
            # read input data to geosupport
            stdout_data = p.communicate(input=inputstring)
            output = stdout_data[0].decode('utf-8')
           
            # extract census tract from output
            m = re.search('\[ 66\]: 2010 Census Tract .*[\n]',output)
            m = m.group()[-8:-1]
            m = re.sub('^\s+','',m)
            censustract = re.sub('\s','0',m)
           
            # extract nta from output
            m = re.search('\[ 72\]: Nta .*[\n]',output)
            nta = m.group()[-5:-1]
            
        except:
            m = re.search('Error Message .*[\n]',output)
            censustract = m.group()
            nta = m.group()

        return censustract,nta
    
    df[['censustract','nta']] = df.apply(hitGeoS,axis=1).apply(pd.Series)
    return df



def heatmap(df):
    '''heat map representation of a dataframe (all values should be numerical)
    Returns a heatmap of a dataframe 
    '''
    dfn = df.astype(float)
    m, n = dfn.shape
    ax = plt.imshow(dfn, interpolation='nearest', cmap='Oranges').axes

    _ = ax.set_xticks(np.linspace(0, n-1, n))
    _ = ax.set_xticklabels(dfn.columns)
    _ = ax.set_yticks(np.linspace(0, m-1, m))
    _ = ax.set_yticklabels(dfn.index)

    ax.grid('off')
    ax.xaxis.tick_top()

    for i, j in product(range(m), range(n)):
        _ = ax.text(j, i, '{0:,.0f}'.format(dfn.iloc[i, j]),
                    fontsize=18, ha='center', va='center')
        
def attributes(df):
    '''returns a dataframe containing the counts and number of unique values
    for all the attributes in a dataframe df
    this is similar to what is returned in df.describe(), but works for numerical
    data as well'''
    return pd.DataFrame({'unique values':df.apply(lambda x: x.nunique()),
                         'count':df.apply(lambda x: x.count())})


def plotAllVars(df,uniqueID):
    ''' Plot frequencies of all columns in dataframe df
        The uniqueID is any column which is never null.
        Can generally use the primary key'''
    for i in df.drop(uniqueID,axis=1).columns:
        df.groupby(i).count()[[uniqueID]].plot()



