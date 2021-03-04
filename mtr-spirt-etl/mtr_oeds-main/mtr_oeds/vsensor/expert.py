# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 14:01:16 2020

@author: oeds-rsmd
"""

import pandas as pd
import numpy as np
import datetime as dt

class Findings():
    def __init__(self,line,station,chainage):
        self.line = line
        self.station = station
        self.chainage = chainage
        self.explanation = []

def slope_expert(df):
    #todo: auto detect location that with abnormal slope

    _hardcode_location = [('TWL','ADM-TST','UT 1.386 - 1.486'),
                          ('TWL','KWH-TWH','UT 14.346 - 14.446'),
                          ('TWL','KWH-TWH','UT 14.746 - 14.846'),
                          ('TWL','MOK-YMT','DT 5.447 - 5.547'),
                          ('TWL','TST-ADM','DT 1.377 - 1.477'),
                          ('TWL','TST-ADM','DT 1.427 - 1.527'),
                          ('TWL','TWH-KWH','DT 14.119 - 14.219'),
                          ('TWL','TWH-KWH','DT 14.719 - 14.819')]


    df_copy = df.copy()
    df_copy['dummy'] = (pd.to_datetime(df_copy['time_stamp'],format='%Y-%m-%d %H:%M:%S')-dt.datetime(year=1900,month=1,day=1)).dt.total_seconds()/(24*3600)
    df_copy.sort_values('dummy',ascending=True,inplace=True)

    #create 2 new columns: time & slope
    df_temp = df_copy.groupby(['Line','Code','_segment_name']).apply(rolling_linearRegression,YaxisName='dummy',XaxisName='segment_x')
    df_temp.reset_index(inplace=True,drop=False)

    #create a new column: "Reason"
    df_temp['Reason']= None

    #Reason1: Slope exceed 20 or less than -20
    df_temp["Reason"] = df_temp.apply(lambda x: 'slope exceed 20 at {}'.format(x['time']) if (x['slope']>20) else x['Reason'] ,axis=1)
    df_temp["Reason"] = df_temp.apply(lambda x: 'slope less than -20 at {}'.format(x['time']) if (x['slope']<-20) else x['Reason'] ,axis=1)

    #Reason2: Hardcode Location
    df_hardcodeLocation = pd.DataFrame(_hardcode_location,columns = ['Line','Code','_segment_name'])
    df_hardcodeLocation['hardCode'] = "Highlight Location by IMD"
    df_temp = pd.merge(df_temp,df_hardcodeLocation,left_on=['Line','Code','_segment_name'],right_on=['Line','Code','_segment_name'],how='left')
    df_temp['Reason'] = np.where(pd.isnull(df_temp['hardCode']),df_temp['Reason'],df_temp['hardCode'])
    df_temp.drop('hardCode',inplace=True,axis=1)

    #if multiplel reasons occur in single location, keep last (except hardcode location)
    df_temp.sort_values(['Reason','time'],ascending=False,inplace=True)
    df_output = pd.concat([df_temp[df_temp['Reason']=='Highlight Location by IMD'].drop_duplicates(['Line','Code','_segment_name'],keep='first'),
                           df_temp[df_temp['Reason']!='Highlight Location by IMD'].dropna(subset=['Reason']).drop_duplicates(['Line','Code','_segment_name'],keep='first')])

    df_output = pd.merge(df,df_output[['Line','Code','_segment_name','Reason']],how='left',left_on=['Line','Code','_segment_name'],right_on=['Line','Code','_segment_name'])

    #df_temp.to_csv('df_slope.csv',index=False)
    #df['slope_expert_explanation'] = 'Normal'
    return df_output  #return Location, Reasons

def rolling_linearRegression(df_,YaxisName,XaxisName,segment_length=200,segment_step=50):
    '''return the timeRange and the slope with different rolling period'''
    if len(df_)<segment_length:
        # return only perform 1 linearRegression
        _timeRange=["{} to {}".format(df_['time_stamp'].values[0][:10],df_['time_stamp'].values[-1][:10])]
        _timeRangeSlope = np.array([_slope(df_[YaxisName].values,df_[XaxisName].values)])
        return pd.DataFrame(list(zip(_timeRange,_timeRangeSlope)),columns= ['time','slope'])
    else:
        # loop and calculate sectional linearRegression
        _ = list(zip(list(range(0,len(df_)-segment_length,segment_step)),[x+segment_length for x in list(range(0,len(df_)-segment_length,segment_step))[1:]+[len(df_)-(segment_length+1)]]))
        _timeRange=["{} to {}".format(df_['time_stamp'].values[i[0]][:10],df_['time_stamp'].values[i[1]][:10]) for i in _]
        _timeRangeSlope = np.array([_slope(df_[YaxisName].values[start:end],df_[XaxisName].values[start:end]) for start,end in _])
        return pd.DataFrame(list(zip(_timeRange,_timeRangeSlope)),columns= ['time','slope'])


def _slope(x,y,exclude_outlier = False):
    #x is the date
    #y is the value
    if exclude_outlier:
         #drop the extreme value
         _max = np.percentile(y,90)
         _min = np.percentie(y,10)
         _temp = [(i[0],i[1]) for i in list(zip(x,y)) if (i[1]<_max)&(i[1]>_min)]
         x,y =zip(*_temp)
    _ = np.cov(x,y)
    return _[0][1]/_[0][0] #slope


#df = pd.read_csv('./TWL_summary.csv')

#df_1 = slope_expert(df)

#df_1.to_csv('df_slope.csv',index=False)
