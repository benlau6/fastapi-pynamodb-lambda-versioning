import numpy as np
import pandas as pd
import os
import datetime as dt

def str_to_bool(value):
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'{value} is not a valid boolean value')

def filter_function(df):
    #hard code any filter rules
    df_filter = df.copy()
    #1. travel time less than 30 mins
    df_filter = df_filter[df_filter['travel_time']<3000]
    return df_filter

def segment_to_meters(row,df_chainage,short_form=False):
    x = row['segment']
    y = row['Code']
    l = row['line_id']
    y1 = y.split('-')[0]
    y2 = y.split('-')[1]
    _direction = row['direction']
    _start,_end = df_chainage[(df_chainage['From']==y1)&
                              (df_chainage['Line']==l)&
                              (df_chainage['To']==y2)][['Start','End']].values[0]

    if short_form==True:
        if _direction.startswith('Up'):
            _direction = 'UT'
        else:
            _direction = 'DT'
    _x = (int(x.split('_')[1])-1)*50
    if _end>=_start:
        return _direction + ' ' + str(np.round(_start + _x/1000,3))+" - "+str(min(np.round(_start + (_x+100)/1000,3),np.round(_end,3)))
    else:
        return _direction + ' '+ str(max(np.round(_start - (_x+100)/1000,3),np.round(_end,3)))+" - "+str(np.round(_start - (_x)/1000,3))

def rotate_fun(df,index_col,value_name):
    df1 = df.copy()
    s = df1.set_index(index_col)
    try:
        s.columns.name = 'segment'
        s = s.stack()
        s.name = value_name
    except:
        pass
    s = s.reset_index()
    return s

def concat_files(_list_of_files):
    list_of_df = []
    for idx,item in _list_of_files.items():
        #print(idx)
        #print(item)
        df_temp = pd.read_csv(item)
        df_temp['version'] = idx
        df_temp = filter_function(df_temp)

        df_x_column = [i for i in df_temp.columns if i.startswith('z_segment')==False]
        df_z_column = [i for i in df_temp.columns if i.startswith('x_segment')==False]
        df_x = df_temp[df_x_column] # with only x information, no z
        df_z = df_temp[df_z_column] # with only z information, no x

        x_1 = [i for i in df_x_column if i.startswith('x_segment')==False]
        x_2 = [i for i in df_x_column if i.startswith('x_segment')==True]
        z_1 = [i for i in df_z_column if i.startswith('z_segment')==False]
        z_2 = [i for i in df_z_column if i.startswith('z_segment')==True]

        dfx = rotate_fun(df_x,x_1,'segment_x')
        dfz = rotate_fun(df_z,z_1,'segment_z')

        dfx['segment'] = dfx['segment'].str[2:]
        dfz['segment'] = dfz['segment'].str[2:]

        df_merge = pd.merge(dfx,dfz,right_on=z_1+['segment'],left_on=x_1+['segment'],how='inner')

        assert df_merge.shape[0]==dfx.shape[0], print('wrong dimension in x')
        assert df_merge.shape[0]==dfz.shape[0], print('wrong dimension in z')

        list_of_df.append(df_merge)

    df = pd.concat(list_of_df,sort=True)
    return df

def get_startend_date(x = dt.datetime.now()):
    end = (x -dt.timedelta(days=x.weekday())).strftime('%Y-%m-%d')
    start = (x -dt.timedelta(days=x.weekday()+7)).strftime('%Y-%m-%d')
    start1 = (x -dt.timedelta(days=x.weekday()+28)).strftime('%Y-%m-%d')
    start2 = (x -dt.timedelta(days=x.weekday()+56)).strftime('%Y-%m-%d')
    return start2,start1,start,end

def assign_show_in_selection(x,s_week,s_month,s_2month,end):
    if (x[:10]>= s_week) & (x[:10]< end):
        return 1
    elif(x[:10]>= s_month) & (x[:10]< s_week):
        return 4
    elif(x[:10]>= s_2month) & (x[:10]< s_month):
        return 8
    else:
        return 10

def calculate_slope(df):
    _inDays=(pd.to_datetime(df['time_stamp'],format='%Y-%m-%d %H:%M:%S')-dt.datetime(year=1900,month=1,day=1)).dt.total_seconds()/(24*3600)
    x = np.cov(_inDays,df['segment_x'])
    z = np.cov(_inDays,df['segment_z'])
    return pd.DataFrame([[x[0][1]/x[0][0],z[0][1]/z[0][0]]],columns= ['slope of lateral vibration','slope of vertical vibration'])

def generate_report(df,df_chainage,df_slope):
    '''
    4 reports
    1 - lateral by magnitude (median)
    2 - vertical by magnitude (median)
    3 - lateral by % exceed with delta mean
    4 - vertical by % exceed with delta mean
    '''
    def fun_3and4(df):
        _x_exceed = len(df[df['segment_x']> df['_top_segment_x']])
        _z_exceed = len(df[df['segment_z']> df['_top_segment_z']])
        _x_count = len(df)
        _z_count = len(df)
        _x_percent = np.round((_x_exceed)/(_x_count+0.0001)*100,1)
        _z_percent = np.round((_z_exceed)/(_z_count+0.0001)*100,1)
        _x_hist_median = df['segment_x_median'].median()
        _z_hist_median = df['segment_z_median'].median()
        _x_cur_median = df['segment_x'].median()
        _z_cur_median = df['segment_z'].median()
        _x_delta_median = _x_cur_median -_x_hist_median
        _z_delta_median = _z_cur_median -_z_hist_median



        return pd.DataFrame([[_x_exceed,_x_count,_x_percent,_x_hist_median,_x_cur_median,_x_delta_median,
                             _z_exceed,_z_count,_z_percent,_z_hist_median,_z_cur_median,_z_delta_median]],
                            columns = ['x exceed','x count','x percent','x hist median','x current median','x delta median',
                                       'z exceed','z count','z percent','z hist median','z current median','z delta median'])


    df1 = df[(df['_show_in_selection']==1)].groupby(['Code','segment','direction','line_id'])['segment_x'].median().reset_index().sort_values('segment_x',ascending=False)
    df2 = df[(df['_show_in_selection']==1)].groupby(['Code','segment','direction','line_id'])['segment_z'].median().reset_index().sort_values('segment_z',ascending=False)
    df34 = df[(df['_show_in_selection']==1)].groupby(['Code','segment','direction','line_id']).apply(fun_3and4)
    df3 = df34[[i for i in df34.columns if i.startswith('x')]].reset_index().sort_values('x percent',ascending=False)
    df4 = df34[[i for i in df34.columns if i.startswith('z')]].reset_index().sort_values('z percent',ascending=False)

    df1['approx chainage'] = df1.apply(segment_to_meters,axis=1,df_chainage=df_chainage)
    df2['approx chainage'] = df2.apply(segment_to_meters,axis=1,df_chainage=df_chainage)
    df3['approx chainage'] = df3.apply(segment_to_meters,axis=1,df_chainage=df_chainage)
    df4['approx chainage'] = df4.apply(segment_to_meters,axis=1,df_chainage=df_chainage)
    df3['_segment_name'] = df3.apply(segment_to_meters,axis=1,df_chainage=df_chainage,short_form=True)
    df4['_segment_name'] = df4.apply(segment_to_meters,axis=1,df_chainage=df_chainage,short_form=True)

    df3 = pd.merge(df3,df_slope[['_segment_name','slope of lateral vibration']],left_on=['_segment_name'],right_on=['_segment_name'],how='left')
    df4 = pd.merge(df4,df_slope[['_segment_name','slope of vertical vibration']],left_on=['_segment_name'],right_on=['_segment_name'],how='left')

    df1 = df1[['Code','approx chainage','segment_x']]
    df1['segment_x'] = (df1['segment_x']).astype(int)
    df1.columns = ['inter-station','approx. chainage','lateral vibration (median)']

    df2 = df2[['Code','approx chainage','segment_z']]
    df2['segment_z'] = (df2['segment_z']).astype(int)
    df2.columns = ['inter-station','approx. chainage','vertical vibration (median)']

    df3 = df3[['Code','approx chainage','x percent','x hist median','x current median','slope of lateral vibration']]
    df3['x hist median'] = df3['x hist median'].astype(int)
    df3['x current median'] = df3['x current median'].astype(int)
    df3.columns = ['inter-station','approx. chainage','% train trip exceed usual range',
                   'Historical vibration (median)','Recent vibration (median)','slope of vibration']
    df3['Vibration Direction'] = 'Lateral'

    df4 = df4[['Code','approx chainage','z percent','z hist median','z current median','slope of vertical vibration']]
    df4['z hist median'] = df4['z hist median'].astype(int)
    df4['z current median'] = df4['z current median'].astype(int)
    df4.columns = ['inter-station','approx. chainage','% train trip exceed usual range',
                   'Historical vibration (median)','Recent vibration (median)','slope of vibration']
    df4['Vibration Direction'] = 'Vertical'


    df5 = pd.concat([df3,df4],sort=False)

    df5['% Vibration Increased'] = np.round((df5['Recent vibration (median)'] - df5['Historical vibration (median)'])/
                                            (df5['Historical vibration (median)']+0.0001)*100,1)
    df5 = df5.sort_values(['% train trip exceed usual range','% Vibration Increased'],ascending=False)
    #df5 = df5[df5['% Vibration Increased']>0]
    df5 = df5[['inter-station','approx. chainage','Vibration Direction','% train trip exceed usual range',
                   'Historical vibration (median)','Recent vibration (median)','% Vibration Increased','slope of vibration']]
    return df1,df2,df3,df4,df5
