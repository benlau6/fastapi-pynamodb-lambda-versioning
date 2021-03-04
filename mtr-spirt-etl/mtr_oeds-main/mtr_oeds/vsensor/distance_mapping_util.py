import os
import re
import sys
import time
import random
import numpy as np
import pandas as pd
import configparser
from collections import Counter
import matplotlib
import matplotlib.pyplot as plt
import datetime as dt
from scipy.integrate import cumtrapz,trapz
import multiprocessing
import ftplib
from ..credential import _pw_sftp
from mtr_oeds.common import df_vsensor_1
verbose=True

def rename_column_axis(df,config):
    forward=config['current'].get('forward_axis','x')
    lateral=config['current'].get('lateral_axis','y')
    vertical=config['current'].get('vertical_axis','z')
    log_and_print('column name {} is used as forward axis and is renamed as y'.format(forward),config['current']['path_to_save'])
    log_and_print('column name {} is used as lateral axis and is renamed as x'.format(lateral),config['current']['path_to_save'])
    log_and_print('column name {} is used as vertical axis and is renamed as z'.format(vertical),config['current']['path_to_save'])
    df.rename(columns={forward:'forward',lateral:'lateral',vertical:'vertical'},inplace=True)
    df.rename(columns={'forward':'y','lateral':'x','vertical':'z'},inplace=True)
    return df

# simple logging function
def log_and_print(message,path=None):
    if verbose:
        print(message)
        if path!=None:
            with open(os.path.join(path,'log.txt'),'a+') as f:
                f.write(message+'\n')
    return

def read_station_distance(excel_path,linename):
    sheetname_list = ['KTL UT','KTL DT','TWL UT','TWL DT','ISL UT','ISL DT']
    _list = []
    for eachsheet in [i for i in sheetname_list if i.startswith(linename)]:
        _list.append(pd.read_excel(excel_path,sheet_name=eachsheet))
    df = pd.concat(_list).reset_index(drop=True)

    return df

def find_and_merge(file_path,auto_z_offset,z_offset_value,config,_min_threshold=500,_manual_shift = 0):
    try:
        _counter = Counter()
        for each in [ i.split('.')[0].split('_')[0] for i in os.listdir(file_path) if i.startswith('V')&i.endswith('XLS')]:
            _counter[each]+=1

        #check if empty counter
        if len(_counter)==0:
            raise Exception ('empty counter')
            exit()
        elif len([i for i in _counter.most_common() if i[1]>=_min_threshold])==0:
            raise Exception ('No file prefix identified')
            exit()

        file_prefix = sorted([i for i in _counter.most_common() if i[1]>=_min_threshold],key = lambda x: int(x[0][1:]),reverse=True)[0][0]
        file_prefix = file_prefix+'_.*.XLS'
        log_and_print('{} is used as file prefix'.format(file_prefix),config['current']['path_to_save'])

        file_list = [i for i in os.listdir(file_path) if re.match(file_prefix,i)]
        file_list= sorted(file_list,key=lambda x:x[3:].zfill(8))
        #file_list = file_list[300:900]
        log_and_print('there are total {} files to be merge'.format(len(file_list)),config['current']['path_to_save'])

    except Exception as e:
        file_list= []
        log_and_print(e,config['current']['path_to_save'])
    df = None
    df_list = []
    for eachFile in file_list:
        try:
            df_temp = pd.read_table(os.path.join(file_path,eachFile),sep='\t', header=None,skiprows=1)
            df_temp.columns = ['datetime','x','y','z']
            df_list.append(df_temp)
            if verbose: print('\r{}'.format(eachFile),end='')
        except:
            log_and_print('cannot read {}'.format(eachFile),config['current']['path_to_save'])
    print('')
    df = pd.concat(df_list)
    df['datetime_convert'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df['datetime'], 'D')
    df.drop('datetime', axis=1, inplace=True)
    df.sort_values('datetime_convert',inplace=True)
    df.reset_index(drop=True,inplace=True)

    #manual shift the datetime_convert
    df['datetime_convert'] = df['datetime_convert'] +dt.timedelta(seconds = _manual_shift)

    # rename the axis such that after renaming, 'y' is use as forward, x is use as lateral
    df = rename_column_axis(df,config)

    if auto_z_offset==True:
        z_mean = df.iloc[90000:540000]['z'].mean()
        y_mean = df.iloc[90000:540000]['y'].mean()
        x_mean = df.iloc[90000:540000]['x'].mean()
        df['z'] = df['z'] - z_mean
        df['y'] = df['y'] - y_mean
        df['x'] = df['x'] - x_mean
        log_and_print('z offset applied: {}'.format(z_mean),config['current']['path_to_save'])
        log_and_print('y offset applied: {}'.format(y_mean),config['current']['path_to_save'])
        log_and_print('x offset applied: {}'.format(x_mean),config['current']['path_to_save'])
    else:
        df['z'] = df['z'] - z_offset_value
        log_and_print('z offset applied: {}'.format(z_offset_value),config['current']['path_to_save'])
    df['absolute z'] = np.absolute(df['z'])
    return df[20:].reset_index(drop=True)

def read_TSSW(config,start_date,end_date):
    # when version1, TSSW is pre-processeed with 7 days
    if 'path_TSSW' in config['current']:
        df = pd.read_excel(config['current']['path_TSSW'])
        df['time_stamp'] = pd.to_datetime(df['time_stamp'],format='%d/%m/%Y %I:%M:%S %p')
        df = df[(df['time_stamp']>=start_date)&(df['time_stamp']<=end_date) ].copy()
        df.reset_index(drop=True,inplace=True)
        return df
    else:
        _temp_df_list = []
        for eachFile in [i for i in os.listdir(config['current']['TSSW_folder']) if (i[:8]>=config['current']['TSSW_start']) & (i[:8]<=config['current']['TSSW_end']) & ('Full' in i)]:
            _temp_df = pd.read_excel(os.path.join(config['current']['TSSW_folder'],eachFile),dtype=str)
            _temp_df['time_stamp'] = pd.to_datetime(_temp_df['time_stamp'],format='%Y-%m-%d %H:%M:%S')
            _temp_df_list.append(_temp_df)
        df = pd.concat(_temp_df_list)
        df = df[df['line_id']==config['current']['linename']]
        df = df[(df['lead_cab']==config['current']['car_number']) | (df['trail_cab']==config['current']['car_number'])]
        df = df[(df['time_stamp']>=start_date)&(df['time_stamp']<=end_date) ].copy()
        df.sort_values('time_stamp',inplace=True)
        df.dropna(subset=['station_name'],inplace=True)
        df.reset_index(drop=True,inplace=True)
        return df

def find_time_shift(df1,df2,auto_time_shift,time_shift_value,config):
    if auto_time_shift==True:
        _number_of_station_to_check = 20
        _maximun_second_to_shift = 300
        _second_range = sorted(np.arange(-1*_maximun_second_to_shift,_maximun_second_to_shift),key = lambda x: abs(x))
        df_source = df2.head(_number_of_station_to_check).append(df2.tail(_number_of_station_to_check)).copy()#df2.loc[:_number_of_station_to_check,:].copy()
        _shift_counter = Counter()

        for idx,row in df_source.iterrows():
            df_temp = df1[(df1['datetime_convert']>=row['time_stamp']-dt.timedelta(seconds=_maximun_second_to_shift*2))&
                          (df1['datetime_convert']<=row['time_stamp']+dt.timedelta(seconds=_maximun_second_to_shift*2))].copy()
            df_temp['datetime_convert second'] = df_temp['datetime_convert'].dt.round('s')
            df_temp = df_temp.groupby('datetime_convert second')['absolute z'].apply(lambda x: np.percentile(abs(x),99)).reset_index()

            for eachSecond in _second_range:
                #apply shift
                #get previous 5 sec and next 1 sec max
                #under certain version, counter add 1
                _previousFiveSec = df_temp[(df_temp['datetime_convert second']>= row['time_stamp']+dt.timedelta(seconds=int(eachSecond)-10))&
                                           (df_temp['datetime_convert second']< row['time_stamp']+dt.timedelta(seconds=int(eachSecond)))]['absolute z'].max()

                _nextTwoSec = df_temp[(df_temp['datetime_convert second']>= row['time_stamp']+dt.timedelta(seconds=int(eachSecond)))&
                                           (df_temp['datetime_convert second']< row['time_stamp']+dt.timedelta(seconds=int(eachSecond)+10))]['absolute z'].max()

                if (_previousFiveSec<200) & (_nextTwoSec>1000):
                    _shift_counter[eachSecond]+=1

        print(_shift_counter.most_common(5))
        top_five = _shift_counter.most_common(5)
        top_five.sort(key=lambda k: (-k[1], abs(k[0])), reverse=False)
        log_and_print('timeshift auto {}'.format(str(top_five)),config['current']['path_to_save'])
        return top_five[0][0]
    else:
        log_and_print('timeshift manually {}'.format(time_shift_value),config['current']['path_to_save'])
        return time_shift_value

def visualize_shifting_result(df_vsensor,df_result,start_time,end_time,filename='dummy.png',figsize=[12,8]):
    #plot a figure to see the shifting works
    fig,ax = plt.subplots(3,figsize=figsize,sharex=True)

    ax[0].set_xlim([start_time, end_time])

    df_temp = df_vsensor[(df_vsensor['datetime_convert']>=start_time)&(df_vsensor['datetime_convert']<=end_time)].copy()

    #plot the vensor
    ax[0].scatter(df_temp['datetime_convert'],df_temp['x'])
    ax[1].scatter(df_temp['datetime_convert'],df_temp['y'])
    ax[2].scatter(df_temp['datetime_convert'],df_temp['z'])
    #plt.scatter(df_temp['datetime_convert'],df_temp['y'])

    if 'vec_x' in df_temp.columns:
        secax0 = ax[0].twinx()
        secax0.plot(df_temp['datetime_convert'],df_temp['vec_x'],color='red',ls='-')
        secax0.spines['right'].set_color('red')
        if 'dist_x' in df_temp.columns:
            secax00 = ax[0].twinx()
            secax00.plot(df_temp['datetime_convert'],df_temp['dist_x'],color='green',ls='-')
            secax00.spines["right"].set_position(("axes", 1.1))
            secax00.spines['right'].set_color('green')

    if 'vec_y' in df_temp.columns:
        secax1 = ax[1].twinx()
        secax1.plot(df_temp['datetime_convert'],df_temp['vec_y'],color='red',ls='-',label='velocity')
        secax1.spines['right'].set_color('red')
        if 'dist_y' in df_temp.columns:
            secax11 = ax[1].twinx()
            secax11.plot(df_temp['datetime_convert'],df_temp['dist_y'],color='green',ls='-',label='displacement')
            secax11.spines["right"].set_position(("axes", 1.1))
            secax11.spines['right'].set_color('green')
        if 'dist_y_norm' in df_temp.columns:
            secax11.plot(df_temp['datetime_convert'],df_temp['dist_y_norm'],color='orange',ls='-',label='displacement_norm')
        if 'vec_y_calibrate' in df_temp.columns:
            secax1.plot(df_temp['datetime_convert'],df_temp['vec_y_calibrate'],color='black',ls='-',label='calibrated velocity')

    if 'dist_y_norm' in df_temp.columns:
        secax2 = ax[2].twinx()
        secax2.plot(df_temp['datetime_convert'],df_temp['dist_y_norm'],color='orange',ls='-')

    if 'time_stamp_shift' in df_result.columns:
        for idx,item in df_result.iterrows():
            if (item['time_stamp_shift']< end_time) & (item['time_stamp_shift']> start_time):
                ax[2].text(item['time_stamp_shift'],2500,item['station_name'],c='red')
                ax[2].scatter(item['time_stamp_shift'],0,color='red')
    elif 'time_stamp' in df_result.columns:
        for idx,item in df_result.iterrows():
            if (item['time_stamp']< end_time) & (item['time_stamp']> start_time):
                ax[2].text(item['time_stamp'],2500,item['station_name'],c='blue')
                ax[2].scatter(item['time_stamp'],0,color='blue')

    if 'next_station_idle time_stamp_shift' in df_result.columns:
        for idx,item in df_result.iterrows():
            if (item['next_station_idle time_stamp_shift']< end_time) & (item['next_station_idle time_stamp_shift']>start_time):
                ax[2].text(item['next_station_idle time_stamp_shift'],2500,item['next_station_name'],c='blue')
                ax[2].scatter(item['next_station_idle time_stamp_shift'],0,color='blue')
    fig.legend()
    fig.savefig(filename)
    return fig,ax

def find_idle_timestamp(df_vsensor,df_tssw,standard_stopping_time=10,_idle_threshold=100, _running_threshold=250):

    df_tssw['next_station time_stamp_shift'] = df_tssw['time_stamp_shift'].shift(-1)
    df_tssw['next_station_idle time_stamp_shift']=df_tssw['next_station time_stamp_shift']
    df_tssw['next_station_name'] = df_tssw['station_name'].shift(-1)
    df_tssw = df_tssw.iloc[:-1,:].copy()
    df_tssw.reset_index(drop=True,inplace=True)

    df_vsensor['datetime_convert second'] = df_vsensor['datetime_convert'].dt.round('s')
    df_vsensor_temp = df_vsensor.groupby('datetime_convert second')['absolute z'].apply(lambda x: np.percentile(abs(x),95)).reset_index()



    for i,j in df_tssw.iterrows():
        if verbose: print('\rTSSW route record : {}'.format(i),end='')
        df_temp = df_vsensor_temp[(df_vsensor_temp['datetime_convert second']>=j['time_stamp_shift'])&
                                  (df_vsensor_temp['datetime_convert second']<=j['next_station time_stamp_shift']-dt.timedelta(seconds=standard_stopping_time))].copy()

        df_temp = df_temp.sort_values('datetime_convert second',ascending=True).reset_index(drop=True)
        df_temp['abs_z_rolling_before'] = df_temp['absolute z'].rolling(10).max()#quantile(1.0,interpolation='midpoint')
        df_temp['abs_z_rolling_after'] =df_temp['abs_z_rolling_before'].shift(-10)
        df_temp['abs_z_rolling_before'].fillna(0,inplace=True)
        df_temp['abs_z_rolling_after'].fillna(0,inplace=True)
        if len(df_temp[(df_temp['abs_z_rolling_after']<=_idle_threshold)&(df_temp['abs_z_rolling_before']>=_running_threshold)])!=0:
            df_tssw.loc[i,'next_station_idle time_stamp_shift'] = df_temp[(df_temp['abs_z_rolling_after']<=standard_stopping_time)&
                                                                      (df_temp['abs_z_rolling_before']>=_running_threshold)]['datetime_convert second'].max()
    df_tssw['travel_time'] = (df_tssw['next_station_idle time_stamp_shift'] -df_tssw['time_stamp_shift']).dt.total_seconds()
    print('')
    return df_tssw

def generate_report(df_res,df,gen_detail=True):
    '''caclulate the rms and 100 m rms'''

    df_result = df_res.copy()
    df_result.reset_index(inplace=True,drop=True)

    df_result['z rms'] = np.nan
    df_result['y rms'] = np.nan
    df_result['x rms'] = np.nan

    for idx,row in df_result.iterrows():
        print('\rworking on trip {}'.format(idx),end='')
        df_temp = df[(df['datetime_convert']>=(row['time_stamp_shift']-dt.timedelta(seconds=0)))&
                     (df['datetime_convert']<=(row['next_station_idle time_stamp_shift']+dt.timedelta(seconds=0)))].copy()

        # whole trip rms in 3 direction
        df_result.loc[idx,'z rms'] = np.sqrt((df_temp['z']**2).mean())
        df_result.loc[idx,'y rms'] = np.sqrt((df_temp['y']**2).mean())
        df_result.loc[idx,'x rms'] = np.sqrt((df_temp['x']**2).mean())

        try:
            # 100m overlapping rms
            df_temp['round_dist'] = df_temp['dist_y_norm'].fillna(np.nan).abs().round(0)
            _start = 0
            _end = np.ceil(df_temp['round_dist'].max()/100)*100
            df_result.loc[idx,'dist_scaled'] = df_temp['round_dist'].max()
            df_result.loc[idx,'dist_unscaled'] = df_temp['dist_y'].fillna(np.nan).abs().max()

        # log the actual displacement for debug purpose
            if np.abs(df_temp['dist_y'].max())> np.abs(df_temp['dist_y'].min()):
                df_result.loc[idx,'debug_param1'] = df_temp['dist_y'].max()
            else:
                df_result.loc[idx,'debug_param1'] = df_temp['dist_y'].min()
        except:
            pass
        else:
            if gen_detail==True:
                while _start < _end:
                    _col_name = 'segment_{}'.format(str(int(_start/50)+1).zfill(4))
                    #df_result.loc[idx,_col_name]=-1
                    #try:
                    df_result.loc[idx,"z_"+_col_name]= np.sqrt((df_temp[(df_temp['round_dist']>=_start)&(df_temp['round_dist']<=_start+100)]['z']**2).mean())
                    df_result.loc[idx,"x_"+_col_name]= np.sqrt((df_temp[(df_temp['round_dist']>=_start)&(df_temp['round_dist']<=_start+100)]['x']**2).mean())
                    #except:
                    #    pass
                    _start+=50

    print('')
    return df_result

def save_report(df,path):
    df['Code'] = df['station_name']+'-' + df['next_station_name']
    _column = ['station_name','next_station_name','time_stamp','time_stamp_shift','next_station_idle time_stamp_shift',
              'next_station time_stamp_shift','travel_time','z rms','y rms','x rms']
    with pd.ExcelWriter(os.path.join(path,'dummy_report.xlsx'),engine='xlsxwriter') as writer:
        workbook = writer.book
        for eachCode in df['Code'].unique():
            df_temp=df[df['Code']==eachCode]#[_column]
            #df_temp =df_temp.rename(columns={"station_name": "From", "next_station_name": "To"})
            df_temp.to_excel(writer,sheet_name=eachCode,index=False)
            writer.sheets[eachCode].set_column('A:B',8)
            writer.sheets[eachCode].set_column('C:F',20)
            writer.sheets[eachCode].set_column('G:G',12)
            writer.sheets[eachCode].set_column('H:J',8)

        writer.save()
    log_and_print('file saved at {}'.format(path),path)
    return

def save_checkpoint(df1,df2,df3,path_to_save,save_df=False):

    df2.to_csv(os.path.join(path_to_save,'df_TSSW.csv'),index=False,float_format='%g')
    df3.to_csv(os.path.join(path_to_save,'df_result.csv'),index=False,float_format='%g')
    if save_df==True:
        #df1.to_csv(os.path.join(path_to_save,'df.csv.gzip'),index=False,compression='gzip')
        df1.to_parquet(os.path.join(path_to_save,'df.parquet.gzip'),index=False,compression='gzip',allow_truncated_timestamps=True)

    return

def load_checkpoint(folder,nrow=None):
    path = os.path.join('./checkpoint',folder)

    parse_dates = ['datetime_convert','datetime_convert second']

    if nrow:
        df1 = pd.read_csv(os.path.join(path,'df.csv.gzip'),compression='gzip',
                          parse_dates = parse_dates,nrows=nrow)
    else:
        df1 = pd.read_csv(os.path.join(path,'df.csv.gzip'),compression='gzip',
                          parse_dates = parse_dates)

    df2 = pd.read_csv(os.path.join(path,'df_TSSW.csv'),parse_dates = ['time_stamp','time_stamp_shift','next_station time_stamp_shift','next_station_idle time_stamp_shift'])
    df3 = pd.read_csv(os.path.join(path,'df_result.csv'),parse_dates = ['time_stamp','time_stamp_shift','next_station time_stamp_shift','next_station_idle time_stamp_shift'])
    return df1,df2,df3

#save_checkpoint(df,df_TSSW,df_result,foldername='version1')

def estimate_displacement(df,df_result,linename,return_dict=None):
    df_ = df.copy()
    df_['vec_y'] = np.nan
    df_['vec_y_calibrate'] = np.nan
    df_['dist_y'] = np.nan
    df_['dist_y_norm'] = np.nan


    #df_station_to_station_distance = read_station_distance(path,linename)
    df_station_to_station_distance = df_vsensor_1

    for eachidx, eachrow in df_result.iterrows():
        print('\restimating trip {} from {} to {}'.format(eachidx,eachrow['time_stamp_shift'],eachrow['next_station_idle time_stamp_shift']),end='')
        if (eachrow['station_name']!=eachrow['next_station_name'])&(pd.isnull(eachrow['next_station_idle time_stamp_shift'])==False)&(eachrow['travel_time']<=3600):
            df_temp = df_[(df_['datetime_convert']>=eachrow['time_stamp_shift']-dt.timedelta(seconds=0))&
                         (df_['datetime_convert']<=eachrow['next_station_idle time_stamp_shift']+dt.timedelta(seconds=0))].copy()

            df_temp['vec_y'] = np.concatenate((np.array([0]),cumtrapz(df_temp['y']*9.80665/1000,dx = 3/1000)))
            df_temp['vec_y_calibrate'] = df_temp['vec_y']-np.linspace(df_temp['vec_y'].values[0],df_temp['vec_y'].values[-1],len(df_temp))
            # adjust the velocity such that there will be no reverse action.
            if df_temp['vec_y_calibrate'].mean()>=0:
                df_temp['vec_y_calibrate'] = df_temp['vec_y_calibrate'].clip(lower=0)
            elif df_temp['vec_y_calibrate'].mean()<0:
                df_temp['vec_y_calibrate'] = df_temp['vec_y_calibrate'].clip(upper=0)
            df_temp['dist_y'] = np.concatenate((np.array([0]),cumtrapz(df_temp['vec_y_calibrate'],dx = 3/1000)))


            df_.loc[df_temp.index,'vec_y'] = df_temp['vec_y']
            df_.loc[df_temp.index,'vec_y_calibrate'] = df_temp['vec_y_calibrate']
            df_.loc[df_temp.index,'dist_y'] = df_temp['dist_y']
            try:
                _correction = df_station_to_station_distance[(eachrow['next_station_name']==df_station_to_station_distance['To'])&
                                                             (eachrow['station_name']==df_station_to_station_distance['From'])&
                                                             (df_station_to_station_distance['Line']==linename)]['Chainage'].values[0]*1000

                df_.loc[df_temp.index,'dist_y_norm'] = df_temp['dist_y']/np.abs(df_temp['dist_y'].values[-1])*_correction
            except:
                df_.loc[df_temp.index,'dist_y_norm'] = df_temp['dist_y']
    print('')
    if return_dict==None:
        return df_
    else:
        return_dict.put(df_)
        return

def upload_result_to_server(folderpath):
    server=_pw_sftp['address']
    user=_pw_sftp['username']
    password=_pw_sftp['password']
    try:
        ftp = ftplib.FTP(server)
        ftp.login(user,password)
        #enter the folder
        ftp.cwd('E200006-VSensor_Track_Monitoring')
        ftp.cwd('checkpoint')
        try:
            ftp.mkd(os.path.basename(folderpath))
        except:
            pass
        ftp.cwd(os.path.basename(folderpath))
        placeFiles(ftp,folderpath)
    except:
        pass

    ftp.quit()
    return

def placeFiles(ftp, path):
#path = ./checkpoint/versionXX/

    for name in os.listdir(path):
        localpath = os.path.join(path, name)
        if os.path.isfile(localpath):
            print("STOR", name, localpath)
            ftp.storbinary('STOR ' + name, open(localpath,'rb'))
        elif os.path.isdir(localpath):
            print("MKD", name)

            try:
                ftp.mkd(name)

            # ignore "directory already exists"
            except error_perm as e:
                if not e.args[0].startswith('550'):
                    raise

            print("CWD", name)
            ftp.cwd(name)
            placeFiles(ftp, localpath)
            print("CWD", "..")
            ftp.cwd("..")

def read_parquet(checkpoint_path,start_time,end_time):
    df = pd.read_parquet(os.path.join(checkpoint_path,'df.parquet.gzip'))
    df_TSSW = pd.read_csv(os.path.join(checkpoint_path,'df_TSSW.csv'))
    df_TSSW['time_stamp_shift'] = pd.to_datetime(df_TSSW['time_stamp_shift'])
    df_TSSW['next_station time_stamp_shift'] = pd.to_datetime(df_TSSW['next_station time_stamp_shift'])
    df_TSSW['next_station_idle time_stamp_shift'] = pd.to_datetime(df_TSSW['next_station_idle time_stamp_shift'])
    visualize_shifting_result(df,df_TSSW,start_time,end_time)

def autofill_config(config):
    '''
    autofill "SOME" part of the config design for ISL/TWL/KTL
    assume only "dummy","path_Vsensor","linename","car_number","TSSW_folder"
    autogenerate the rest
    '''

    df = pd.read_table(os.path.join(config['current']['path_Vsensor'],random.choice([x for x in os.listdir(config['current']['path_Vsensor']) if x.endswith('.XLS')])),sep='\t', header=None,skiprows=1)
    df.columns = ['datetime','x','y','z']
    df['datetime_convert'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df['datetime'], 'D')
    df.sort_values('datetime_convert',inplace=True)
    df.reset_index(drop=True,inplace=True)

    config['current']['TSSW_start'] = config['current'].get('TSSW_start',(df.iloc[5,:]['datetime_convert']-dt.timedelta(days=7)).strftime('%Y%m%d'))
    config['current']['TSSW_end'] = config['current'].get('TSSW_end',(df.iloc[5,:]['datetime_convert']+dt.timedelta(days=7)).strftime('%Y%m%d'))

    config['current']['auto_time_shift'] = config['current'].get('auto_time_shift','True')
    config['current']['time_shift_value'] = config['current'].get('time_shift_value','0')
    config['current']['auto_z_offset'] = config['current'].get('auto_z_offset','True')
    config['current']['z_offset_value'] = config['current'].get('z_offset_value','0')
    config['current']['gen_detail'] = config['current'].get('gen_detail','True')
    config['current']['save_checkpoint_bool'] = config['current'].get('save_checkpoint_bool','True')
    config['current']['savecheckpoint_foldername'] = config['current'].get('savecheckpoint_foldername',os.path.join(os.path.dirname(os.path.dirname(config['current']['TSSW_folder'])),
                                                                                            "checkpoint","{}_{}".format(config['current']['dummy'],config['current']['linename'])))
    config['current']['forward_axis'] = config['current'].get('forward_axis','x')
    config['current']['lateral_axis'] = config['current'].get('lateral_axis','y')
    config['current']['vertical_axis'] = config['current'].get('vertical_axis','z')

    config['current']['idle_time_idle_threshold'] = config['current'].get('idle_time_idle_threshold','100')
    config['current']['idle_time_running_threshold'] = config['current'].get('idle_time_running_threshold','250')

    return config

def detect_transfer_complete(config):
    '''
    At the beginning of the program, count the file number. Wait 10 second and count again.
    If number match: proceed; else: next batch.
    '''
    _count_before = len(os.listdir(config['current']['path_Vsensor']))
    time.sleep(10)
    _count_after = len(os.listdir(config['current']['path_Vsensor']))

    if _count_before == _count_after:
        return True
    else:
        return False
