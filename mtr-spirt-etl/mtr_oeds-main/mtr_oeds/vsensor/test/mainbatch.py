import os
import re
import sys
import numpy as np
import pandas as pd
import configparser
import argparse
import matplotlib
import matplotlib.pyplot as plt
import datetime as dt
from mtr_oeds.vsensor import distance_mapping_util as dmu

def main(config,load_checkpoint_path=None,
         read_nrow=None,verbose=True):
    '''
    input:
        auto_time_shift = boolean
        time_shift_value = integer
        auto_z_offset = boolean
        z_offset_value = integer

    '''
    #read from config
    auto_time_shift = config['current'].getboolean('auto_time_shift',True)
    time_shift_value = int(config['current'].get('time_shift_value','0'))
    auto_z_offset =  config['current'].getboolean('auto_z_offset',True)
    z_offset_value = int(config['current'].get('z_offset_value','0'))
    save_checkpoint_bool= config['current'].getboolean('save_checkpoint_bool',True)
    savecheckpoint_foldername=config['current']['savecheckpoint_foldername']
    gen_detail = config['current'].getboolean('gen_detail',True)
    _manual_vsensor_shift = int(config['current'].get('manual_vsensor_shift','0')) #only use when faulty vsensor
    _idle_time_idle_threshold = int(config['current'].get('idle_time_idle_threshold','100'))
    _idle_time_running_threshold = int(config['current'].get('idle_time_running_threshold','250'))

    #create folder
    if save_checkpoint_bool:
        try:
            os.mkdir(savecheckpoint_foldername)
        except:
            pass
        path_to_save = savecheckpoint_foldername
    else:
        path_to_save='./'

    # add log file location to config
    config['current']['path_to_save'] = path_to_save

    # log the config files used
    dmu.log_and_print('Start at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),config['current']['path_to_save'])
    for each in config['current']:
        dmu.log_and_print('current-{}: {}'.format(each,config['current'][each]),config['current']['path_to_save'])

    #load checkpoint if any
    if load_checkpoint_path:
        if read_nrow:
            df,df_TSSW,df_result = dmu.load_checkpoint(load_checkpoint_path,nrow=read_nrow)
            dmu.log_and_print('read part of the file from checkpoint at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                         config['current']['path_to_save'])
        else:
            df,df_TSSW,df_result = dmu.load_checkpoint(load_checkpoint_path)
            dmu.log_and_print('read file from checkpoint at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                         config['current']['path_to_save'])

    #read xls and merge only V Sensor files
    else:

        df = dmu.find_and_merge(config['current']['path_Vsensor'],auto_z_offset,z_offset_value,config,
                                _manual_shift = _manual_vsensor_shift)
        dmu.log_and_print('v sensor from {} to {}'.format(df['datetime_convert'].min(),df['datetime_convert'].max()),
                     config['current']['path_to_save'])
        dmu.log_and_print('finish merge raw files at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                     config['current']['path_to_save'])

        #read the relevant TSSW files
        df_TSSW = dmu.read_TSSW(config,df['datetime_convert'].min(),df['datetime_convert'].max())
        dmu.log_and_print('TSSW from {} to {}'.format(df_TSSW['time_stamp'].min(),df_TSSW['time_stamp'].max()),
                     config['current']['path_to_save'])
        dmu.log_and_print("Number of TSSW record: {}".format(len(df_TSSW)),config['current']['path_to_save'])
        df_result = pd.DataFrame()

        #trim part of the df data
        df = df[(df['datetime_convert']>=df_TSSW['time_stamp'].dt.round('h').min()-dt.timedelta(hours=1))&
                (df['datetime_convert']<=(df_TSSW['time_stamp'].dt.round('h').max()+dt.timedelta(hours=1)))].reset_index(drop=True)
        dmu.log_and_print('v sensor from {} to {}'.format(df['datetime_convert'].min(),df['datetime_convert'].max()),
                     config['current']['path_to_save'])

    if 'time_stamp_shift' not in df_TSSW.columns:
        #detect the time shift
        number_of_second = dmu.find_time_shift(df,df_TSSW,auto_time_shift,time_shift_value,config)
        dmu.log_and_print('{} of second has been shifted'.format(number_of_second),config['current']['path_to_save'])
        dmu.log_and_print('finish time shift at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                      config['current']['path_to_save'])
        df_TSSW['time_stamp_shift'] = df_TSSW['time_stamp']+dt.timedelta(seconds=int(number_of_second))

    if 'next_station_idle time_stamp_shift' not in df_result.columns:
        #detect the train stop time
        df_result = dmu.find_idle_timestamp(df,df_TSSW,_idle_threshold =_idle_time_idle_threshold , _running_threshold = _idle_time_running_threshold)
        df_result.reset_index(drop=True,inplace=True)
        dmu.log_and_print('finish estimating idle time at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                     config['current']['path_to_save'])

    #visualize the shift result (3 random plots)
    if verbose:
        try:
            for eachindex in np.arange(0,len(df_result),20)[:3]:
                f,a= dmu.visualize_shifting_result(df,df_result,
                                      (df_result.iloc[eachindex]['time_stamp']-dt.timedelta(minutes=0)).replace(second=0),
                                      (df_result.iloc[eachindex]['time_stamp']+dt.timedelta(minutes=30)).replace(second=0),
                                      filename=os.path.join(config['current']['path_to_save'],'dummy{}.png'.format(str(eachindex).zfill(3))))
        except:
            pass

    dmu.save_checkpoint(df,df_TSSW,df_result,config['current']['path_to_save'],save_df = False)
#########################################################
    df = dmu.estimate_displacement(df,df_result,config['current']['linename'])
    dmu.log_and_print('finish estimating displacement at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                 config['current']['path_to_save'])



    #print some of the trip for visual inspection!
    for eachindex in np.arange(0,len(df_result),20)[:10]:
         _1,_2 = dmu.visualize_shifting_result(df,df_result,
                                      (df_result.iloc[eachindex]['time_stamp']-dt.timedelta(minutes=10)).replace(second=0),
                                      (df_result.iloc[eachindex]['time_stamp']+dt.timedelta(minutes=20)).replace(second=0),
                                      filename=os.path.join(config['current']['path_to_save'],'trip_{}.png'.format(str(eachindex).zfill(5))))


    #generate report to csv
    df_result = dmu.generate_report(df_result,df,gen_detail=gen_detail)
    df_result['Code'] = df_result['station_name']+'-' + df_result['next_station_name']
    #dmu.save_report(df_result,config['current']['path_to_save'])


    #if there are speific folder name, save everything in that folder
    if save_checkpoint_bool:
        dmu.log_and_print('start checkpoint at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                     config['current']['path_to_save'])
        dmu.save_checkpoint(df,df_TSSW,df_result,config['current']['path_to_save'],save_df = True)
        dmu.log_and_print('finish checkpoint at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),
                     config['current']['path_to_save'])

    dmu.log_and_print('Finish at {}'.format(dt.datetime.now().strftime('%H:%M:%S')),config['current']['path_to_save'])
########################################################
    plt.close('all')
    return df,df_TSSW,df_result,f,a

if __name__=='__main__':

    #argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v","--version",help='version to run',type=str,nargs="+",required=True)
    parser.add_argument('-c','--config',help='config file to use',type=str,default='config_mainbatch.cfg')
    args = parser.parse_args()

    #read the configparser
    try:
        config = configparser.ConfigParser()
        config.read(args.config)
        print("Read config file from : {}".format(args.config))

    except Exception as e:
        print('read config fail')
        print(e)
        quit()

    for eachVersion in args.version:
        config['current']=config[eachVersion]
        config = dmu.autofill_config(config)
        if dmu.detect_transfer_complete(config)==True:
            df,df_TSSW,df_result,f,a = main(config)
            dmu.upload_result_to_server(config['current']['savecheckpoint_foldername'])
        else:
            print('{} is not proceed due to incomplete transfer'.format(eachVersion))
    print('main batch finished at {}'.format(dt.datetime.now().strftime('%H:%M:%S')))
