import numpy as np
import pandas as pd
import os
import datetime as dt
import configparser
import argparse
from mtr_oeds.vsensor import *
from mtr_oeds.common import df_vsensor_1
from mtr_oeds.vsensor.expert import slope_expert

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config",help='config filename',type=str,default='config_summary.cfg')
    args = parser.parse_args()
    try:
        config = configparser.ConfigParser()
        config.read(args.config)
        print("Read config file from : {}".format(args.config))

    except Exception as e:
        print('read config fail')
        print(e)
        quit()

    #location of the checkpoiint file
    _path_checkpoint_folder = config['general'].get('path','../checkpoint/') #location of the df_result.csv
    _gen_table=config['general'].getboolean('gen_table',True) #generate the excel file
    _output_location = config['general'].get('output_location','./') # where to save the output
    _output_filename = config['general'].get('output_filename','df_result.csv') #name of the output file, for tableau
    _line = config['general'].get('line','None')
    _dt_offset = float(config['general'].get('DT_offset','0'))
    _ut_offset = float(config['general'].get('UT_offset','0'))

    list_of_files_reference = {} #last 2 weeks df_result
    list_of_files_current = {} #reporting week df_result
    list_of_files_history = {} #historcial data

    for eachitem in config.items('reference'):
        list_of_files_reference[eachitem[0]] = os.path.join(_path_checkpoint_folder,eachitem[1])
    for eachitem in config.items('target'):
        list_of_files_current[eachitem[0]] = os.path.join(_path_checkpoint_folder,eachitem[1])
    if config.has_section('history'):
        for eachitem in config.items('history'):
            list_of_files_history[eachitem[0]] = os.path.join(_path_checkpoint_folder,eachitem[1])


    #df_chainage = pd.read_csv(config['general']['mapping_table_path']) #mapping tablue
    df_chainage = df_vsensor_1
    if _line!="None":
        try:
            df_chainage['Start'] = np.where((df_chainage['Line']==_line)&(df_chainage['Direction']=='Up'),df_chainage['Start']+_ut_offset,df_chainage['Start'])
            df_chainage['End'] = np.where((df_chainage['Line']==_line)&(df_chainage['Direction']=='Up'),df_chainage['End']+_ut_offset,df_chainage['End'])

            df_chainage['Start'] = np.where((df_chainage['Line']==_line)&(df_chainage['Direction']=='Down'),df_chainage['Start']+_dt_offset,df_chainage['Start'])
            df_chainage['End'] = np.where((df_chainage['Line']==_line)&(df_chainage['Direction']=='Down'),df_chainage['End']+_dt_offset,df_chainage['End'])

            df_chainage.to_csv('df_chainage_temp.csv')

            print('offset applied for downTrack headWall: {}'.format(_dt_offset))
            print('offset applied for upTrack headWall: {}'.format(_ut_offset))
        except:
            pass

    if len(list_of_files_current)!=0: df_current = concat_files(list_of_files_current)
    if len(list_of_files_history)!=0:
        df_history = concat_files(list_of_files_history)
    else:
        df_history = pd.DataFrame()

    if len(list_of_files_reference)!=0:
        df_reference = concat_files(list_of_files_reference)
        # groupby Code+segment to generate the grey area limit
        df_reference_grouped = df_reference.groupby(['Code','segment']).agg({'segment_z':['mean','std','median'],'segment_x':['mean','std','median']})
        df_reference_grouped.columns = ["_".join(x) for x in df_reference_grouped.columns.ravel()]
        df_reference_grouped.reset_index(inplace=True)
    else:
        df_reference=pd.DataFrame()

    try:
        df_final_current=pd.merge(df_current,df_reference_grouped,right_on=['Code','segment'],left_on=['Code','segment'],how='left')
        assert df_final_current.shape[0]==df_current.shape[0], print('wrong dimension final')
        df_final_current['_display_mode'] = 'current'
    except:
        print('current merge with reference fail')
        df_final_current=df_current
    df_final_current['_display_mode'] = 'current'

    try:
        df_final_reference=pd.merge(df_reference,df_reference_grouped,right_on=['Code','segment'],left_on=['Code','segment'],how='left')
        assert df_final_reference.shape[0]==df_reference.shape[0], print('wrong dimension final')
        df_final_reference['_display_mode'] = 'reference'
    except:
        print('reference merge with reference fail')
        df_final_reference=df_reference
    df_final_reference['_display_mode'] = 'reference'


    try:
        df_final_history=pd.merge(df_history,df_reference_grouped,right_on=['Code','segment'],left_on=['Code','segment'],how='left')
        assert df_final_history.shape[0]==df_history.shape[0], print('wrong dimension final')
        df_final_history['_display_mode'] = 'history'
    except:
        print('history merge with reference fail')
        df_final_history=df_history
    df_final_history['_display_mode'] = 'history'

    print('len of history week is {}'.format(len(df_final_history)))
    print('len of current week is {}'.format(len(df_final_current)))
    print('len of reference week is {}'.format(len(df_final_reference)))
    if len(df_history) != 0:
        df_final=pd.concat([df_final_history,df_final_current,df_final_reference],sort=False)
    else:
        df_final=pd.concat([df_final_current,df_final_reference],sort=False)
    print(df_final.groupby('version').apply(len))

    #add variable to make my life earier, for plotting in tableau
    df_final['_axis_max_x'] = np.round(np.max(df_final['segment_x']),-2)+200
    df_final['_axis_max_z'] = np.round(np.max(df_final['segment_z']),-2)+200
    try: #because refence may not exist
        df_final['_top_segment_x'] = df_final['segment_x_mean']+3*df_final['segment_x_std']
        df_final['_top_segment_z'] = df_final['segment_z_mean']+3*df_final['segment_z_std']
        df_final['_bot_segment_x'] = df_final['segment_x_mean']-3*df_final['segment_x_std']
        df_final['_bot_segment_z'] = df_final['segment_z_mean']-3*df_final['segment_z_std']
    except:
        pass
    #alignemnt the direction with mapping table
    df_final.drop(columns = 'direction',inplace=True)
    df_final1 = pd.merge(left=df_final,right=df_chainage[['Line','Code','Plot_Rank','Direction']],left_on=['line_id','Code'],right_on=['Line','Code'],how='inner')
    df_final = df_final1.copy()
    df_final.rename(columns = {'Plot_Rank':"_plot_order","Direction":"direction"},inplace=True)

    #should not happen, because above merge is inner
    df_final['direction'].fillna('-',inplace=True)
    df_final['_plot_order'].fillna(9999,inplace=True)

    #get what is the previous week and previouw month
    s_2month,s_month,s_week,end = get_startend_date()
    df_final['_show_in_selection'] = df_final['time_stamp'].apply(assign_show_in_selection,args=(s_week,s_month,s_2month,end))

    df_final.sort_values('_plot_order',inplace=True)
    df_final['_segment_order'] = df_final['segment'].str[-4:].astype(int)

    #create a temp dataframe
    df_temp_segment = df_final.groupby(['line_id','Code','segment','direction']).size().reset_index()
    df_temp_segment = df_temp_segment[['line_id','Code','segment','direction']]
    df_temp_segment['_segment_name'] = df_temp_segment.apply(segment_to_meters,axis=1,df_chainage=df_chainage,short_form=True)

    df_final = pd.merge(df_final,df_temp_segment,on=['line_id','Code','segment','direction'],how='inner')
    df_slope = df_final[df_final['_show_in_selection']<=8].groupby('_segment_name').apply(calculate_slope).reset_index()[['_segment_name','slope of lateral vibration','slope of vertical vibration']]
    df_final = slope_expert(df_final) #developing
    df_final.to_csv(os.path.join(_output_location,_output_filename),index=False, float_format='%g')

    if _gen_table==True:
        a,b,c,d,e = generate_report(df_final,df_chainage,df_slope)

        writer = pd.ExcelWriter('{}_summary_vsensor_mapping.xlsx'.format(dt.datetime.now().strftime('%Y%m%d')))
        a.to_excel(writer, sheet_name = 'lateral-byMagnitude',float_format='%g',index=False)
        b.to_excel(writer, sheet_name = 'vertical-byMagnitude',float_format='%g',index=False)
        c.to_excel(writer, sheet_name = 'lateral-byPercent',float_format='%g',index=False)
        d.to_excel(writer, sheet_name = 'vertical-byPercent',float_format='%g',index=False)
        e.to_excel(writer, sheet_name = 'combine-byPercent',float_format='%g',index=False)
        writer.save()
    print('Done')
