#!/usr/bin/env python
# coding: utf-8

# In[148]:



#last modified 2020-07-27


#########major updated in this version#########

#auto_set_font_size for remarks colu; fix font size for other columns

#download and read remarks from IMD
#insert table below the chart
#summary page with color
#EAL and TML detail in excel file
#update in lateral bolster threshold
#plot seperate into up and down track




import os
import pandas as pd
import numpy as np
import datetime
import xlsxwriter
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
import matplotlib.dates as mdates
import seaborn as sns
from pandas.plotting import register_matplotlib_converters
import PyPDF2
from shutil import copyfile
# In[149]:


#######Production version##########

today =datetime.datetime.now()
today_date = today.strftime("%Y%m%d") #20200619
today_hour = today.strftime("%H"+"00") #1400
last_hour_date = (today-datetime.timedelta(hours=1)).strftime("%H"+"00") #1300
yst_date = (today-datetime.timedelta(days=1)).strftime("%Y%m%d") #20200618
data_path = "C:/Users/rscmonc/Documents/SPIRT/download_data/Data"
remarks_path = 'C:/Users/rscmonc/Documents/SPIRT/download_data/Data/Remarks.csv'
last_7days_date = (today-datetime.timedelta(days=7)).strftime("%Y%m%d")  #20200611
interim_path = "C:/Users/rscmonc/Documents/SPIRT/storage"
output_path = "C:/Users/rscmonc/Documents/SPIRT/output"

#######Production version##########

####### development version#########
'''
today =datetime.datetime(year=2020,month=8,day=31,hour=12)
today_date = today.strftime("%Y%m%d") #20200619
today_hour = today.strftime("%H"+"00") #1400
last_hour_date = (today-datetime.timedelta(hours=1)).strftime("%H"+"00") #1300
yst_date = (today-datetime.timedelta(days=1)).strftime("%Y%m%d") #20200618
data_path = "C:/Users/CHENMaHH/Desktop/SPIRT/download_data/Data"
remarks_path = 'C:/Users/CHENMaHH/Desktop/SPIRT/download_data/Data/Remarks.csv'
last_7days_date = (today-datetime.timedelta(days=7)).strftime("%Y%m%d")  #20200611
interim_path = "C:/Users/CHENMaHH/Desktop/SPIRT/storage"
output_path = "C:/Users/CHENMaHH/Desktop/SPIRT/output"
'''
####### development version#########



try:
    os.mkdir(os.path.join(interim_path,today_date+today_hour))
except:
    pass

try:
    os.mkdir(os.path.join(output_path,today_date+today_hour))
except:
    pass

output_path = os.path.join(output_path,today_date+today_hour)
interim_path = os.path.join(interim_path,today_date+today_hour)

filename = sorted([i for i in os.listdir(data_path) if (i.startswith(today_date)) or (i.startswith(yst_date)) ],reverse=True)
assert len(filename)>0, print('no file found')
df = pd.concat([pd.read_csv(os.path.join(data_path,i)) for i in filename])
df = df.drop_duplicates(subset = ['linename','subtrackname','km','vehicle','dtstamp_hkt'],keep='last')
df.reset_index(drop=True,inplace=True)
df['datetime'] = pd.to_datetime(df['dtstamp_hkt'],format='%Y-%m-%d %H:%M:%S')
# only use the last 24 hours
df = df[df['datetime']>datetime.datetime(year=today.year,month=today.month,day=today.day,hour=today.hour)-datetime.timedelta(days=1)].copy()
df.drop_duplicates(inplace=True)
df.reset_index(drop=True,inplace=True)


global_page_count = 1
#df = pd.read_csv(os.path.join(data_path,filename))
if len(df)==0:
    print('empty df')
    exit()
# In[152]:


date = filename[:8]
ETLdata_filename = today_date+ today_hour +'_ETLdata.csv'
report_filename = today_date+today_hour +'_SPIRT_alarms.xlsx'


# In[153]:


#EAl Thershold
#vertical acceleration all
s1EAL_acc = 30
s2EAL_acc = 21
s3EAL_acc = 12

#ride jerk all
s1EAL_ride_jerk_all = 1
s2EAL_ride_jerk_all = 0.7
s3EAL_ride_jerk_all = 0.3

#Lateral Bolster Acceleration
s1EAL_bolaccy = 0.9 #0.7
s2EAL_bolaccy = 0.65 #0.45
s3EAL_bolaccy = 0.4 #0.2

#EAL dynamic track gauge
s1EAL_trackgauge = 1457
s2EAL_trackgauge = 9999
s3EAL_trackgauge = 9999

#TML Thershold
#vertical acceleration all
s1TML_acc = 30
s2TML_acc = 21
s3TML_acc = 12

#ride jerk all
s1TML_ride_jerk_all = 1
s2TML_ride_jerk_all = 0.7
s3TML_ride_jerk_all = 0.3

#Lateral Bolster Acceleration
s1TML_bolaccy = 0.9 #0.7
s2TML_bolaccy = 0.65 #0.45
s3TML_bolaccy = 0.4 #0.2


# In[154]:


df['ride_jerk_all'] = df[['bod1_lat_jerk','bod1_lon_jerk','bod2_lat_jerk','bod2_lon_jerk']].values.max(1)


# In[155]:


df_TML = df[['linename','subtrackname','km','vehicle','dtstamp_hkt','speed','acc','ride_jerk_all','bolaccy']].copy()
df_EAL = df[['linename','subtrackname','km','vehicle','dtstamp_hkt','speed','acc','ride_jerk_all','bolaccy','gauge']].copy()
df_TML = df_TML[df_TML['linename'].str.contains('TML')]
df_EAL = df_EAL[~df_EAL['linename'].str.contains('TML')]


# In[156]:


def EAL_severity(input_col,output_col,s1,s2,s3):
    conditions = [
        ((df_EAL[input_col] >= s3) & (df_EAL[input_col] < s2)),
        ((df_EAL[input_col] >= s2) & (df_EAL[input_col] < s1)),
        (df_EAL[input_col] >= s1)]
    choices = [3,2,1]

    df_EAL[output_col] = np.select(conditions, choices, default=4)

EAL_severity('acc','severity_acc', s1EAL_acc, s2EAL_acc, s3EAL_acc)
EAL_severity('ride_jerk_all','severity_ride_jerk_all', s1EAL_ride_jerk_all, s2EAL_ride_jerk_all, s3EAL_ride_jerk_all)
EAL_severity('bolaccy','severity_bolaccy', s1EAL_bolaccy, s2EAL_bolaccy, s3EAL_bolaccy)
EAL_severity('gauge','severity_gauge', s1EAL_trackgauge, s2EAL_trackgauge, s3EAL_trackgauge)
df_EAL['severity_overall'] = df_EAL[['severity_acc','severity_ride_jerk_all','severity_bolaccy','severity_gauge']].values.min(1)


# In[157]:


def TML_severity(input_col,output_col,s1,s2,s3):
    conditions = [
        ((df_TML[input_col] >= s3) & (df_TML[input_col] < s2)),
        ((df_TML[input_col] >= s2) & (df_TML[input_col] < s1)),
        (df_TML[input_col] >= s1)]
    choices = [3,2,1]

    df_TML[output_col] = np.select(conditions, choices, default=4)

TML_severity('acc','severity_acc', s1TML_acc, s2TML_acc, s3TML_acc)
TML_severity('ride_jerk_all','severity_ride_jerk_all', s1TML_ride_jerk_all, s2TML_ride_jerk_all, s3TML_ride_jerk_all)
TML_severity('bolaccy','severity_bolaccy', s1TML_bolaccy, s2TML_bolaccy, s3TML_bolaccy)
df_TML['severity_overall'] = df_TML[['severity_acc','severity_ride_jerk_all','severity_bolaccy']].values.min(1)


# In[158]:


df2 = pd.concat([df_EAL, df_TML], ignore_index=True, sort =False)


# In[159]:


df2.to_csv(os.path.join(interim_path,ETLdata_filename),index=False)


# In[160]:


#df_sum = df2[df2['severity_overall']==1]


# In[161]:


#summary page
df2['dtstamp_hkt'] = df2['dtstamp_hkt'].astype('datetime64[ns]')

df_sum = df2[df2['severity_overall']==1]

df_sum=df_sum.groupby(['linename','subtrackname','km'],as_index=False).agg(
    {

        'acc':max,
        'ride_jerk_all':max,
        'bolaccy':max,
        'gauge':max,
        'speed':'count',
        'severity_overall':min,
        'dtstamp_hkt':max
    }
)

df_sum.sort_values(by=['linename','severity_overall','speed'], ascending=[True,True,False], inplace=True)
df_sum.rename(columns = {'speed':'s1_trip_count'}, inplace = True)
df_sum = df_sum[df_sum['severity_overall']==1]
df_sum.columns = ['line','subtrackname','km','vertical_acc_max', 'ride_jerk_all_max','bostler_lateral_acc_max','track_gauge_max','s1_trips_count','serverity_overall','last_alarm_time']
df_sum['serverity_overall']='S1'


# merge with remarks.csv from IMD
df_remarks = pd.read_csv(remarks_path)
i, j = np.where((df_sum['km'].values[:, None]>=df_remarks['From_km'].values)
                & (df_sum['km'].values[:, None]<=df_remarks['To_km'].values)
               & (df_sum['subtrackname'].values[:,None]==df_remarks['Trackname'].values))
df_result = pd.DataFrame(
    np.column_stack([df_sum.values[i], df_remarks.values[j]]),
    columns=df_sum.columns.append(df_remarks.columns)
).append(
    df_sum[~np.in1d(np.arange(len(df_sum)), np.unique(i))],
    ignore_index=True, sort=False
).sort_values(['line','s1_trips_count'],ascending=[True,False])

#in case no col remarks
if 'Remarks' not in df_result.columns:
    df_result['Remarks'] = None

df_result.drop_duplicates(subset=['line','subtrackname','km','vertical_acc_max', 'ride_jerk_all_max','bostler_lateral_acc_max','track_gauge_max','s1_trips_count','serverity_overall','last_alarm_time'],inplace=True,keep='last')
df_sum = df_result.loc[:,['line','subtrackname','km','vertical_acc_max', 'ride_jerk_all_max','bostler_lateral_acc_max','track_gauge_max','s1_trips_count','serverity_overall','last_alarm_time','Remarks']].reset_index(drop=True)


#details page
df3 = df2.drop(columns=['severity_acc','severity_ride_jerk_all','severity_bolaccy','severity_gauge']).copy()


# In[167]:


df3 = df3[df3.severity_overall<4]


# In[168]:


df3.sort_values(by=['severity_overall','km'], ascending=True, inplace=True)


# In[169]:


d = {1 :'S1', 2:'S2', 3:'S3'}


# In[170]:


df3['severity_overall']=df3['severity_overall'].map(d)


# In[171]:


df3.columns = ['line','subtrackname','km','vehicle','dtstamp_hkt','speed','vertical_acc', 'ride_jerk_all','bostler_lateral_acc','gauge','serverity_overall']


# In[172]:


df3_TML = df3[df3['line'].str.contains('TML')]
df3_EAL = df3[~df3['line'].str.contains('TML')]


# In[ ]:





# In[173]:


#write to xlsx
writer = pd.ExcelWriter(os.path.join(output_path,report_filename), engine='xlsxwriter')


# In[174]:


df_sum.to_excel(writer, sheet_name='s1_summary', index=False)


# In[175]:


df3_EAL.to_excel(writer, sheet_name='EAL_details', index=False)
df3_TML.to_excel(writer, sheet_name='TML_details', index=False)


# In[176]:


workbook  = writer.book
worksheet1 = writer.sheets['s1_summary']
worksheet2 = writer.sheets['EAL_details']
worksheet3 = writer.sheets['TML_details']


# In[177]:


# S1
format1 = workbook.add_format({'bg_color': '#FFC7CE',
                               'font_color': '#9C0006'})

# S2
format2 = workbook.add_format({'bg_color':   '#F5C47B',
                               'font_color': '#974706'})

# S3
format3 = workbook.add_format({'bg_color':   '#F7F293',
                               'font_color': '#000000'})


# In[178]:


# Conditional formating summary sheet
EAL_count = len(df_sum[~df_sum['line'].str.contains('TML')])
TML_count = len(df_sum[df_sum['line']=='TML'])

if EAL_count > 0:
    cell_EAL_vacc = 'D2:D' + str(EAL_count +1)
    cell_EAL_jerk = 'E2:E' + str(EAL_count +1)
    cell_EAL_lacc = 'F2:F' + str(EAL_count +1)
    cell_EAL_gauge = 'G2:G' + str(EAL_count +1)

    cell_TML_vacc = 'D'+ str(EAL_count+2) + ':D' + str(EAL_count+TML_count +1)
    cell_TML_jerk = 'E'+ str(EAL_count+2) + ':E' + str(EAL_count+TML_count +1)
    cell_TML_lacc = 'F'+ str(EAL_count+2) + ':F' + str(EAL_count+TML_count +1)
    cell_TML_gauge = 'G'+ str(EAL_count+2) + ':G' + str(EAL_count+TML_count +1)

    #EAL vertical acceleration all
    worksheet1.conditional_format(cell_EAL_vacc, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s3EAL_acc,
                                             'maximum': s2EAL_acc,
                                             'format': format3})
    worksheet1.conditional_format(cell_EAL_vacc, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s2EAL_acc,
                                             'maximum': s1EAL_acc,
                                             'format': format2})
    worksheet1.conditional_format(cell_EAL_vacc, {'type': 'cell',
                                             'criteria': '>=',
                                             'value': s1EAL_acc,
                                             'format': format1})
    #EAL ride jerk all
    worksheet1.conditional_format(cell_EAL_jerk, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s3EAL_ride_jerk_all,
                                             'maximum': s2EAL_ride_jerk_all,
                                             'format': format3})
    worksheet1.conditional_format(cell_EAL_jerk, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s2EAL_ride_jerk_all,
                                             'maximum': s1EAL_ride_jerk_all,
                                             'format': format2})
    worksheet1.conditional_format(cell_EAL_jerk, {'type': 'cell',
                                             'criteria': '>=',
                                             'value': s1EAL_ride_jerk_all,
                                             'format': format1})
    #EAL Lateral Bolster Acceleration
    worksheet1.conditional_format(cell_EAL_lacc, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s3EAL_bolaccy,
                                             'maximum': s2EAL_bolaccy,
                                             'format': format3})
    worksheet1.conditional_format(cell_EAL_lacc, {'type': 'cell',
                                             'criteria': 'between',
                                             'minimum': s2EAL_bolaccy,
                                             'maximum': s1EAL_bolaccy,
                                             'format': format2})
    worksheet1.conditional_format(cell_EAL_lacc, {'type': 'cell',
                                             'criteria': '>=',
                                             'value': s1EAL_bolaccy,
                                             'format': format1})

    #EAL Gauge
    worksheet1.conditional_format(cell_EAL_gauge, {'type': 'cell',
                                             'criteria': '>=',
                                             'value': s1EAL_trackgauge,
                                             'format': format1})

else:

    cell_TML_vacc = 'D2:D' + str(TML_count +1)
    cell_TML_jerk = 'E2:E' + str(TML_count +1)
    cell_TML_lacc = 'F2:F' + str(TML_count +1)
    cell_TML_gauge = 'G2:G' + str(TML_count +1)



#TML
#TML vertical acceleration all
worksheet1.conditional_format(cell_TML_vacc, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_acc,
                                         'maximum': s2TML_acc,
                                         'format': format3})
worksheet1.conditional_format(cell_TML_vacc, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_acc,
                                         'maximum': s1TML_acc,
                                         'format': format2})
worksheet1.conditional_format(cell_TML_vacc, {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_acc,
                                         'format': format1})

#TML ride jerk all
worksheet1.conditional_format(cell_TML_jerk, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_ride_jerk_all,
                                         'maximum': s2TML_ride_jerk_all,
                                         'format': format3})
worksheet1.conditional_format(cell_TML_jerk, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_ride_jerk_all,
                                         'maximum': s1TML_ride_jerk_all,
                                         'format': format2})
worksheet1.conditional_format(cell_TML_jerk, {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_ride_jerk_all,
                                         'format': format1})

#TML Lateral Bolster Acceleration
worksheet1.conditional_format(cell_TML_lacc, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_bolaccy,
                                         'maximum': s2TML_bolaccy,
                                         'format': format3})
worksheet1.conditional_format(cell_TML_lacc, {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_bolaccy,
                                         'maximum': s1TML_bolaccy,
                                         'format': format2})
worksheet1.conditional_format(cell_TML_lacc, {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_bolaccy,
                                         'format': format1})



#Conditional Formating EAL Sheet alarm

#vertical acceleration all
worksheet2.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3EAL_acc,
                                         'maximum': s2EAL_acc,
                                         'format': format3})
worksheet2.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2EAL_acc,
                                         'maximum': s1EAL_acc,
                                         'format': format2})
worksheet2.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1EAL_acc,
                                         'format': format1})



#ride jerk all
worksheet2.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3EAL_ride_jerk_all,
                                         'maximum': s2EAL_ride_jerk_all,
                                         'format': format3})
worksheet2.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2EAL_ride_jerk_all,
                                         'maximum': s1EAL_ride_jerk_all,
                                         'format': format2})
worksheet2.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1EAL_ride_jerk_all,
                                         'format': format1})

#Lateral Bolster Acceleration
worksheet2.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3EAL_bolaccy,
                                         'maximum': s2EAL_bolaccy,
                                         'format': format3})
worksheet2.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2EAL_bolaccy,
                                         'maximum': s1EAL_bolaccy,
                                         'format': format2})
worksheet2.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1EAL_bolaccy,
                                         'format': format1})

#Gauge
worksheet2.conditional_format('J2:J1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1EAL_trackgauge,
                                         'format': format1})

# In[180]:


#Conditional Formating TML Sheet alarm

#vertical acceleration all
worksheet3.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_acc,
                                         'maximum': s2TML_acc,
                                         'format': format3})
worksheet3.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_acc,
                                         'maximum': s1TML_acc,
                                         'format': format2})
worksheet3.conditional_format('G2:G1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_acc,
                                         'format': format1})


#ride jerk all
worksheet3.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_ride_jerk_all,
                                         'maximum': s2TML_ride_jerk_all,
                                         'format': format3})
worksheet3.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_ride_jerk_all,
                                         'maximum': s1TML_ride_jerk_all,
                                         'format': format2})
worksheet3.conditional_format('H2:H1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_ride_jerk_all,
                                         'format': format1})

#Lateral Bolster Acceleration
worksheet3.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s3TML_bolaccy,
                                         'maximum': s2TML_bolaccy,
                                         'format': format3})
worksheet3.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': 'between',
                                         'minimum': s2TML_bolaccy,
                                         'maximum': s1TML_bolaccy,
                                         'format': format2})
worksheet3.conditional_format('I2:I1048576', {'type': 'cell',
                                         'criteria': '>=',
                                         'value': s1TML_bolaccy,
                                         'format': format1})

# In[181]:


#set column width
worksheet1.set_column('A:A',7)
worksheet1.set_column('B:B',28)
worksheet1.set_column('C:J',17)
worksheet1.set_column('K:K',70)

worksheet2.set_column('A:A',7)
worksheet2.set_column('B:B',28)
worksheet2.set_column('C:K',17)

worksheet3.set_column('A:A',7)
worksheet3.set_column('B:B',28)
worksheet3.set_column('C:K',17)


# In[182]:


workbook.close()


# In[187]:


def plot_graph(df,linename_list,xaxis_min,x_axis_max,title,filename,df_remarks,remarks_fs = None):
    #df_bottom = remarks table from googleDrive
    df_bottom=df_remarks.copy()
    df_temp = df[df['linename'].isin(linename_list)].copy()
    if len(df_temp)==0:
        return

    def int_to_roman(num):
        _values = [1000000, 900000, 500000, 400000, 100000, 90000, 50000, 40000, 10000, 9000, 5000, 4000, 1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]

        _strings = ['M', 'C', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', "M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"]

        result = ""
        decimal = num

        while decimal > 0:
            for i in range(len(_values)):
                if decimal >= _values[i]:
                    if _values[i] > 1000:
                        result += u'\u0304'.join(list(_strings[i])) + u'\u0304'
                    else:
                        result += _strings[i]
                    decimal -= _values[i]
                    break
        return result
    def int_to_english(num):
        _dict = {1:'A',2:'B',3:'C',4:'D',5:'E'}
        return _dict.get(num,'')



    def plot_historgram(df,ax,label='',remarks_annotation=None):
        df_s1 = df[(df['severity_acc']==1)].copy()
        df_s2 = df[(df['severity_ride_jerk_all']==1)].copy()
        df_s3 = df[(df['severity_bolaccy']==1)].copy()
        df_s4 = df[(df['severity_gauge']==1)].copy()
        _data = [df_s1['km'],df_s2['km'],df_s3['km'],df_s4['km']]

        x,y,_= ax.hist(_data,np.arange(xaxis_min,x_axis_max,0.1),stacked=True,color=['blue','orange','red','green'],label=['vertical acc','ride jerk all','lateral acc','gauge'])
        ax.xaxis.set_major_locator(MultipleLocator(1))
        ax.xaxis.set_minor_locator(MultipleLocator(0.1))
        ax.yaxis.set_major_locator(MultipleLocator(1))
        #ax.set_ylim([0,x[-1].max()+5])
        ax.tick_params(which='both', width=1,bottom=True)
        ax.grid(True)
        ax.set_ylabel(label)

        if type(remarks_annotation)==pd.DataFrame:
            #if there are remarks
            try:
                for i,r in remarks_annotation.iterrows():
                    ax.text(x=r['km'],y=ax.get_ylim()[1]+0.5,s=r['id'],fontsize=8)
            except:
                pass
        return

    #divide the figure into 3 parts if there are remarks
    if len(df_bottom)>0:
        fig,axs = plt.subplots(3,figsize=[25,8],sharex=True,sharey=True)
        fig.suptitle(title)

        df_bottom['id'] = np.arange(len(df_bottom))+1
        df_bottom['id'] = df_bottom['id'].apply(lambda x: int_to_english(x))
        df_bottom_copy = df_bottom[['id','subtrackname','km','Remarks']].copy()
        #df_bottom_copy = pd.concat([df_bottom_copy,df_bottom_copy,df_bottom_copy])
        df_bottom_copy['Remarks']=df_bottom['Remarks'].str.replace('\r\n',' ').str.replace('\n',' ').str.replace('\r',' ')

        plot_historgram(df_temp[df_temp['subtrackname'].str.contains('Up')],axs[1],label='Up Track',remarks_annotation=df_bottom_copy[df_bottom_copy['subtrackname'].str.contains('Up Track')])
        plot_historgram(df_temp[df_temp['subtrackname'].str.contains('Down')],axs[2],label='Down Track',remarks_annotation=df_bottom_copy[df_bottom_copy['subtrackname'].str.contains('Down Track')])
        y_lim_bottom,y_lim_top = plt.ylim()
        plt.ylim(top = max(5,y_lim_top+1))

        if len(df_bottom_copy)>5:
            table = axs[0].table(cellText = df_bottom_copy.head(5).append(pd.DataFrame({'id':[''],'subtrackname':[''],'km':[''],'Remarks':['more remarks in the excel file']})).values,colLabels = df_bottom_copy.columns,loc='center',cellLoc='left',colWidths=[0.02,0.1,0.08,0.7])
        else:
            table = axs[0].table(cellText = df_bottom_copy.values,colLabels = df_bottom_copy.columns,loc='center',cellLoc='left',colWidths=[0.02,0.1,0.08,0.7])

        if remarks_fs==None:
        # if first time run, no predefined font size for remarks column
            remarks_font_size = table.auto_set_font_size(True)
        else:
            table.auto_set_font_size(False)
            print('fontsize {}'.format(remarks_fs))
            for (row, col), cell in table.get_celld().items():
                if (col == 3) and  (row!=0) :
                    cell.set_fontsize(remarks_fs)
                else:
                    cell.set_fontsize(9)

        table.scale(1,1.2)
        axs[0].axis('off')
        axs[1].legend(loc=1)

    #divide the figure into 2 parts if there no remarks
    else:
        fig,axs = plt.subplots(2,figsize=[25,8],sharex=True,sharey=True)
        fig.suptitle(title)

        plot_historgram(df_temp[df_temp['subtrackname'].str.contains('Up')],axs[0],label='Up Track')
        plot_historgram(df_temp[df_temp['subtrackname'].str.contains('Down')],axs[1],label='Down Track')
        y_lim_bottom,y_lim_top = plt.ylim()
        plt.ylim(top = max(5,y_lim_top+1))
        axs[0].legend(loc=1)
        table = None

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(filename)

    return table #return table if there is a table, else return None


# In[188]:


table1 = plot_graph(df2,['EAL','LMC'],-0.2,37.5,'EAL : count of S1 alarm in the last 24-hour',os.path.join(interim_path,'0001-Historgram_EAL.pdf'),df_sum[(~df_sum['line'].str.contains('TML'))&(pd.isnull(df_sum['Remarks'])==False)])
if table1!=None:
    table1 = plot_graph(df2,['EAL','LMC'],-0.2,37.5,'EAL : count of S1 alarm in the last 24-hour',os.path.join(interim_path,'0001-Historgram_EAL.pdf'),df_sum[(~df_sum['line'].str.contains('TML'))&(pd.isnull(df_sum['Remarks'])==False)],remarks_fs = table1.get_celld()[1,0].get_fontsize())
table2 = plot_graph(df2,['TML'],81,99,'TML : count of S1 alarm in the last 24-hour',os.path.join(interim_path,'0002-Historgram_TML.pdf'),df_sum[(df_sum['line'].str.contains('TML'))&(pd.isnull(df_sum['Remarks'])==False)])
if table2!=None:
    table2 = plot_graph(df2,['TML'],81,99,'TML : count of S1 alarm in the last 24-hour',os.path.join(interim_path,'0002-Historgram_TML.pdf'),df_sum[(df_sum['line'].str.contains('TML'))&(pd.isnull(df_sum['Remarks'])==False)],remarks_fs = table2.get_celld()[1,0].get_fontsize())

# In[ ]:

df_rolling = [pd.read_csv(os.path.join(data_path,i)) for i in os.listdir(data_path) if (i[:8]>=last_7days_date) & (i.startswith('20'))]
df_rolling = pd.concat(df_rolling)

def plot_trend(df_total,chainage,linename,subtrackname,xaxis_max,xaxis_min,current_hour,threshold):
    global global_page_count
    title = linename+ "_"+ subtrackname + "_" + str(chainage)
    filename=os.path.join(interim_path,str(global_page_count).zfill(3)+"-"+title+'.pdf')
    global_page_count+=1
    df = df_total[(df_total['linename']==linename)&
                  (df_total['km']==chainage)&
                  (df_total['subtrackname']==subtrackname)].copy()
    df['datetime'] = pd.to_datetime(df['dtstamp_hkt'])
    df.sort_values('datetime')
    df.reset_index(drop=True,inplace=True)
    if len(df)==0:
        return

    df['ride_jerk_all'] = df[['bod1_lat_jerk','bod1_lon_jerk','bod2_lat_jerk','bod2_lon_jerk']].values.max(1)
    fig,axs = plt.subplots(len(threshold),figsize=[25,8],sharex=True)
    axs[0].set_xlim([datetime.datetime(year=int(xaxis_min[:4]),month=int(xaxis_min[4:6]),day=int(xaxis_min[6:8]),hour=int(current_hour[:2])),
                    datetime.datetime(year=int(xaxis_max[:4]),month=int(xaxis_max[4:6]),day=int(xaxis_max[6:8]),hour=int(current_hour[:2]))])
    fig.suptitle(title)


    def plot_trend_1(df,col_name,ax,label='',color='k',s1threshold=None):

        ax.scatter(df['datetime'],df[col_name],color=color)

        #ax.xaxis.set_major_locator(mdates.HourLocator(interval=12))
        ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        #ax.yaxis.set_major_locator(MultipleLocator(1))
        #ax.set_ylim([0,x[-1].max()+5])
        ax.axhline(s1threshold,color='red',ls='--',label='s1 threshold')
        #ax.text(x = 10,y = s1threshold+0.5,s = 's1 threshold')

        ax.tick_params(which='both', width=1,bottom=True)
        ax.grid(True)
        ax.set_ylabel(label)
        return

    plot_trend_1(df,'ride_jerk_all',axs[0],label='Ride Jerk All',color='orange',s1threshold=threshold[2])
    plot_trend_1(df,'bolaccy',axs[1],label='Lateral Bolster Accel',color='red',s1threshold=threshold[1])
    plot_trend_1(df,'acc',axs[2],label='Vertical Accel',color='blue',s1threshold=threshold[0])
    if len(threshold)==4:
        plot_trend_1(df,'gauge',axs[3],label='Track Gauge',color='green',s1threshold=threshold[3])
    l,s = axs[-1].get_legend_handles_labels()
    fig.legend(l,s,loc='upper right')
    #y_lim_bottom,y_lim_top = plt.ylim()
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(filename)

    return
if len(df_sum)>0:
    try:
        for x,y in df_sum.iterrows():
            if y['line'] in ['EAL','LMC']:
                plot_trend(df_rolling,y['km'],y['line'],y['subtrackname'],today_date,last_7days_date,today_hour,[s1EAL_acc,s1EAL_bolaccy,s1EAL_ride_jerk_all,s1EAL_trackgauge])
            elif y['line']=='TML':
                plot_trend(df_rolling,y['km'],y['line'],y['subtrackname'],today_date,last_7days_date,today_hour,[s1TML_acc,s1TML_bolaccy,s1TML_ride_jerk_all])

    except:
        pass


def merge_pdf(path_from,path_to,userfilename):
    pdf2merge = []
    for filename in os.listdir(path_from):
        if filename.endswith('.pdf'):
            pdf2merge.append(filename)
    pdf2merge = sorted(pdf2merge,key=lambda x: x[:3])
    pdfWriter = PyPDF2.PdfFileWriter()

    #loop through all PDFs
    for filename in pdf2merge:
        #rb for read binary
        pdfFileObj = open(os.path.join(path_from,filename),'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        #Opening each page of the PDF
        for pageNum in range(pdfReader.numPages):
            pageObj = pdfReader.getPage(pageNum)
            pdfWriter.addPage(pageObj)
    #save PDF to file, wb for write binary
    pdfOutput = open(os.path.join(path_to,userfilename), 'wb')
    #Outputting the PDF
    pdfWriter.write(pdfOutput)
    #Closing the PDF writer
    pdfOutput.close()

    return
try:
    merge_pdf(interim_path,output_path,'{}_SPIRT_graphs.pdf'.format(today_date+today_hour))
    copyfile(remarks_path,os.path.join(interim_path,'Remarks.csv'))
except:
    pass
