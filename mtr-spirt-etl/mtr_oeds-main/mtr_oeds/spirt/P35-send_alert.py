import sys
import os
import win32com.client as win32
import glob
import datetime as dt
import shutil
import configparser
import logging
import pandas as pd
import datetime

def send_email(to_list,cur_time):

    '''
    Send warning email if no IRT files updated
    '''

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = to_list
    #send on behalf of code
    outlookaccount = None
    for oacc in outlook.Session.Accounts:
        print('available account: '+ oacc.SmtpAddress.lower())
        if oacc.SmtpAddress.lower() == 'dstudio@mtr.com.hk':
            outlookaccount=oacc
            break
    if outlookaccount:
        print('using account:'+ outlookaccount.SmtpAddress.lower())
        mail._oleobj_.Invoke(*(64209, 0, 8, 0, outlookaccount))
        #mail.SendUsingAccount=outlookaccount
    #end of the on behalf code


    mail.Subject = 'AUTO ALERT - SPIRT file missing at {}'.format(cur_time)

    mail.display(False)
    mail.Send()
def record_temp(message):
    print(message)

    #with open('temp.txt','a+') as f:
    #    f.write('{} {} \n'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),message))

    try:
        os.remove('log_temp.txt')
    except:
        pass
    with open('temp.txt','r') as f: data = f.read()
    data = data.split('\n')
    if len(data)>240: data = data[:240]
    data = '\n'.join(data)

    with open('log_temp.txt','w+') as f: f.write('{} {} \n'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),message) + data)
    os.remove('temp.txt')
    os.rename('log_temp.txt','temp.txt')

    return

today = dt.datetime.today()
today_str = today.strftime('%Y%m%d')
today_hour = today.strftime("%H"+"00") #1400
three_hour_before = today.replace(minute=0, second=0, microsecond=0)- dt.timedelta(hours=3)
search_range = [(today.replace(minute=0, second=0, microsecond=0)- dt.timedelta(hours=i)).strftime('%Y%m%d_%H%M') for i in [1,2,3]]
print(search_range)
to_list = 'dstudio@mtr.com.hk'
data_path = "C:/Users/rscmonc/Documents/SPIRT/download_data/Data"

file_list = [i for i in os.listdir(data_path) if (i[:13] in search_range) & (i.endswith('data.csv'))]
if len(file_list)==0:
    send_email(to_list,today_str+today_hour)
    record_temp('alert email sent at {}'.format(today_str+today_hour))
