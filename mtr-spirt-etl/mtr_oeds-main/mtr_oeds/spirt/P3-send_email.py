####################################################
# Update history:
# v 0.1 2020-07-02 by Horace

#Major Update
# email sending logic bug fix, deal with NaT value comparison


# v 0.1
# - prepend message to temp.txt and only keep recent 240 record (~10days)
# - 4 addtional sentence in the body of the email and show '-' when error
# - only send email when 12 nn or new alaram to different email list
# - drop duplicated when concat hourly files

####################################################

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

def send_email(attachment_files, cur_time, file_path,mode):

    '''
    Send email to Daily Summary recipient
    '''

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)

    if not os.path.exists(file_path):
        logger.error("No email recipient config found. Please check.")
        return

    to_list, cc_list, bcc_list = read_recipient_config(file_path)

    if to_list:
        mail.To = ";".join(to_list)

    if cc_list:
        mail.Cc = ";".join(cc_list)

    if bcc_list:
        mail.Bcc = ";".join(bcc_list)


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

    try:
        if len(df_ETL[df_ETL['linename'].isin(['EAL','LMC'])])==0:
            content_sent1 = 'No data from SPIRT mtr01 (EAL) in the last 24 hour'
        else:
            content_sent1 = 'The last data from SPIRT mtr01 (EAL) was at {}'.format(df_ETL[df_ETL['linename'].isin(['EAL','LMC'])]['DateTime'].max())
    except:
        content_sent1 = '-'

    try:
        if len(df_ETL[df_ETL['linename']=='TML'])==0:
            content_sent2 = 'No data from SPIRT mtr02 (TML) in the last 24 hour'
        else:
            content_sent2 = 'The last data from SPIRT mtr02 (TML) was at {}'.format(df_ETL[df_ETL['linename'].isin(['TML'])]['DateTime'].max())
    except:
        content_sent2 = '-'

    try:
        if len(df_sum)==0:
            content_sent3 = 'There are no s1 alarm in the last 24 hours'
            content_sent4 = ''
        else:
            content_sent3 = 'The last s1 alaram was at {}'.format(df_sum['DateTime'].max())
            content_sent4 = 'There were {} s1 alarm in the last 24 hour'.format(df_sum['s1_trips_count'].sum())
    except:
        content_sent3 = '-'
        content_sent4 = '-'

    if mode=='new':
        mailSubject = 'SPIRT new s1 alarm {}'.format(cur_time)
    else:
        mailSubject = 'SPIRT daily alarm summary {}'.format(cur_time)

    if 'dev' in file_path:
        mail.Subject = '(TEST) '+mailSubject
    else:
        mail.Subject = mailSubject




    body_msg = "<span style='font-size:12.0pt;font-family:Calibri'>Dear Sirs,<br><br> \
    Please find attached last 24-hour summary of SPIRT alarms and alarm count distribution histogram updated for {} for your perusal.\
    <br> <br> \
    {} <br>\
    {} <br> <br>\
    {} <br>\
    {} <br>\
   </span>".format(cur_time,content_sent1,content_sent2,content_sent3,content_sent4)


    for attachment_file in attachment_files:
        mail.Attachments.Add(attachment_file)
    mail.GetInspector

    index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body'))
    mail.HTMLbody = mail.HTMLbody[:index + 1] + body_msg + mail.HTMLbody[index + 1:]

    print(mail.To)
    print(mail.Cc)
    print(mail.Bcc)

    mail.display(False)
    mail.Send()

def read_recipient_config(file_path):

    df = pd.read_excel(file_path, sheet_name="To")
    to_list = df['Email Recipient Address'].tolist()

    df = pd.read_excel(file_path, sheet_name="Cc")
    cc_list = df['Email Recipient Address'].tolist()

    df = pd.read_excel(file_path, sheet_name="Bcc")
    bcc_list = df['Email Recipient Address'].tolist()

    return to_list, cc_list, bcc_list

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
#config_path = r"daily_damage_report.cfg"
#log_path = r"daily_damage_report.log"

# create logger with 'daily_damage_report'
#logger = logging.getLogger('daily_damage_report')
#logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
#fh = logging.FileHandler(log_path)
#fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#fh.setFormatter(formatter)
# add the handlers to the logger
#logger.addHandler(fh)

# Get Login Credential from config
#try:
#    config = configparser.ConfigParser()
#    config.read(config_path)
#
#except:
#    logger.error("Error reading config", exc_info=True)
#    sys.exit(1)

#email_config = config["email"]

today = dt.datetime.today()
today_str = today.strftime('%Y%m%d')
today_hour = today.strftime("%H"+"00") #1400
three_hour_before = today.replace(minute=0, second=0, microsecond=0)- dt.timedelta(hours=3)
yst_date = (today-datetime.timedelta(days=1)).strftime("%Y%m%d")
#today = "20190808" # For testing
print(today_str+today_hour)
email_receipt_path_regular = "C:/Users/rscmonc/Documents/SPIRT/email_recipient_config.xlsx"
email_receipt_path_new = "C:/Users/rscmonc/Documents/SPIRT/email_recipient_config1.xlsx"
output_path = "C:/Users/rscmonc/Documents/SPIRT/output"
storage_path = "C:/Users/rscmonc/Documents/SPIRT/storage"

email_attachment_path = os.path.join(output_path,today_str+today_hour)
df_sum = pd.read_excel(os.path.join(email_attachment_path,[i for i in os.listdir(email_attachment_path) if i.endswith('.xlsx')][0]),sheet_name='s1_summary')
df_sum['DateTime'] = pd.to_datetime(df_sum['last_alarm_time'])
_temp = sorted(os.listdir(output_path))
if _temp[-1] == (today_str+today_hour):
    try:
        email_attachment_path_prev = os.path.join(output_path,_temp[-2])
        df_sum_prev = pd.read_excel(os.path.join(email_attachment_path_prev,[i for i in os.listdir(email_attachment_path_prev) if i.endswith('.xlsx')][0]),sheet_name='s1_summary')
        df_sum_prev['DateTime'] = pd.to_datetime(df_sum_prev['last_alarm_time'])

        if pd.isnull(df_sum['DateTime'].max())==True:
            _flag_new = 'SAME: new table empty'
            record_temp('new table empty; table comparison success')
        elif pd.isnull(df_sum_prev['DateTime'].max())==True:
            _flag_new = 'NEW: 2 table not equal'
            record_temp('prev table empty and cur table not; table comparison success')
        elif df_sum['DateTime'].max()<=df_sum_prev['DateTime'].max():
            _flag_new = 'SAME: 2 table equal'
            record_temp('2 table same; table comparison success')
        else:
            _flag_new = 'NEW: 2 table not equal'
            record_temp('2 table difference; table comparison success')
    except:
        df_sum_prev = pd.DataFrame()

        if pd.isnull(df_sum['DateTime'].max())==True:
            _flag_new = 'SAME: new table empty'
            record_temp('new table empty; table comparison failed')
        elif max(df_sum['DateTime'])>three_hour_before:
            _flag_new = 'NEW: within 3 hour'
            record_temp('last alarm within 3 hour and summary table comparison failed')
        else:
            _flag_new = 'SAME: not in 3 hour'
            record_temp('last alarm 3 hours + ago and summary table comparison failed')
else:
    df_sum_prev = pd.DataFrame()

    if pd.isnull(df_sum['DateTime'].max())==True:
        _flag_new = 'SAME: new table empty'
        record_temp('new table empty but no previous record found')
    elif max(df_sum['DateTime'])>three_hour_before:
        _flag_new = 'NEW: within 3 hour'
        record_temp('last alarm within 3 hour but no previous record found')
    else:
        _flag_new = 'SAME: not in 3 hour'
        record_temp('last alarm 3 hours + ago but no previous record found')

ETL_csv_path = os.path.join(storage_path,today_str+today_hour)
df_ETL = pd.read_csv(os.path.join(ETL_csv_path,[i for i in os.listdir(ETL_csv_path) if i.endswith('.csv')][0]))
df_ETL['DateTime'] = pd.to_datetime(df_ETL['dtstamp_hkt'])

# if there are attachments
if (os.path.exists(email_attachment_path)) & (len(os.listdir(email_attachment_path))>0):
    email_attachments = os.listdir(email_attachment_path)
    assert len(email_attachments)>0,print('no file found')
    email_attachments = [os.path.join(email_attachment_path,i) for i in email_attachments]
    # if time = 12 or new alarm :
    if (today_hour=='0700'):
        send_email(email_attachments, today_str[:4]+"-"+today_str[4:6]+"-"+today_str[6:8]+" "+today_hour[:2]+":"+today_hour[2:], email_receipt_path_regular,mode='regular')
    # if new alarm
    elif (_flag_new.startswith('NEW')):
        send_email(email_attachments, today_str[:4]+"-"+today_str[4:6]+"-"+today_str[6:8]+" "+today_hour[:2]+":"+today_hour[2:], email_receipt_path_new,mode='new')
    else:
        print('not send because no new alaram or not 12 nm')
else:
    print('no attachment')
#logger.info("Send email for %s completed" % today_str)
