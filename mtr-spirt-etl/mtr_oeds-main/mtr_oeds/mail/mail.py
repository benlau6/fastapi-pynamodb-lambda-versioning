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

def send_email(list_attachment_file,
               receipent_excel,
               email_subject = '',
               email_content=''):

    '''
    Send email to Daily Summary recipient


    sample email_content =
        #email_content = "<span style='font-size:12.0pt;font-family:Calibri'>Dear Sirs,<br><br> \
        #Please find attached last 24-hour summary of SPIRT alarms and alarm count distribution histogram updated for {} for your perusal.\
        #<br> <br> \
        #{} <br>\
        #{} <br> <br>\
        #{} <br>\
        #{} <br>\
       #</span>".format(cur_time,content_sent1,content_sent2,content_sent3,content_sent4)
    '''

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)

    if not os.path.exists(receipent_excel):
        logger.error("No email recipient config found. Please check.")
        return

    to_list, cc_list, bcc_list = read_recipient_config(receipent_excel)

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

    mail.Subject = email_subject

    #email_content = "<span style='font-size:12.0pt;font-family:Calibri'>Dear Sirs,<br><br> \
    #Please find attached last 24-hour summary of SPIRT alarms and alarm count distribution histogram updated for {} for your perusal.\
    #<br> <br> \
    #{} <br>\
    #{} <br> <br>\
    #{} <br>\
    #{} <br>\
   #</span>".format(cur_time,content_sent1,content_sent2,content_sent3,content_sent4)


    for attachment_file in list_attachment_file:
        mail.Attachments.Add(attachment_file)
    mail.GetInspector

    index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body'))
    mail.HTMLbody = mail.HTMLbody[:index + 1] + email_content + mail.HTMLbody[index + 1:]

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
