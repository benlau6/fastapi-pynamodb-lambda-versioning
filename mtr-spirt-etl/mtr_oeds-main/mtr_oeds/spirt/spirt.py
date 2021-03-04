# module that contains all function to generate spirt report
'''
config files
- days2keep : positive integer
-

def download_spirt_from_server ()

def housekeep(days=7)

def generate_spirt_email ()

'''
import os
from . import spirt
import configparser
import logging
import datetime as dt
import pandas as pd
import numpy as np
import xlsxwriter
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
import matplotlib.dates as mdates
import seaborn as sns
from pandas.plotting import register_matplotlib_converters
import PyPDF2
import ftplib
from shutil import copyfile,rmtree
from mtr_oeds.credential import _pw_sftp
#from mtr_oeds.mail.mail import send_email
#from mtr_oeds.drive.connect2sftp import sftp_placeFiles
import awswrangler as wr

class SpirtReport:
    '''

    '''

    def __init__(self):

        self.module_path = os.path.dirname(os.path.abspath(spirt.__file__))
        self.execute_path=os.path.abspath('') #

        #config
        try:
            self.config=configparser.ConfigParser()
            self.config.read(os.path.join(self.execute_path,'spirt_config.cfg'))
            self.BASE_PATH = self.config['path']['base_path']
            if self.execute_path!=self.BASE_PATH: print('WARNING: execute path not equal base path')
            print('read config file {}'.format(os.path.join(self.execute_path,'spirt_config.cfg')))
        except Exception as e:
            print('read config fail')
            print (e)
            exit()

        #time
        self.target_time = dt.datetime.strptime(self.config['time'].get('target_time',dt.datetime.now().strftime("%Y%m%dT%H00")),'%Y%m%dT%H%M')
        self.start_time = dt.datetime.now()

        #uniuqe folder name
        self.foldername = self.target_time.strftime("%Y%m%dT%H00")+"_"+self.start_time.strftime("%H%M%S")

        self.interim_dir='interim'
        self.output_dir='output'
        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['input_path']))
        self.INPUT_PATH = os.path.join(self.BASE_PATH,self.config['path']['input_path'])
        self.REMARKS_PATH = os.path.join(self.INPUT_PATH,'Remarks.csv')

        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['log_path']))
        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername))
        self.SPECIFIC_PATH = os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername)

        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.config['housekeep']['previousTableDir']))
        self.PREVIOUS_PATH=os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.config['housekeep']['previousTableDir'])

        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername,self.interim_dir))
        self.INTERIM_PATH = os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername,self.interim_dir)
        self.create_dir(path=os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername,self.output_dir))
        self.OUTPUT_PATH = os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername,self.output_dir)

        self.pageCount = 1 #count number of pdf

        #logger
        self.logger = None
        self.superlogger = None
        self.logger = logging.getLogger('spirt_log')
        self.superlogger = logging.getLogger('spirt_log_summary')
        self.logger.setLevel(logging.INFO)
        self.superlogger.setLevel(logging.INFO)
        self.fh = logging.FileHandler(os.path.join(self.BASE_PATH,self.config['path']['log_path'],self.foldername,'logfile.log'))
        self.superfh = logging.FileHandler(os.path.join(self.BASE_PATH,self.config['path']['log_path'],'spirt_summary.log'))
        self.SUPERLOGGER_PATH = os.path.join(self.BASE_PATH,self.config['path']['log_path'],'spirt_summary.log')
        self.fmt = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        self.fh.setFormatter(self.fmt)
        self.superfh.setFormatter(self.fmt)
        self.logger.addHandler(self.fh)
        self.superlogger.addHandler(self.superfh)
        self.logger.info('object initialization completed')
        self.superlogger.info('{} - function start, filename'.format(self.foldername))

    def create_dir(self,path):
        print(path)
        try:
            os.mkdir(path)
        except:
            pass
        return

    def validate_config(self):
        for attr,value in self.__dict__.items():
            print('{} : {}'.format(attr,value))
        return

    def spirt_downloadCSV(self):
        '''
        download every filename within 1 months from sftp to local and Remarks.csv
        '''

        try:
            ftp = ftplib.FTP(_pw_sftp['address'])
            ftp.login(_pw_sftp['username'],_pw_sftp['password'])
            ftp.cwd('C190021-IRV')
        except Exception as e:
            print (e)
        else:
            #get a list of file already there
            ftp_available_files = [] # store file name in nice format
            ftp_available_files = ftp.nlst()

            #upload file
            for eachFile in [i for i in ftp_available_files if ((i[:8]>=((self.target_time-dt.timedelta(days=14)).strftime("%Y%m%d"))) and i.endswith('.csv')) or (i.startswith('Remarks'))]:
                #check if file in FTP
                self.logger.info('donwload {} to {}'.format(eachFile,self.INPUT_PATH))
                with open(os.path.join(self.INPUT_PATH,eachFile),'wb') as f:
                    ftp.retrbinary('RETR '+eachFile , f.write)
        ftp.quit()

        return

    def spirt_combineCSV(self):
        self.logger.info('spirt_comebineCSV start')

        _today_date = self.target_time.strftime("%Y%m%d")
        _yst_date = (self.target_time - dt.timedelta(days=1)).strftime("%Y%m%d")

        #find the input csv files that are related
        _filenames = sorted([i for i in os.listdir(self.INPUT_PATH) if (i[:8] in [_today_date,_yst_date]) and (i.endswith('.csv')) ],reverse=True)
        self.logger.info('{} of csv in the {} between {} - {}'.format(len(_filenames),self.INPUT_PATH,_yst_date,_today_date ))
        for eachFile in _filenames:
            self.logger.info("File {} included".format(eachFile))

        assert len(_filenames)>0, self.logger.warning('no input csv found')

        #concat all the csv
        df = pd.concat([pd.read_csv(os.path.join(self.INPUT_PATH,i)) for i in _filenames])
        df = df.drop_duplicates(subset = ['linename','subtrackname','km','vehicle','dtstamp_hkt'],keep='last')
        df.reset_index(drop=True,inplace=True)
        df['datetime'] = pd.to_datetime(df['dtstamp_hkt'],format='%Y-%m-%d %H:%M:%S')
        self.logger.info('combined input file ranged from {}-{}'.format(df['datetime'].min(),df['datetime'].max()))

        #check whether there are extreme old or future data
        _dflen = len(df)
        df = df[df['datetime']>self.target_time - dt.timedelta(days=1)].copy()
        df = df[df['datetime']<=self.target_time + dt.timedelta(hours=1)].copy()
        if len(df)!=_dflen: self.logger.warning('some data not related to the time range')
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True,inplace=True)

        if len(df)==0:
            self.logger.warning('combined input file is length 0')
            exit()
        self.df = df
        self.logger.info('spirt_comebineCSV finish')
        return

    def spirt_downloadS3CSV(self):
        self.logger.info('spirt_downloadS3CSV start')
        '''
        download every filename within 1 months from sftp to local and Remarks.csv
        '''
        files = wr.s3.list_objects('s3://mtr-hk-dev-raw-data-bucket/OEDS/Spirt/*')
        print(files)
        print('------')
        print([i for i in files if ((i.rsplit('/', 1)[-1][:8]>=((self.target_time-dt.timedelta(days=14)).strftime("%Y%m%d"))) and i.endswith('.csv')) or (i.rsplit('/', 1)[-1].startswith('Remarks'))])
        #upload file
        for eachFile in [i for i in files if ((i.rsplit('/', 1)[-1][:8]>=((self.target_time-dt.timedelta(days=14)).strftime("%Y%m%d"))) and i.endswith('.csv')) or (i.rsplit('/', 1)[-1].startswith('Remarks'))]:
            #check if file in FTP
            print('donwload {} to {}'.format(eachFile,self.INPUT_PATH))
            self.logger.info('donwload {} to {}'.format(eachFile,self.INPUT_PATH))
            with open(os.path.join(self.INPUT_PATH,eachFile.rsplit('/', 1)[-1]),'wb') as f:
                wr.s3.download(path=eachFile, local_file=f)

        return

    def calSeverity(self,df,input_col,output_col,s1,s2,s3):
        conditions = [
            ((df[input_col] >= s3) & (df[input_col] < s2)),
            ((df[input_col] >= s2) & (df[input_col] < s1)),
            (df[input_col] >= s1)]
        choices = [3,2,1]
        df[output_col] = np.select(conditions, choices, default=4)
        return df

    def spirt_genResult(self):

        self.logger.info('spirt_genResult start')

        s1EAL_acc=float(self.config['threshold']['s1EAL_acc'])
        s2EAL_acc=float(self.config['threshold']['s2EAL_acc'])
        s3EAL_acc=float(self.config['threshold']['s3EAL_acc'])

        s1EAL_ride_jerk_all=float(self.config['threshold']['s1EAL_ride_jerk_all'])
        s2EAL_ride_jerk_all=float(self.config['threshold']['s2EAL_ride_jerk_all'])
        s3EAL_ride_jerk_all=float(self.config['threshold']['s3EAL_ride_jerk_all'])

        s1EAL_bolaccy=float(self.config['threshold']['s1EAL_bolaccy'])
        s2EAL_bolaccy=float(self.config['threshold']['s2EAL_bolaccy'])
        s3EAL_bolaccy=float(self.config['threshold']['s3EAL_bolaccy'])

        s1EAL_trackgauge=float(self.config['threshold']['s1EAL_trackgauge'])
        s2EAL_trackgauge=float(self.config['threshold']['s2EAL_trackgauge'])
        s3EAL_trackgauge=float(self.config['threshold']['s3EAL_trackgauge'])

        s1TML_acc=float(self.config['threshold']['s1TML_acc'])
        s2TML_acc=float(self.config['threshold']['s2TML_acc'])
        s3TML_acc=float(self.config['threshold']['s3TML_acc'])

        s1TML_ride_jerk_all=float(self.config['threshold']['s1TML_ride_jerk_all'])
        s2TML_ride_jerk_all=float(self.config['threshold']['s2TML_ride_jerk_all'])
        s3TML_ride_jerk_all=float(self.config['threshold']['s3TML_ride_jerk_all'])

        s1TML_bolaccy=float(self.config['threshold']['s1TML_bolaccy'])
        s2TML_bolaccy=float(self.config['threshold']['s2TML_bolaccy'])
        s3TML_bolaccy=float(self.config['threshold']['s3TML_bolaccy'])


        self.df['ride_jerk_all'] = self.df[['bod1_lat_jerk','bod1_lon_jerk','bod2_lat_jerk','bod2_lon_jerk']].values.max(1)

        df_TML = self.df[['linename','subtrackname','km','vehicle','dtstamp_hkt','speed','acc','ride_jerk_all','bolaccy']].copy()
        df_EAL = self.df[['linename','subtrackname','km','vehicle','dtstamp_hkt','speed','acc','ride_jerk_all','bolaccy','gauge']].copy()
        df_TML = df_TML[df_TML['linename'].isin(['TML'])]
        df_EAL = df_EAL[df_EAL['linename'].isin(['EAL','LMC'])]

        #convert to severity
        df_EAL =self.calSeverity(df_EAL,'acc','severity_acc', s1EAL_acc, s2EAL_acc, s3EAL_acc)
        df_EAL =self.calSeverity(df_EAL,'ride_jerk_all','severity_ride_jerk_all', s1EAL_ride_jerk_all, s2EAL_ride_jerk_all, s3EAL_ride_jerk_all)
        df_EAL =self.calSeverity(df_EAL,'bolaccy','severity_bolaccy', s1EAL_bolaccy, s2EAL_bolaccy, s3EAL_bolaccy)
        df_EAL =self.calSeverity(df_EAL,'gauge','severity_gauge', s1EAL_trackgauge, s2EAL_trackgauge, s3EAL_trackgauge)
        df_EAL['severity_overall'] = df_EAL[['severity_acc','severity_ride_jerk_all','severity_bolaccy','severity_gauge']].values.min(1)

        df_TML =self.calSeverity(df_TML,'acc','severity_acc', s1TML_acc, s2TML_acc, s3TML_acc)
        df_TML =self.calSeverity(df_TML,'ride_jerk_all','severity_ride_jerk_all', s1TML_ride_jerk_all, s2TML_ride_jerk_all, s3TML_ride_jerk_all)
        df_TML =self.calSeverity(df_TML,'bolaccy','severity_bolaccy', s1TML_bolaccy, s2TML_bolaccy, s3TML_bolaccy)
        df_TML['severity_overall'] = df_TML[['severity_acc','severity_ride_jerk_all','severity_bolaccy']].values.min(1)

        df_interim = pd.concat([df_EAL, df_TML], ignore_index=True, sort =False)
        df_interim['dtstamp_hkt'] = df_interim['dtstamp_hkt'].astype('datetime64[ns]')
        self.df_interim = df_interim
        self.df_interim.to_csv(os.path.join(self.INTERIM_PATH,'{}_ETLdata.csv'.format(self.target_time.strftime("%Y%m%d%H00"))),index=False)


        self.logger.info('spirt_genResult finish')
        return

    def spirt_genSummary(self):

        self.logger.info('spirt_genSummary start')

        df_sum = self.df_interim.copy()

        df_sum = df_sum[df_sum['severity_overall']==1]

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

        df_remarks = pd.read_csv(self.REMARKS_PATH)
        i, j = np.where((df_sum['km'].values[:, None]>=df_remarks['From_km'].values)
                & (df_sum['km'].values[:, None]<=df_remarks['To_km'].values)
               & (df_sum['subtrackname'].values[:,None]==df_remarks['Trackname'].values))

        df_result = pd.DataFrame(np.column_stack([df_sum.values[i], df_remarks.values[j]]),
                                 columns=df_sum.columns.append(df_remarks.columns)
                                 ).append(
                                df_sum[~np.in1d(np.arange(len(df_sum)), np.unique(i))],
                                ignore_index=True, sort=False).sort_values(['line','s1_trips_count'],ascending=[True,False])

        #in case no col remarks
        if 'Remarks' not in df_result.columns:
            df_result['Remarks'] = None
        df_result.drop_duplicates(subset=['line','subtrackname','km','vertical_acc_max', 'ride_jerk_all_max','bostler_lateral_acc_max','track_gauge_max','s1_trips_count','serverity_overall','last_alarm_time'],inplace=True,keep='last')
        df_sum = df_result.loc[:,['line','subtrackname','km','vertical_acc_max', 'ride_jerk_all_max','bostler_lateral_acc_max','track_gauge_max','s1_trips_count','serverity_overall','last_alarm_time','Remarks']].reset_index(drop=True)
        self.df_summary = df_sum

        self.logger.info('spirt_genSummary finish')

        #report_filename = today_date+today_hour +'_SPIRT_alarms.xlsx'
        return

    def spirt_genExcel(self):
        self.logger.info('spirt_genExcel start')
        df = self.df_interim.copy()

        s1EAL_acc=float(self.config['threshold']['s1EAL_acc'])
        s2EAL_acc=float(self.config['threshold']['s2EAL_acc'])
        s3EAL_acc=float(self.config['threshold']['s3EAL_acc'])

        s1EAL_ride_jerk_all=float(self.config['threshold']['s1EAL_ride_jerk_all'])
        s2EAL_ride_jerk_all=float(self.config['threshold']['s2EAL_ride_jerk_all'])
        s3EAL_ride_jerk_all=float(self.config['threshold']['s3EAL_ride_jerk_all'])

        s1EAL_bolaccy=float(self.config['threshold']['s1EAL_bolaccy'])
        s2EAL_bolaccy=float(self.config['threshold']['s2EAL_bolaccy'])
        s3EAL_bolaccy=float(self.config['threshold']['s3EAL_bolaccy'])

        s1EAL_trackgauge=float(self.config['threshold']['s1EAL_trackgauge'])
        s2EAL_trackgauge=float(self.config['threshold']['s2EAL_trackgauge'])
        s3EAL_trackgauge=float(self.config['threshold']['s3EAL_trackgauge'])

        s1TML_acc=float(self.config['threshold']['s1TML_acc'])
        s2TML_acc=float(self.config['threshold']['s2TML_acc'])
        s3TML_acc=float(self.config['threshold']['s3TML_acc'])

        s1TML_ride_jerk_all=float(self.config['threshold']['s1TML_ride_jerk_all'])
        s2TML_ride_jerk_all=float(self.config['threshold']['s2TML_ride_jerk_all'])
        s3TML_ride_jerk_all=float(self.config['threshold']['s3TML_ride_jerk_all'])

        s1TML_bolaccy=float(self.config['threshold']['s1TML_bolaccy'])
        s2TML_bolaccy=float(self.config['threshold']['s2TML_bolaccy'])
        s3TML_bolaccy=float(self.config['threshold']['s3TML_bolaccy'])


        df=df.drop(columns=['severity_acc','severity_ride_jerk_all','severity_bolaccy','severity_gauge']).copy()
        df = df[df.severity_overall<4]
        df.sort_values(by=['severity_overall','km'], ascending=True, inplace=True)
        d = {1 :'S1', 2:'S2', 3:'S3'}
        df['severity_overall']=df['severity_overall'].map(d)
        df.columns = ['line','subtrackname','km','vehicle','dtstamp_hkt','speed','vertical_acc', 'ride_jerk_all','bostler_lateral_acc','gauge','serverity_overall']
        df_TML = df[df['line'].isin(['TML'])]
        df_EAL = df[df['line'].isin(['EAL','LMC'])]

        writer = pd.ExcelWriter(os.path.join(self.OUTPUT_PATH,'{}_SPIRT_alarms.xlsx'.format(self.target_time.strftime("%Y%m%d%H00"))), engine='xlsxwriter')
        df_sum = self.df_summary.copy()
        df_sum.to_excel(writer, sheet_name='s1_summary', index=False)
        df_EAL.to_excel(writer, sheet_name='EAL_details', index=False)
        df_TML.to_excel(writer, sheet_name='TML_details', index=False)

        workbook  = writer.book
        worksheet1 = writer.sheets['s1_summary']
        worksheet2 = writer.sheets['EAL_details']
        worksheet3 = writer.sheets['TML_details']

        format1 = workbook.add_format({'bg_color': '#FFC7CE',
                               'font_color': '#9C0006'})

        # S2
        format2 = workbook.add_format({'bg_color':   '#F5C47B',
                                       'font_color': '#974706'})

        # S3
        format3 = workbook.add_format({'bg_color':   '#F7F293',
                                       'font_color': '#000000'})





        # Conditional formating summary sheet
        EAL_count = len(df_sum[df_sum['line'].isin(['EAL','LMC'])])
        TML_count = len(df_sum[df_sum['line'].isin(['TML'])])

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

        workbook.close()
        self.logger.info('spirt_genExcel finish')
        return

    def spirt_plot_graph(self,df,linename_list,xaxis_min,x_axis_max,title,filename,df_remarks,remarks_fs = None):
        '''
        df_bottom = remarks table from googleDrive
        '''
        self.logger.info('spirt_plot_graph {} start'.format(filename))
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
                self.logger.info('fontsize {}'.format(remarks_fs))
                for (row, col), cell in table.get_celld().items():
                    if (col == 3) and  (row!=0) :
                        cell.set_fontsize(remarks_fs)
                    else:
                        cell.set_fontsize(9)

            table.scale(1,1.2)
            axs[0].axis('off')
            axs[1].legend(loc=1)


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

        return table

    def spirt_plot_trend(self,df_total,chainage,linename,subtrackname,xaxis_max,xaxis_min,current_hour,threshold):


        title = linename+ "_"+ subtrackname + "_" + str(chainage)
        self.logger.info('spirt_plot_trend {} start'.format(title))
        filename=os.path.join(self.INTERIM_PATH,str(self.pageCount).zfill(3)+"-"+title+'.pdf')
        self.pageCount+=1
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
        axs[0].set_xlim([dt.datetime(year=int(xaxis_min[:4]),month=int(xaxis_min[4:6]),day=int(xaxis_min[6:8]),hour=int(current_hour[:2])),
                        dt.datetime(year=int(xaxis_max[:4]),month=int(xaxis_max[4:6]),day=int(xaxis_max[6:8]),hour=int(current_hour[:2]))])
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

    def spirt_merge_pdf(self,path_from,path_to,userfilename):
        pdf2merge = []
        pdfFileObj_list = []
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
            pdfFileObj_list.append(pdfFileObj)

        #save PDF to file, wb for write binary
        pdfOutput = open(os.path.join(path_to,userfilename), 'wb')
        #Outputting the PDF
        pdfWriter.write(pdfOutput)
        #Closing the PDF writer
        pdfOutput.close()

        for eachItem in pdfFileObj_list:
            eachItem.close()
        return

    def spirt_uploadSuperLog(self):
        try:
            ftp = ftplib.FTP(_pw_sftp['address'])
            ftp.login(_pw_sftp['username'],_pw_sftp['password'])
            ftp.cwd('E200005-SPIRT_Archive')
        except Exception as e:
            self.logger.warning('spirt_uploadSuperLog {} fail'.format(folder_path))
            self.superlogger.warning('spirt_uploadSuperLog {} fail'.format(folder_path))
            print (e)
        else:
            ftp.storbinary('STOR ' + 'spirt_summary.log', open(self.SUPERLOGGER_PATH,'rb'))
            ftp.quit()
            return

    def spirt_findPrevious(self):
        #read the previous file
        try:
            _targetFile_summary = sorted([i for i in os.listdir(self.PREVIOUS_PATH) if i.endswith('SPIRT_alarms.xlsx')])[-1]
            df_prevsiouSummary = pd.read_excel(os.path.join(os.path.join(self.PREVIOUS_PATH,_targetFile_summary)),sheet_name='s1_summary')
            df_prevsiouSummary['last_alarm_time'] = pd.to_datetime(df_prevsiouSummary['last_alarm_time'])
            self.logger.info('previous summary : {}'.format(_targetFile_summary))
            self.df_prevsiouSummary = df_prevsiouSummary
        except:
            self.df_prevsiouSummary=''

        try:
            _targetFile_ETL = sorted([i for i in os.listdir(self.PREVIOUS_PATH) if i.endswith('ETLdata.csv')])[-1]
            df_prevsiouETL = pd.read_csv(os.path.join(os.path.join(self.PREVIOUS_PATH,_targetFile_ETL)))
            df_prevsiouETL['dtstamp_hkt'] = pd.to_datetime(df_prevsiouETL['dtstamp_hkt'])
            self.logger.info('previous ETL : {}'.format(_targetFile_ETL))
            self.df_prevsiouETL = df_prevsiouETL
        except:
            self.df_prevsiouETL = ''
        return

    def spirt_updatePrevious(self):
        #delete everything inside
        try:
            for eachFile in os.listdir(self.PREVIOUS_PATH):
                os.remove(os.path.join(self.PREVIOUS_PATH,eachFile))
        except:
            pass
        #copy current to path
        _targetFile_summary = sorted([i for i in os.listdir(self.OUTPUT_PATH) if i.endswith('SPIRT_alarms.xlsx')])[-1]
        _targetFile_ETL = sorted([i for i in os.listdir(self.INTERIM_PATH) if i.endswith('ETLdata.csv')])[-1]
        copyfile(os.path.join(self.OUTPUT_PATH,_targetFile_summary),os.path.join(self.PREVIOUS_PATH,_targetFile_summary))
        copyfile(os.path.join(self.INTERIM_PATH,_targetFile_ETL),os.path.join(self.PREVIOUS_PATH,_targetFile_ETL))
        return

    def spirt_sendDecision(self):
        '''
        self.emailMode = {new,regular}
        self.emailSub=''
        self.emailDecision=True/False (True = send)
        self.emailReason='reason_of_decision'
        self.emailContent=''
        '''
        if self.target_time.hour==7:
            self.emailDecision=True
            self.emailReason='target time is 7 am, send email'
            self.emailMode = 'regular'
            self.emailSub = 'SPIRT daily alarm summary {}'.format(self.target_time.strftime("%Y%m%d %H%M"))
            self.emailContent = ''
        else:
            if len(self.df_summary)==0:
                self.emailDecision=False
                self.emailReason='current summary page empty, dont send email'
                self.emailMode = ''
                self.emailSub = ''
                self.emailContent = ''
            else:
                try:
                    self.spirt_findPrevious()
                except:
                    # current not 0 and cannot read df_previsouSummary
                    self.emailDecision=True
                    self.emailReason='current summary page not empty, cannot locate previous, send email'
                    self.emailMode='new'
                    self.emailSub='SPIRT new s1 alarm {}'.format(self.target_time.strftime("%Y%m%d %H%M"))
                    self.emailContent = ''
                else:
                    if isinstance(self.df_prevsiouSummary,str):
                        self.emailDecision=True
                        self.emailReason='current summary page not empty,previous found by empty string, send email'
                        self.emailMode='new'
                        self.emailSub='SPIRT new s1 alarm {}'.format(self.target_time.strftime("%Y%m%d %H%M"))
                        self.emailContent = ''
                    #current not 0 and can read df_previsouSummary
                    else:
                        try:
                            _left = self.df_prevsiouSummary.groupby('line')['last_alarm_time'].max().reset_index()
                            _right = self.df_summary.groupby('line')['last_alarm_time'].max().reset_index()
                            _left.rename(columns = {'last_alarm_time':"last_alarm_time_previous"},inplace=True)
                            _left['last_alarm_time_previous'].fillna(dt.datetime(year=1960,month=1,day=1),inplace=True)
                            _right.rename(columns = {'last_alarm_time':"last_alarm_time_current"},inplace=True)
                            _temp = pd.merge(_left,_right,how='right',on='line')
                            self.mail_df = _temp
                            self.logger.info(self.mail_df.to_string())
                            if (self.mail_df['last_alarm_time_current']>(self.mail_df['last_alarm_time_previous']+dt.timedelta(seconds=1))).any():
                                self.emailDecision=True
                                self.emailReason='current summary page not empty,previous found, concat compare, send email'
                                self.emailMode='new'
                                self.emailSub='SPIRT new s1 alarm {}'.format(self.target_time.strftime("%Y%m%d %H%M"))
                                self.emailContent = ''
                            else:
                                self.emailDecision=False
                                self.emailReason='current summary page not empty,previous found, concat compare, dont send email'
                                self.emailMode=''
                                self.emailSub=''
                                self.emailContent = ''
                        except:
                            self.emailDecision=True
                            self.emailReason='current summary page not empty,previous found, concat fail, send email'
                            self.emailMode='new'
                            self.emailSub='SPIRT new s1 alarm {}'.format(self.target_time.strftime("%Y%m%d %H%M"))
                            self.emailContent = ''

        self.logger.info('{} : {}'.format('emailDecision',self.emailDecision))
        self.logger.info('{} : {}'.format('emailReason',self.emailReason))
        self.logger.info('{} : {}'.format('emailMode',self.emailMode))
        self.logger.info('{} : {}'.format('emailSub',self.emailSub))
        self.logger.info('{} : {}'.format('emailContent',self.emailContent))
        self.superlogger.info('{} - {} : {}'.format(self.foldername,'emailDecision',self.emailDecision))
        self.superlogger.info('{} - {} : {}'.format(self.foldername,'emailReason',self.emailReason))
        self.superlogger.info('{} - {} : {}'.format(self.foldername,'emailMode',self.emailMode))
        self.superlogger.info('{} - {} : {}'.format(self.foldername,'emailSub',self.emailSub))
        self.superlogger.info('{} - {} : {}'.format(self.foldername,'emailContent',self.emailContent))
        return

    def spirt_draftEmail(self):

        df_ETL = self.df_interim.copy()
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


        self.emailContent = "<span style='font-size:12.0pt;font-family:Calibri'>Dear Sirs,<br><br> \
        Please find attached last 24-hour summary of SPIRT alarms and alarm count distribution histogram updated for {} for your perusal.\
        <br> <br> \
        {} <br>\
        {} <br> <br>\
        {} <br>\
        {} <br>\
       </span>".format(self.target_time.strftime("%Y%m%d %H%M"),content_sent1,content_sent2,content_sent3,content_sent4)

        return

    def spirt_uploadDir(self,folder_path):
        '''
        upload all the thing inside the folder_path
        '''
        self.logger.info('uploadDir {} start'.format(folder_path))
        try:
            ftp = ftplib.FTP(_pw_sftp['address'])
            ftp.login(_pw_sftp['username'],_pw_sftp['password'])
            ftp.cwd('E200005-SPIRT_Archive')
            if os.path.basename(folder_path) not in ftp.nlst():
                ftp.mkd(os.path.basename(folder_path))
            ftp.cwd(os.path.basename(folder_path))
        except Exception as e:
            self.logger.warning('uploadDir {} fail'.format(folder_path))
            self.superlogger.warning('uploadDir {} fail'.format(folder_path))
            print (e)
        else:
            try:
                ftp.quit()
                #sftp_placeFiles(ftp,folder_path)
            except:
                self.logger.info('uploadDir fail {}'.format(folder_path))
                self.superlogger.info('{} - uploadDir fail {}'.format(self.foldername,folder_path))
            else:
                self.logger.info('uploadDir {} finish'.format(folder_path))
                self.fh.close()
                rmtree(self.SPECIFIC_PATH)
                ftp.quit()

        return

    def spirt_cleanInput(self):
        for eachFile in [i for i in os.listdir(self.INPUT_PATH) if (i[:8]>"20190101") and (i[:8]<(self.start_time-dt.timedelta(days=90)).strftime('%Y%m%d')) and (i.endswith('csv'))]:
            self.logger.info('remove {} from INPUT'.format(eachFile))
            os.remove(os.path.join(self.INPUT_PATH,eachFile))

    def spirt_end2endProcess(self):


        #download_spirt_from_server()
        self.spirt_downloadS3CSV()

        #combine relavent csv files
        self.spirt_combineCSV()
        self.spirt_genResult()

        #calculate summary page
        self.spirt_genSummary()

        #generate excel file for output
        self.spirt_genExcel()

        #generate pdf plot page I
        df_sum = self.df_summary.copy()
        table1 = self.spirt_plot_graph(self.df_interim,['EAL','LMC'],float(self.config['chainage'].get('EAL_start',"-0.2")),float(self.config['chainage'].get('EAL_end',"37.5")),'EAL : count of S1 alarm in the last 24-hour',
                                 os.path.join(self.INTERIM_PATH,'0001-Historgram_EAL.pdf'),
                                 df_sum[(df_sum['line'].isin(['EAL','LMC']))&(pd.isnull(df_sum['Remarks'])==False)])
        if table1!=None:
            table1 = self.spirt_plot_graph(self.df_interim,['EAL','LMC'],float(self.config['chainage'].get('EAL_start',"-0.2")),float(self.config['chainage'].get('EAL_end',"37.5")),'EAL : count of S1 alarm in the last 24-hour',
                                     os.path.join(self.INTERIM_PATH,'0001-Historgram_EAL.pdf'),
                                     df_sum[(df_sum['line'].isin(['EAL','LMC']))&(pd.isnull(df_sum['Remarks'])==False)],
                                     remarks_fs = table1.get_celld()[1,0].get_fontsize())
        table2 = self.spirt_plot_graph(self.df_interim,['TML'],float(self.config['chainage'].get('TML_start',"81")),float(self.config['chainage'].get('TML_end',"99")),'TML : count of S1 alarm in the last 24-hour',
                                 os.path.join(self.INTERIM_PATH,'0002-Historgram_TML.pdf'),
                                 df_sum[(df_sum['line'].isin(['TML']))&(pd.isnull(df_sum['Remarks'])==False)])
        if table2!=None:
            table2 = self.spirt_plot_graph(self.df_interim,['TML'],float(self.config['chainage'].get('TML_start',"81")),float(self.config['chainage'].get('TML_end',"99")),'TML : count of S1 alarm in the last 24-hour',
                                     os.path.join(self.INTERIM_PATH,'0002-Historgram_TML.pdf'),
                                     df_sum[(df_sum['line'].isin(['TML']))&(pd.isnull(df_sum['Remarks'])==False)],
                                     remarks_fs = table2.get_celld()[1,0].get_fontsize())



        #generate pdf plot page II
        today_date = (self.target_time).strftime("%Y%m%d")
        last_7days_date = (self.target_time -dt.timedelta(days=7)).strftime("%Y%m%d")
        today_hour = (self.target_time).strftime("%H00")
        df_rolling = [pd.read_csv(os.path.join(self.INPUT_PATH,i)) for i in os.listdir(self.INPUT_PATH) if (i[:8]>=last_7days_date) & (i[:8]<=today_date) & (i.endswith('.csv'))]
        df_rolling = pd.concat(df_rolling)

        if len(df_sum)>0:
            try:
                for x,y in df_sum.iterrows():
                    if y['line'] in ['EAL','LMC']:
                        self.spirt_plot_trend(df_rolling,y['km'],y['line'],y['subtrackname'],today_date,last_7days_date,today_hour,[float(self.config['threshold']['s1EAL_acc']),
                                                                                                                             float(self.config['threshold']['s1EAL_bolaccy']),
                                                                                                                             float(self.config['threshold']['s1EAL_ride_jerk_all']),
                                                                                                                             float(self.config['threshold']['s1EAL_trackgauge'])])
                    elif y['line']=='TML':
                        self.spirt_plot_trend(df_rolling,y['km'],y['line'],y['subtrackname'],today_date,last_7days_date,today_hour,[float(self.config['threshold']['s1TML_acc']),
                                                                                                                             float(self.config['threshold']['s1TML_bolaccy']),
                                                                                                                             float(self.config['threshold']['s1TML_ride_jerk_all'])])

            except:
                pass

        try:
            self.spirt_merge_pdf(self.INTERIM_PATH,self.OUTPUT_PATH,'{}_SPIRT_graphs.pdf'.format(self.target_time.strftime("%Y%m%dT%H00")))
            copyfile(self.REMARKS_PATH,os.path.join(self.INTERIM_PATH,'Remarks.csv'))
        except:
            pass
        self.superlogger.info('{} - attachment generated finish'.format(self.foldername))


        #send email
        self.spirt_sendDecision()
        if self.emailDecision==True:
            self.spirt_draftEmail()
            #send_email()

        self.spirt_updatePrevious()

        #housekeep()
        #remove csv older than 90 days
        self.spirt_cleanInput()

        #upload the current folder to server and delete
        self.superlogger.info('{} - uploadDir start'.format(self.foldername))
        #self.spirt_uploadDir(self.SPECIFIC_PATH)
        self.superlogger.info('{} - finish upload'.format(self.foldername))

        #upload super logger to cloud
        self.spirt_uploadSuperLog()
        return
