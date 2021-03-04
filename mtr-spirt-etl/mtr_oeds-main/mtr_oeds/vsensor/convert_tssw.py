import os
import re
import sys
import time
import random
import numpy as np
import pandas as pd
import configparser
import matplotlib
import matplotlib.pyplot as plt
import datetime as dt
from ..credential import _pw_sftp
from mtr_oeds.common import df_vsensor_1
verbose=True


def convert_isps_to_tssw(lineid,mode,input_location ,output_location='./'):

    '''
    convert isps to tssw (train leave platform time)
    for each train in each station, get the last time with ttnt ==0

    Input:
        input_location: input dir of isps (str)
        mode: ['json','C200013']
            - json = source from Janet in the google drive
        lineid: ['WRL','ISL',...]

    output:
        output excel file '{YYYYMMDD}_{lineid}_TSSW.xlsx'    # for 20201215_WRL_TSSW = contain 20201215 0500 to 2020116 0459
             - time_stamp : YYYY-MM-DD HH:MM:SS
             - lead_cab :
             - route : -1 if unknown
             - station_name : [MEF, TUN ...]
             - platform : [1,2,3]
             - line_id : WRL
             - trail_cab : can be same as leab_cab
             - direction : [UP/DN]

    mode

    '''

    if mode=='json':

        pass


        # df.to_excel("{}_{}_TSSW.xlsx".format(date,lineid),index=False)
    else:

        pass



    return
