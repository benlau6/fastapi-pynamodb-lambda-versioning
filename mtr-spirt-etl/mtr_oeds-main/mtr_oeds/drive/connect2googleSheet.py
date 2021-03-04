from __future__ import print_function
import os
import io
import requests
import pandas as pd

def download_googlesheet(googleSheetUrl,filename='googleSheet.csv',filelocation='./'):
    '''
    download google sheet and save to local computer

    Parameters:
        googleSheetUrl
        filename
        filelocation
    '''
    response = requests.get(googleSheetUrl)
    data = response.content.decode()
    assert response.status_code == 200, "Wrong Status Code"
    df = pd.read_csv(io.StringIO(data))
    df.to_csv(os.path.join(filelocation,filename),index=False)
    return
