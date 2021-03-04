import pandas as pd
import mtr_oeds
import os
import shutil
import tarfile

df_vsensor_1 = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(mtr_oeds.__file__)),'common','urban_line_chainage_info.csv'))

def is_aws():
    return True if os.environ.get("AWS_DEFAULT_REGION") else False

def gzip_and_delete(source_folder,target_file,remove_after_gzip=True):
    '''
    source_folder:full path
    target_file:full path
    '''
    try:
        #gzip folder
        with tarfile.open(target_file,'w:gz') as tar:
            tar.add(source_folder,arcname=os.path.basename(source_folder))
    except:
        print('gzip {} fail'.format(source_folder))
    else:
        if remove_after_gzip==True:
            #delete
            try:
                shutil.rmtree(source_folder)
            except:
                print('rmtree {} fail'.format(source_folder))
    return
