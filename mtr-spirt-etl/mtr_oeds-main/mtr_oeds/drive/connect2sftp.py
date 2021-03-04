from __future__ import print_function
import os
import io
import ftplib
from mtr_oeds.credential import _pw_sftp


# a function to upload local file to sftp
def upload_to_ftp(files,ftp_dir,delete=False):

    return

def create_ftp_to_datalake():
    ftp = ftplib.FTP(_pw_sftp['address'])
    ftp.login(_pw_sftp['username'],_pw_sftp['password'])

    return ftp

def download_sftp2local(ftp,filepath,filename,localpath='./'):
    '''ftp'''
    ftp.cwd(filepath)
    ftp.retrbinary("RETR "+filename,open(os.path.join(localpath,filename),'wb').write)

    return ftp

def sftp_placeFiles(ftp, path):
    for name in os.listdir(path):
        localpath = os.path.join(path, name)
        if os.path.isfile(localpath):
            print("STOR", name, localpath)
            ftp.storbinary('STOR ' + name, open(localpath,'rb'))
        elif os.path.isdir(localpath):
            print("MKD", name)

            try:
                ftp.mkd(name)

            # ignore "directory already exists"
            except Exception as e:
                if not e.args[0].startswith('550'):
                    raise

            print("CWD", name)
            ftp.cwd(name)
            sftp_placeFiles(ftp, localpath)
            print("CWD", "..")
            ftp.cwd("..")
