import ftplib

server="128.151.208.8"
user="sftpadmin"
password="mtrftp@12345"
try:
    ftp = ftplib.FTP(server)
    ftp.login(user,password)
    ftp.cwd('Trial')
except Exception as e:
    print (e)
else:
    with open('temp.txt','rb') as f:
        ftp.storbinary('STOR spirt_temp.txt',f)