import sys
import ftplib
import shutil
import os
import time
import re
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

import smtplib
from email.message import EmailMessage


import pandas as pd
from sqlalchemy import create_engine





# with open('password.txt') as file:
#     EmailPassword = file.read()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
LOG_FILENAME = datetime.now().strftime('/var/tmp/ingram_log/PSALE_FTP_Download_logfile.log')
file_handler = RotatingFileHandler(LOG_FILENAME,mode='w')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
file_handler.flush()
logger.addHandler(file_handler)




server = "edi.lightningsource.com"
# user = "blurusord"
# password = "blurus0109"
source = "outgoing"
destination = "/data/archive/ingram/INCOMING"
filenametimeloc="/data/archive/ingram/filenametime.txt"
processed= "/data/archive/ingram/PROCESSED"

# FromEmail='ranjeetkumarpython@gmail.com'
# ToEmail='ranjeetkumarpython@gmail.com'

dbuser='postgres'
dbpassword='J311o5h0ts'
dbserver='slc-bi-postgres03:5432'
database='analytics'


cntry=['US','UK','AU']
users={'US':'blurusord','UK':'blurukord','AU':'blurauord'}
passwords={'US':'blurus0109','UK':'bluruk0120','AU':'blurau0120'}


def email_notification(BODY,SUBJECT,TO_ADDRESS):
    os.system('echo {}| mailx -s "{}" {}'.format(BODY,SUBJECT,TO_ADDRESS))


def latestfile_incoming(destination):
    if len(os.listdir(destination) ) == 0:
        return 1
    else:
        with os.scandir(destination) as dir_content:
            return sorted([(i.name,i.stat().st_mtime) for i in dir_content],key=lambda x:x[1],reverse=True)[0]

def file_down_completed(cntry):
    f_comp='file_download_completed_{}.txt'.format(cntry)
    with open(os.path.join(destination,f_comp),'w+') as f:
        f.write('File Dowanload Completed for {}'.format(cntry))





def file_load_db(destination,dbuser,dbpassword,dbserver,database,processed,cntry):
    f_comp='file_download_completed_{}.txt'.format(cntry)
    f_comp_loc=os.path.join(destination,f_comp)
    if os.path.exists(f_comp_loc) and os.path.getsize(f_comp_loc)>0:
        logger.debug('File is about to get loaded in postgres table')
        files_xls = [i for i in os.listdir(destination) if i[-4:]=='.xls' and i[:2]==cntry]
        list_of_dataframes =[]
        for f in files_xls:
            file_xls_path=os.path.join(destination,f)
            data_xls = pd.read_csv(file_xls_path,encoding ='latin1',sep='\t', lineterminator='\n')
            data_xls.columns = [c.lower().replace('%','per') for c in data_xls.columns]
            data_xls['load_date_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            data_xls['file_name'] = f
            data_xls['country'] = f[:2]
            list_of_dataframes.append(data_xls)
        merged_df = pd.concat(list_of_dataframes)
        cnt=merged_df['country'].count()
        try:
            conn='postgresql://'+dbuser+':'+dbpassword+'@'+dbserver+'/'+database
            #engine = create_engine('postgresql://postgres:ranjeet@localhost:5432/postgres')
            engine = create_engine(conn)
            merged_df.to_sql('psales', engine, schema='dw_ingram',if_exists='append',index=False)
            logger.debug('Data Load Success.{} record Loaded'.format(cnt))
            try:
                logger.debug('Archiving .xls and file_download_completed.txt files')
                for f in files_xls:
                    src=os.path.join(destination,f)
                    dest_fname=f[:-4]+datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+'.xls'
                    dest=os.path.join(processed,dest_fname)
                    shutil.move(src, dest)
                logger.debug('.xls file archived in PROCESSED folder')
                os.remove(os.path.join(destination,f_comp))
                body='Data Load Success for {}'.format(cntry)
                subj='Ingram PSales - Data Load Success for {}'.format(cntry)
                email_notification(body,subj,'rkumar@blurb.com')
            except Exception as ea:
                logger.exception('File Archive failed')
                #email_notification('File Archive failed','File Archive failed','rkumar@blurb.com')

        except Exception as ed:
            logger.exception('Data Load failed')
            body='Data Load failed for {}'.format(cntry)
            subj='Ingram PSales - Data Load failed for {}'.format(cntry)
            email_notification(body,subj,'rkumar@blurb.com')






def file_download(destination, latest_name, cntry):
    local_filename = os.path.join(destination, latest_name)
    lf = open(local_filename, "wb")
    try:
        ftp = ftplib.FTP(server)
        ftp.login(users[c],passwords[c])
        ftp.cwd(source)
        ftp.retrbinary("RETR " + latest_name, lf.write, 8*1024)
        lf.close()
        ftp.quit()
        filesize = os.path.getsize(local_filename)
        logger.debug('{} File Successfully downloaded. File size: {} Bytes'.format(latest_name,filesize))
        file_down_completed(cntry)
        new_fname='{}_{}'.format(cntry,latest_name)
        src=os.path.join(destination,latest_name)
        tgt=os.path.join(destination,new_fname)
        os.rename(src,tgt)
    except Exception as e:
        logger.exception('Could not download {} file'.format(latest_name))
        body='Could not download {} for {}'.format(latest_name,cntry)
        subj='Ingram PSales - Could not download {} for {}'.format(latest_name,cntry)
        email_notification(body,subj,'rkumar@blurb.com')


def ftp_login(server,user,password,source,cntry):

    try:
        ftp = ftplib.FTP(server)
        ftp.login(user, password)
        logger.debug('Successfully logged in to {}'.format(server))
    except Exception as e:
        logger.exception('FTP Server login error:')
        body='FTP Login Failed for {}'.format(cntry)
        subj='Ingram PSales - FTP Login Failed for {}'.format(cntry)
        email_notification(body,subj,'rkumar@blurb.com')
        return

    ftp.cwd(source)
    files = []
    files = ftp.nlst()
    fnames=list(filter(lambda x:"PSALES." in x and '.xls' in x ,files))
    if fnames==[]:
        logger.debug('PSALES .xls File Not Found')
        body='PSALES .xls File Not Found for {}'.format(cntry)
        subj='PSALES .xls File Not Found for {}'.format(cntry)
        #email_notification(body,subj,'rkumar@blurb.com')
        return
    filenametime=sorted([(name,ftp.voidcmd("MDTM " + name)[4:].strip()) for name in fnames],key=lambda x:x[1],reverse=True)[0]
    towrite=filenametime[0]+filenametime[1]+cntry
    latest_name=filenametime[0]
    #x='20200311095511'
    dt=filenametime[1]
    today = datetime.today()
    fdate = datetime.strptime(dt[0:8], '%Y%m%d')
    if (today-fdate).days>=40:
        logger.debug('PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate))
        body='PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate)
        subj='Ingram PSales - PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate)
        email_notification(body,subj,'rkumar@blurb.com')
    elif (today-fdate).days>=30:
        logger.debug('PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate))
        body='PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate)
        subj='Ingram PSales - PSALES file for {} has NOT been arrived since {}'.format(cntry,fdate)
        email_notification(body,subj,'rkumar@blurb.com')
    if os.path.exists(filenametimeloc) and os.path.getsize(filenametimeloc)>0:
        with open(filenametimeloc,'r') as f: s = f.read()
        if s==towrite:
            logger.debug('{} This file is already downloaded.'.format(filenametime))
            #email_notification('This file is already downloaded','This file is already downloaded.','rkumar@blurb.com')
        else:
            logger.debug('{} Most recent file is ready to download.'.format(filenametime[0]))

            with open(filenametimeloc,'w+') as f:
                f.write(towrite)
            #latest_name=file_name_time[0]
            #Download file
            file_download(destination, latest_name, cntry)
        #file_load_db(destination,dbuser,dbpassword,dbserver,database,processed)
    else:
        with open(filenametimeloc,'w+') as f:
            f.write(towrite)
        #Download file
        file_download(destination, latest_name, cntry)
    file_load_db(destination,dbuser,dbpassword,dbserver,database,processed,cntry)
    return latest_name



if __name__=="__main__":
    for c in cntry:
        logger.debug('START :: ##########----{}-----##########'.format(c))
        ftp_login(server,users[c],passwords[c],source,c)
        logger.debug('END :: ##########----{}-----##########'.format(c))
        logger.debug('\n'*2)
    #time.sleep(300)
    #file_load_db(destination,dbuser,dbpassword,dbserver,database,processed)
