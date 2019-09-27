# Import smtplib for the actual sending function
import smtplib, ssl
#import os
#from pathlib import Path
# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

import traceback

from utils import common as cm
from utils import ConfigData
from utils import global_const as gc
from utils import logging

'''
import yagmail

def send_yagmail(emails_to, subject, message, email_from = None, attachment_path =  None, smtp_server = None):
    root_dir = cm.get_project_root()
    cnf_path = str(root_dir.joinpath(gc.MAIN_CONFIG_FILE))
    m_cfg = ConfigData(cnf_path)
    if not email_from:
        email_from = m_cfg.get_value('Email/default_from_email')
    if not smtp_server:
        smtp_server = m_cfg.get_value('Email/smtp_server')
    
    receiver = emails_to  # 'stasrirak.ms@gmail.com, stasrira@yahoo.com, stas.rirak@mssm.edu'
    body = "Hello there from Yagmail"
    filename = attachment_path  # 'test.png'
    
    yag = yagmail.SMTP(smtp_server)
    yag.send(
        to=receiver,
        subject="Yagmail test with attachment",
        contents=body, 
        attachments=filename,
    )
'''

def send_email (emails_to, subject, message, email_from = None, attachment_path =  None, smtp_server = None):
    # ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    # common_logger_name = gc.MAIN_LOG_NAME
    #mlog= logging.getLogger(gc.MAIN_LOG_NAME)

    # ROOT_DIR = Path(__file__).parent.parent
    root_dir = cm.get_project_root()
    cnf_path = str(root_dir.joinpath(gc.MAIN_CONFIG_FILE))
    # print (cnf_path)
    # replace empty arguments with default values
    m_cfg = ConfigData(cnf_path)
    if not email_from:
        email_from = m_cfg.get_value('Email/default_from_email')
    if not smtp_server:
        smtp_server = m_cfg.get_value('Email/smtp_server')

    # Create the container (outer) email message.
    msg = MIMEMultipart()
    msg['Subject'] = subject  # 'Test with many email addresses'
    emails_to_list = emails_to.split(',')  # ['stasrirak.ms@gmail.com','stasrira@yahoo.com']
    msg['From'] = email_from
    msg['To'] = emails_to
    # msg.preamble = message  # 'Test email with attachment'
    body = MIMEText(message)  # convert the body to a MIME compatible string
    msg.attach(body)  # attach it to your main message


    # Assume we know that the image files are all in PNG format
    #for file in pngfiles:
    file = attachment_path # 'test.png'
    # Open the files in binary mode.  Let the MIMEImage class automatically
    # guess the specific image type.
    with open(file, 'rb') as fp:
        img = MIMEImage(fp.read())
    msg.attach(img)

    # try:
    # Send the email via our own SMTP server.
    with smtplib.SMTP(smtp_server) as s:
        s.sendmail(email_from, emails_to_list, msg.as_string())
        s.quit()
    """
    except Exception as ex:
        # report unexpected error to log file
        #mlog.critical('Unexpected Error "{}" occurred during processing file: {}\n{} '
        #              .format(ex, os.path.abspath(__file__), traceback.format_exc()))
        raise
    """

send_email ('stasrirak.ms@gmail.com, stasrira@yahoo.com, stas.rirak@mssm.edu', 'Test Email #4.2', 'Body of the test email??.', None, 'test.png')

# send_yagmail ('stasrirak.ms@gmail.com, stasrira@yahoo.com, stas.rirak@mssm.edu', 'Test Email #5', 'Body of the test email!!.', None, 'test.png')