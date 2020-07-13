from file_load import MetaFileText, MetaFileExcel
from pathlib import Path
import sys
import os
import getpass
from os import walk
import time
import traceback
from utils import ConfigData, common as cm, global_const as gc, send_email as email
from api_load import ApiProcess


# if executed by itself, do the following
if __name__ == '__main__':

    # load main config file and get required values
    gc.CURRENT_PROCCESS_LOG_ID = 'api_load'
    m_cfg = ConfigData(gc.MAIN_CONFIG_FILE)

    # setup application level logger
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    mlog = cm.setup_logger(m_cfg, cur_dir, gc.CURRENT_PROCCESS_LOG_ID)

    # validate expected environment variables; if some variable are not present, abort execution
    cm.validate_available_envir_variables(mlog, m_cfg, ['default', 'redcap'])

    email_msgs_apicalls = []

    mlog.info('Start processing API calls.')
    api_proc_cnt = 0
    # get list of required API calls
    api_process_configs = m_cfg.get_value('API_process_configs')
    api_cfg = None
    if api_process_configs:
        mlog.info('The following api config files were loaded from the config file: {}'.format(api_process_configs))
        for api_cfg_file in api_process_configs:
            api_proc_cnt += 1
            ap = ApiProcess(api_cfg_file, mlog)
            if ap.loaded:
                ap.process_api_call()

                # prepare stats of the api process and create an email body to report stats in the email
                submitted_records = len(ap.dataset.ds) # if ap.dataset.ds else 0
                db_response_alerts_count = len(ap.dataset.db_response_alerts) if ap.dataset.db_response_alerts else 0
                db_response_ok = submitted_records - db_response_alerts_count
                validation_alerts_count = len(ap.dataset.validation_alerts) if ap.dataset.validation_alerts else 0

                # create a dictionary to feed into template for preparing an email body
                template_feeder = {
                    'name': ap.api_name,
                    'config_file': api_cfg_file,
                    'url': ap.api_url,
                    'log_file': mlog.handlers[0].baseFilename,
                    'submitted_records': submitted_records,
                    'db_response_ok': db_response_ok,
                    'db_response_alerts_count': db_response_alerts_count,
                    'db_response_alerts': ap.dataset.db_response_alerts,
                    'validation_alerts_count': validation_alerts_count,
                    'validation_alerts': ap.dataset.validation_alerts,
                    'errors_present': ap.error.errors_exist()
                }
                email_body_part = cm.populate_email_template('processed_api_dataset.html', template_feeder)
                email_msgs_apicalls.append(email_body_part)
                pass
            else:
                mlog.warning('API process based on "{}" file has failed. Check error message posted earlier.'
                             .format(api_cfg_file))
        mlog.info('Number of API calls processed = {}'.format(api_proc_cnt))
        if api_proc_cnt > 0:
            # collect final details and send email about processed API calls
            email_subject = 'Loading metadata through API calls'
            email_body = 'Total of {} API data pulls were completed. See details for each data load below' \
                             .format(api_proc_cnt) \
                         + '<br/><br/>' + \
                         '<br/><br/>'.join(email_msgs_apicalls)
            # print ('email_subject = {}'.format(email_subject))
            # print('email_body = {}'.format(email_body))
        else:
            # collect final details and send email about processed API calls
            email_subject = 'No loading metadata through API calls were performed'
            email_body = 'Total of {} API data pulls were completed.'.format(api_proc_cnt)
    else:
        _str = 'No API config files were loaded based on the "API_process_configs" parameter of the main config file.'
        mlog.warning(_str)
        # collect final details and send email about processed API calls
        email_subject = 'No loading metadata through API calls were performed'
        email_body = _str

    # remove return characters from the body of the email, to keep just clean html code
    email_body = email_body.replace("\r", "")
    email_body = email_body.replace("\n", "")

    # attempt to send status email
    try:
        if m_cfg.get_value('Email/send_emails'):
            email.send_yagmail(
                emails_to=m_cfg.get_value('Email/sent_to_emails'),
                subject=email_subject,
                message=email_body
                # , attachment_path=email_attchms_study
            )
    except Exception as ex:
        # report unexpected error during sending emails to a log file and continue
        _str = 'Unexpected Error "{}" occurred during an attempt to send a status email ' \
               'upon finish processing API calls:\n{} '\
            .format(ex, traceback.format_exc())
        mlog.critical(_str)

    pass