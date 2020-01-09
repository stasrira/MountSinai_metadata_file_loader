from file_load import MetaFileText, MetaFileExcel
from pathlib import Path
import sys
import os
import getpass
from os import walk
import time
import traceback
from utils.log_utils import setup_logger_common
from utils import ConfigData
from utils import global_const as gc
from utils import send_email as email

# if executed by itself, do the following
if __name__ == '__main__':

    # load main config file and get required values
    m_cfg = ConfigData(gc.MAIN_CONFIG_FILE)

    # print ('m_cfg = {}'.format(m_cfg.cfg))
    # assign values
    common_logger_name = gc.MAIN_LOG_NAME # m_cfg.get_value('Logging/main_log_name')
    logging_level = m_cfg.get_value('Logging/main_log_level')
    datafiles_path = m_cfg.get_value('Location/data_folder')
    log_folder_name = gc.LOG_FOLDER_NAME
    processed_folder_name = gc.PROCESSED_FOLDER_NAME

    # datafiles_path = 'E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles'
    df_path = Path(datafiles_path)

    # get current location of the script and create Log folder
    wrkdir = Path(os.path.dirname(os.path.abspath(__file__))) / log_folder_name  # 'logs'
    lg_filename = time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log'

    lg = setup_logger_common(common_logger_name, logging_level, wrkdir, lg_filename)  # logging_level
    mlog = lg['logger']

    mlog.info('Start processing files in "{}". '
              'Expected user login: "{}", Effective user: "{}"  '.format(df_path, os.getlogin(), getpass.getuser()))
    
    if not os.path.exists(df_path):
        mlog.error('Directory "{}" does not exist or not accessible for the current user. Aborting execution. '
                   'Expected user login: "{}", Effective user: "{}"  '.format(df_path, os.getlogin(), getpass.getuser()))
        exit(1)
        
    try:

        (_, dirstudies, _) = next(walk(df_path))
        # print('Study dirs: {}'.format(dirstudies))

        mlog.info('Studies (sub-directories) to be processed (count = {}): {}'.format(len(dirstudies), dirstudies))

        for st_dir in dirstudies:
            st_path = Path(datafiles_path) / st_dir
            mlog.info('Start processing study: "{}", full path: {}'.format(st_dir, st_path))

            (_, _, proc_files) = next(walk(Path(st_path)))
            mlog.info('Files presented (count = {}): "{}"'.format(len(proc_files), proc_files))
            # print ('Study st_dir files: {}'.format(proc_files))

            email_msgs_study = []
            email_attchms_study = []

            fl_proc_cnt = 0
            for fl in proc_files:
                fl_path = Path(st_path) / fl
                ln = len(gc.DEFAULT_STUDY_CONFIG_FILE_EXT)  # 9
                if fl[-ln:] != gc.DEFAULT_STUDY_CONFIG_FILE_EXT:  # '.cfg.yaml':
                    try:
                        # print('--------->Process file {}'.format(fl_path))
                        mlog.info('File {} was selected for processing.'.format(fl_path))
                        if fl[-4:] == '.xls' or fl[-5:] == '.xlsx':
                            # identify excel file and create appropriate object to handle it
                            mlog.info('File {} was identified as Excel file'.format(fl_path))
                            fl_ob = MetaFileExcel(fl_path)
                        else:
                            # create an object to process text files
                            mlog.info('File {} was identified as Text file'.format(fl_path))
                            fl_ob = MetaFileText(fl_path)

                        # save timestamp of beginning of the file processing
                        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())

                        mlog.info('Start processing {} file.'.format(fl_path))

                        # process selected file
                        fl_ob.process_file()

                        mlog.info('Finish processing {} file.'.format(fl_path))
                        fl_proc_cnt += 1

                        # identify if any errors were identified and set status variable accordingly
                        if not fl_ob.error.errors_exist() and fl_ob.error.row_errors_count() == 0:
                            fl_status = 'OK'
                            _str = 'Processing status: "{}"; file: {}'.format(fl_status, fl_path)
                        else:
                            fl_status = 'ERROR'
                            _str = 'Processing status: "{}". Check log file for the processed file: {}'\
                                .format(fl_status, fl_path)

                        if fl_status == "OK":
                            mlog.info(_str)
                        else:
                            mlog.warning(_str)

                        # print('=============>>File level errors: {}'.format(fl_ob.error.errors_exist()))
                        # print('=============>>Row level errors: {}'.format(fl_ob.error.row_errors_count()))

                        processed_dir = Path(st_path) / processed_folder_name  # 'Processed'
                        if not os.path.exists(processed_dir):
                            # if Processed folder does not exist in the current study folder, create it
                            mlog.info('Creating directory for processed files "{}"'.format(processed_dir))
                            os.mkdir(processed_dir)

                        fl_processed_name = ts + '_' + fl_status + '_' + fl
                        # print('New file name: {}'.format(ts + '_' + fl_status + '_' + fl))
                        # move processed files to Processed folder
                        os.rename(fl_path, processed_dir / fl_processed_name)
                        mlog.info('Processed file "{}" was moved and renamed as: "{}"'
                                  .format(fl_path, processed_dir / fl_processed_name))

                        # preps for email notification
                        email_msgs_study.append(
                                    ('File <br/>"{}" <br/> was processed and moved/renamed to <br/> "{}".'
                                     '<br/> <b>Errors summary:</b> '
                                     '<br/> File level errors: {}'
                                     '<br/> Row level errors: {}'
                                     '<br/> <i>Log file location: <br/>"{}"</i>'
                                     ''.format(fl_path,
                                               processed_dir / fl_processed_name,
                                               '<font color="red">Check Errors in the log file (attached)</font>'
                                                    if fl_ob.error.errors_exist()
                                                    else '<font color="green">No Errors</font> (the log file is attached)',
                                               '<font color="red">Check Errors in the log file (attached)</font>'
                                                    if fl_ob.error.row_errors_count()
                                                    else '<font color="green">No Errors</font> (the log file is attached)',
                                               fl_ob.log_handler.baseFilename)
                                     )
                        )
                        email_attchms_study.append(fl_ob.log_handler.baseFilename)

                        # print ('email_msgs_study = {}'.format(email_msgs_study))

                        fl_ob = None
                    except Exception as ex:
                        # report an error to log file and proceed to next file.
                        mlog.error('Error "{}" occurred during processing file: {}\n{} '
                                   .format(ex, fl_path, traceback.format_exc()))
                        raise
            mlog.info('Number of files processed for study "{}" = {}'.format(st_dir, fl_proc_cnt))

            if fl_proc_cnt>0:
                # collect final details and send email about this study results
                email_subject = 'Metadata files loading for study "{}"'.format(st_dir)
                email_body = ('Number of files processed for study "{}": {}.'.format(st_dir, fl_proc_cnt)
                                + '<br/><br/>'
                                + '<br/><br/>'.join(email_msgs_study)
                                )

                # print ('email_subject = {}'.format(email_subject))
                # print('email_body = {}'.format(email_body))

                try:
                    if m_cfg.get_value('Email/send_emails'):
                        email.send_yagmail(
                            emails_to = m_cfg.get_value('Email/sent_to_emails'),
                            subject = email_subject,
                            message = email_body,
                            attachment_path = email_attchms_study
                        )
                except Exception as ex:
                    # report unexpected error during sending emails to a log file and continue
                    _str = 'Unexpected Error "{}" occurred during an attempt to send email upon ' \
                           'finishing processing "{}" study: {}\n{} '\
                        .format(ex, st_dir, os.path.abspath(__file__), traceback.format_exc())
                    mlog.critical(_str)


    except Exception as ex:
        # report unexpected error to log file
        _str = 'Unexpected Error "{}" occurred during processing file: {}\n{} '\
            .format(ex, os.path.abspath(__file__), traceback.format_exc())
        mlog.critical(_str)
        raise

    sys.exit()

