from file_load import MetaFileText, MetaFileExcel
from pathlib import Path
import sys
import os
import getpass
from os import walk
import time
import traceback
import fnmatch
from utils import ConfigData, common as cm, common2 as cm2, global_const as gc, send_email as email


# if executed by itself, do the following
if __name__ == '__main__':

    gc.CURRENT_PROCCESS_LOG_ID = 'file_load'
    # load main config file and get required values
    m_cfg = ConfigData(gc.MAIN_CONFIG_FILE)

    # setup application level logger
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    mlog = cm.setup_logger(m_cfg, cur_dir, gc.CURRENT_PROCCESS_LOG_ID)

    # validate expected environment variables; if some variable are not present, abort execution
    cm2.validate_available_envir_variables(mlog, m_cfg, ['default'], str(Path(os.path.abspath(__file__))))

    # get data processing related config values
    datafiles_path = m_cfg.get_value('Location/data_folder')
    ignore_files = m_cfg.get_value('Location/ignore_files')
    processed_file_copies_max_number = m_cfg.get_value('Location/processed_file_copies_max_number')
    if not processed_file_copies_max_number is None:
        gc.PROCESSED_FOLDER_MAX_FILE_COPIES = processed_file_copies_max_number
    processed_folder_name = gc.PROCESSED_FOLDER_NAME
    # datafiles_path = 'E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles'
    df_path = Path(datafiles_path)

    # perform initial validations
    mlog.info('Start processing files in "{}". '
              'Expected user login: "{}", Effective user: "{}"  '.format(df_path, os.getlogin(), getpass.getuser()))

    # Verify that target directory (df_path) is accessible for the current user (under which the app is running)
    # Identify the user under which the app is running if the df_path is not accessible
    if not os.path.exists(df_path):
        _str = 'Directory "{}" does not exist or not accessible for the current user. Aborting execution. ' \
               'Expected user login: "{}", Effective user: "{}"'.format(df_path, os.getlogin(), getpass.getuser())
        mlog.error(_str)

        # send notification email alerting about the error case
        email_subject = 'Error occurred during running Metadata File Loader tool.'
        email_body = 'The following error caused interruption of execution of the application<br/>' \
                     + str(Path(os.path.abspath(__file__))) \
                     + '<br/><br/><font color="red">' \
                     + _str + '</font>'
        try:
            email.send_yagmail(
                emails_to=m_cfg.get_value('Email/sent_to_emails'),
                subject=email_subject,
                message=email_body
                # ,attachment_path = email_attchms_study
            )
        except Exception as ex:
            # report unexpected error during sending emails to a log file and continue
            _str = 'Unexpected Error "{}" occurred during an attempt to send email upon ' \
                   'finishing execution of file monitoring app.\n{}'.format(ex, traceback.format_exc())
            mlog.critical(_str)

        exit(1)

    # start processing metadata files at the appropriate locations
    try:
        fl_proc_cnt_total = 0  # variable to keep total count of processed files

        (_, dirstudies, _) = next(walk(df_path))
        # print('Study dirs: {}'.format(dirstudies))

        mlog.info('Studies (sub-directories) to be processed (count = {}): {}'.format(len(dirstudies), dirstudies))

        for st_dir in dirstudies:
            st_path = Path(datafiles_path) / st_dir
            mlog.info('Start processing study: "{}", full path: {}'.format(st_dir, st_path))

            # load local study config file
            study_cfg_path = st_path / gc.DEFAULT_STUDY_CONFIG_FILE
            mlog.info('Loading config file for the current study: {}'.format(study_cfg_path))
            study_cfg = ConfigData(study_cfg_path)
            # check if any command is expected to be run on study init event
            exec_cmd = study_cfg.get_value('on_study_init_exec_command')

            if exec_cmd:
                mlog.info('Study On-Init command is provided: {}'.format(exec_cmd))
                mlog.info('Attempting to execute the Study On-Init')
                # if exec_cmd is present attempt to run it
                cm.eval_cfg_value (exec_cmd, mlog, None )

            (_, _, proc_files) = next(walk(Path(st_path)))
            # filter out file that should be ignored
            # ignore_files = ['.DS_Store']

            exl_files = []
            for ign_item in ignore_files:
                exl_files.extend (fnmatch.filter(proc_files, ign_item))

            proc_files = [file for file in proc_files
                          # '~$' should filter out temp file created when excel is open
                          if file not in exl_files and not file.startswith('~$')]

            mlog.info('Files presented (count = {}): "{}"'.format(len(proc_files), proc_files))
            # print ('Study st_dir files: {}'.format(proc_files))

            email_msgs_study = []
            email_attchms_study = []

            proc_files.sort() #sort items in the proc_files to predict order of processing of files

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
                            fl_ob = MetaFileExcel(fl_path, str(study_cfg_path))
                        else:
                            # check if the provided file is not binary one
                            if not cm.is_binary(fl_path):
                                # create an object to process text files
                                mlog.info('File {} was identified as Text file'.format(fl_path))
                                fl_ob = MetaFileText(fl_path, str(study_cfg_path))
                            else:
                                mlog.warning('File "{}" was identified as binary and will not be processed.'
                                             .format(fl_path))
                                continue #skip to the next file in the list

                        # save timestamp of beginning of the file processing
                        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())

                        if not fl_ob.error.errors_exist():
                            mlog.info('Start processing {} file.'.format(fl_path))
                            # process selected file
                            fl_ob.process_file()
                            mlog.info('Finish processing {} file.'.format(fl_path))
                        else:
                            mlog.info('Skipping processing of the file "{}" since some errors were identified '
                                      'during its initializing. Reported error(s): {}'
                                      .format(fl_path, fl_ob.error.get_errors_to_str()))

                        fl_proc_cnt += 1

                        # get total count of rows in the file
                        submitted_records = fl_ob.db_submitted_count
                        # get count of "ERROR" responses from attempts to submit data to DB
                        db_response_alerts_count = len(fl_ob.db_response_alerts) if fl_ob.db_response_alerts else 0
                        db_response_ok = submitted_records - db_response_alerts_count

                        # identify if any errors were identified and set status variable accordingly
                        if not fl_ob.error.errors_exist() and fl_ob.error.row_errors_count() == 0 \
                                and db_response_alerts_count == 0:
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
                        # if not os.path.exists(processed_dir):
                            # if Processed folder does not exist in the current study folder, create it
                        #    mlog.info('Creating directory for processed files "{}"'.format(processed_dir))
                        #    os.mkdir(processed_dir)

                        if fl_ob.processed_add_datestamp:
                            new_file_name = ts + '_' + fl
                        else:
                            new_file_name = fl

                        fl_processed_name = cm.move_file_to_processed(fl_path, new_file_name, processed_dir,fl_ob.logger,fl_ob.error)
                        # fl_processed_name = ts + '_' + fl_status + '_' + fl
                        # print('New file name: {}'.format(ts + '_' + fl_status + '_' + fl))
                        # move processed files to Processed folder
                        # os.rename(fl_path, processed_dir / fl_processed_name)

                        if fl_processed_name:
                            mlog.info('Processed file "{}" was moved(renamed) to: "{}"'
                                  .format(fl_path, processed_dir / fl_processed_name))

                        # create a dictionary to feed into template for preparing an email body
                        template_feeder = {
                            'file_name': fl_path,
                            'file_name_new': processed_dir / fl_processed_name,
                            'log_file': fl_ob.log_handler.baseFilename,
                            'submitted_records': submitted_records,
                            'db_response_ok': db_response_ok,
                            'db_response_alerts_count': db_response_alerts_count,
                            'db_response_alerts': fl_ob.db_response_alerts,
                            'file_errors_present': fl_ob.error.errors_exist(),
                            'row_errors_present': (fl_ob.error.row_errors_count() > 0)
                        }
                        email_body_part = cm.populate_email_template('processed_file.html', template_feeder)
                        email_msgs_study.append(email_body_part)

                        """
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
                        """
                        # print ('email_msgs_study = {}'.format(email_msgs_study))

                        fl_ob = None
                    except Exception as ex:
                        # report an error to log file and proceed to next file.
                        mlog.error('Error "{}" occurred during processing file: {}\n{} '
                                   .format(ex, fl_path, traceback.format_exc()))
                        raise
            mlog.info('Number of files processed for study "{}" = {}'.format(st_dir, fl_proc_cnt))

            fl_proc_cnt_total += fl_proc_cnt

            if fl_proc_cnt>0:
                # collect final details and send email about this study results
                email_subject = 'Metadata files loading for study "{}"'.format(st_dir)
                email_body = ('Number of files processed for study "{}": {}.'.format(st_dir, fl_proc_cnt)
                                + '<br/><br/>'
                                + '<br/><br/>'.join(email_msgs_study)
                                )

                # print ('email_subject = {}'.format(email_subject))
                # print('email_body = {}'.format(email_body))

                # remove return characters from the body of the email, to keep just clean html code
                email_body = email_body.replace("\r", "")
                email_body = email_body.replace("\n", "")

                try:
                    if m_cfg.get_value('Email/send_emails'):
                        email.send_yagmail(
                            emails_to = m_cfg.get_value('Email/sent_to_emails'),
                            subject = email_subject,
                            message = email_body
                            # ,attachment_path = email_attchms_study
                        )
                except Exception as ex:
                    # report unexpected error during sending emails to a log file and continue
                    _str = 'Unexpected Error "{}" occurred during an attempt to send email upon ' \
                           'finish processing "{}" study: {}\n{} '\
                        .format(ex, st_dir, os.path.abspath(__file__), traceback.format_exc())
                    mlog.critical(_str)

        if fl_proc_cnt_total == 0:
            # if no files were found for processing, send a summary email confirming the run of the application
            email_subject = 'Metadata File Loader has run.'.format(st_dir)
            email_body = ('Metadata File Loader<br/>{}<br/>ran successfully, however no metadata files to be processed'
                          ' were found.'.format (str(Path(os.path.abspath(__file__)))))
            try:
                if m_cfg.get_value('Email/send_emails'):
                    email.send_yagmail(
                        emails_to=m_cfg.get_value('Email/sent_to_emails'),
                        subject=email_subject,
                        message=email_body
                        # ,attachment_path = email_attchms_study
                    )
            except Exception as ex:
                # report unexpected error during sending emails to a log file and continue
                _str = 'Unexpected Error "{}" occurred during an attempt to send email upon ' \
                       'finish running Metadata File Loader application<br/>{}' \
                    .format(ex, str(Path(os.path.abspath(__file__))), traceback.format_exc())
                mlog.critical(_str)
            pass

    except Exception as ex:
        # report unexpected error to log file
        _str = 'Unexpected Error "{}" occurred during processing file: {}\n{} '\
            .format(ex, os.path.abspath(__file__), traceback.format_exc())
        mlog.critical(_str)
        raise

    sys.exit()

