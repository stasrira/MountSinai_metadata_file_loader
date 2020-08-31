from pathlib import Path
import os
import time
import traceback
from jinja2 import Environment, FileSystemLoader, escape
from utils import global_const as gc
# from utils import setup_logger_common  # TODO: figure out how to import setup_logger_common from utils module
from .log_utils import setup_logger_common
import shutil


def get_project_root():
    # Returns project root folder.
    return Path(__file__).parent.parent

def file_exists(fn):
    try:
        with open(fn, "r"):
            return 1
    except IOError:
        return 0

# validates presence of a single environment variable
def validate_envir_variable(var_name):
    out = False
    if os.environ.get(var_name):
        out = True
    return out


# verifies if the list to_add parameter is a list and extend the target list with its values
def extend_list_with_other_list(list_trg, list_to_add):
    if list_to_add and isinstance(list_to_add, list):
        list_trg.extend(list_to_add)
    return list_trg

def setup_logger(m_cfg, log_dir_location, cur_proc_name_prefix = None):
    # get logging related config values
    common_logger_name = gc.MAIN_LOG_NAME
    log_folder_name = gc.LOG_FOLDER_NAME
    logging_level = m_cfg.get_value('Logging/main_log_level')
    # get current location of the script and create Log folder
    # wrkdir = Path(os.path.dirname(os.path.abspath(__file__))) / log_folder_name  # 'logs'
    wrkdir = Path(log_dir_location) / log_folder_name

    # lg_filename = time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log'
    if not cur_proc_name_prefix:
        lg_filename = '{}_{}'.format(time.strftime("%Y%m%d_%H%M%S", time.localtime()),'.log')
    else:
        lg_filename = '{}_{}_{}'.format(cur_proc_name_prefix, time.strftime("%Y%m%d_%H%M%S", time.localtime()), '.log')
    # setup logger

    lg = setup_logger_common(common_logger_name, logging_level, wrkdir, lg_filename)  # logging_level
    mlog = lg['logger']
    return mlog


# this function performs actual API call to the remote server
def perform_api_call(api_url, post_data, mlog_obj, error_obj):
    import pycurl
    from io import BytesIO, StringIO
    from urllib.parse import urlencode
    import certifi

    mlog_obj.info('Start performing api call to {}.'.format(api_url))
    val_out = None
    errors_reported = False

    try:
        buf =  BytesIO()  # StringIO()  #
        """
        data = {
            'token': os.environ.get('REDCAP_HCW_TOKEN'),  
            'content': 'report',
            'format': 'json',
            'report_id': '24219',
            'rawOrLabel': 'label',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'true',
            'returnFormat': 'json'
        }
        """
        # data = post_data
        # print(post_data)
        pf = urlencode(post_data)
        ch = pycurl.Curl()
        ch.setopt(ch.URL, api_url)  # 'https://redcap.mountsinai.org/redcap/api/'
        ch.setopt(ch.POSTFIELDS, pf)
        # ch.setopt(ch.WRITEFUNCTION, buf.write)
        ch.setopt(ch.WRITEDATA, buf)
        # the following is used to avoid "unable to get local issuer certificate"
        # (https://stackoverflow.com/questions/16192832/pycurl-https-error-unable-to-get-local-issuer-certificate)
        ch.setopt(pycurl.CAINFO, certifi.where())
        ch.perform()
        ch.close()

        output = buf.getvalue()  # gets data in bytes
        # print(output)
        val_out = output.decode('UTF-8')  # convert data from bytes to string
        # print(val_out)

        buf.close()

        mlog_obj.info('API call completed successfully.')

    except Exception as ex:
        _str = 'Error "{}" occurred during executing the API call to "{}"\n{}'\
            .format(ex, api_url, traceback.format_exc())
        mlog_obj.error(_str)
        error_obj.add_error(_str)
        errors_reported = True

    return val_out, errors_reported


def eval_cfg_value(cfg_val, mlog_obj, error_obj, self_obj_ref = None):
    eval_flag = gc.YAML_EVAL_FLAG  # 'eval!'

    # if self object is referenced in some of the config values, it will provide are reference to it
    if self_obj_ref:
        self = self_obj_ref
    else:
        self = None

    # check if some configuration instruction/key was retrieved for the given "key"
    if cfg_val:
        if eval_flag in str(cfg_val):
            cfg_val = cfg_val.replace(eval_flag, '')  # replace 'eval!' flag key
            try:
                out_val = eval(cfg_val)
            except Exception as ex:
                _str = 'Error "{}" occurred while attempting to evaluate the following value "{}" \n{} ' \
                    .format(ex, cfg_val, traceback.format_exc())
                if mlog_obj:
                    mlog_obj.error(_str)
                if error_obj:
                    error_obj.add_error(_str)
                out_val = ''
        else:
            out_val = cfg_val
    else:
        out_val = ''
    return out_val

def populate_email_template(template_name, template_feeder):
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    env.trim_blocks = True
    env.lstrip_blocks = True
    env.rstrip_blocks = True

    template = env.get_template(template_name)
    output = template.render(process=template_feeder)
    # print(output)
    return output

def move_file_to_processed(file_path, new_file_name, processed_dir_path, log_obj, error_obj):
    if not os.path.exists(processed_dir_path):
        # if Processed folder does not exist in the current study folder, create it
        log_obj.info('Creating directory for processed files "{}"'.format(processed_dir_path))
        os.mkdir(processed_dir_path)

    file_name = Path(file_path).name
    file_name_new = new_file_name
    file_name_new_path = Path(processed_dir_path) / file_name_new
    cnt = 0
    #check if file with the same name was already saved in "processed" dir
    while os.path.exists(file_name_new_path):
        # if file exists, identify a new name, so the new file won't overwrite the existing one
        if cnt >= gc.PROCESSED_FOLDER_MAX_FILE_COPIES and gc.PROCESSED_FOLDER_MAX_FILE_COPIES >= 0:
            file_name_new_path = None
            break
        cnt += 1
        file_name_new = '{}({}){}'.format(os.path.splitext(file_name)[0], cnt, os.path.splitext(file_name)[1])
        file_name_new_path = Path(processed_dir_path) / file_name_new

    if not file_name_new_path is None:
        # new file name was successfully identified
        # move the file to the processed dir under the identified new name
        os.rename(file_path, file_name_new_path)
        log_obj.info('Processed file "{}" was moved to "{}" under {} name: "{}".'
                     .format(str(file_path), str(processed_dir_path)
                          ,('the same' if cnt == 0 else 'the new')
                          ,file_name_new_path))
    else:
        # new file name was not identified
        _str = 'Processed file "{}" cannot be moved to "{}" because {} copies of this file already exist in this ' \
               'folder that exceeds the allowed application limit of copies for the same file.'\
            .format(file_path, processed_dir_path, cnt + 1)
        log_obj.error (_str)
        error_obj.add_error(_str)
        pass
    return file_name_new_path