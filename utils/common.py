from pathlib import Path
import os
import time
import traceback
from utils import global_const as gc
from jinja2 import Environment, FileSystemLoader, escape

# from utils import setup_logger_common TODO: figure out how to import setup_logger_common from utils module
from .log_utils import setup_logger_common


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


# Validate expected Environment variables; if some variable are not present, abort execution
# setup environment variable sources:
# windows: https://www.youtube.com/watch?v=IolxqkL7cD8
# linux: https://www.youtube.com/watch?v=5iWhQWVXosU
def validate_available_envir_variables (mlog, m_cfg, env_cfg_groups = None):
    # env_cfg_groups should be a list of config groups of expected environment variables
    if not env_cfg_groups:
        env_cfg_groups = []
    if not isinstance(env_cfg_groups, list):
        env_cfg_groups = [env_cfg_groups]

    mlog.info('Start validating presence of required environment variables.')
    env_vars = []
    env_var_confs = m_cfg.get_value('Validate/environment_variables')  # get dictionary of envir variables lists
    if env_var_confs and isinstance(env_var_confs, dict):
        for env_gr in env_var_confs:  # loop groups of envir variables
            if env_gr in env_cfg_groups:
                # proceed here for the "default" group of envir variables
                env_vars = extend_list_with_other_list(env_vars, env_var_confs[env_gr])
        # validate existence of the environment variables
        missing_env_vars = []
        for evar in env_vars:
            if not validate_envir_variable(evar):
                missing_env_vars.append(evar)

        if missing_env_vars:
            # check if any environment variables were recorded as missing
            mlog.error('The following environment variables were not found: {}. Aborting execution. '
                       'Make sure that the listed variables exist before next run.'.format(missing_env_vars))
            exit(1)
        else:
            mlog.info('All required environment variables were found.')


def setup_logger(m_cfg, log_dir_location):
    # get logging related config values
    common_logger_name = gc.MAIN_LOG_NAME
    log_folder_name = gc.LOG_FOLDER_NAME
    logging_level = m_cfg.get_value('Logging/main_log_level')
    # get current location of the script and create Log folder
    # wrkdir = Path(os.path.dirname(os.path.abspath(__file__))) / log_folder_name  # 'logs'
    wrkdir = Path(log_dir_location) / log_folder_name
    lg_filename = time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log'
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

    #TODO: add user friendly handler for errors returned from the remote server
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
        print(post_data)
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
        print(val_out)

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
                mlog_obj.error(_str)
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
    print(output)
    return output
