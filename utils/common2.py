
from utils import global_const as gc
# from utils import setup_logger_common TODO: figure out how to import setup_logger_common from utils module
# from utils import setup_logger_common
from utils import ConfigData
from utils import common as cm
from utils import send_email as email


# convert headers of a dataset (file or api dataset) to a predefined dictionary structure
# ds_header_list - list of headers to be converted
# cfg_dict - part of configuration file containing predefined dictionary parameters used by the function
# use "sort" parameter to differentiate between 2 scenarios:
#   - preserve columns order of dictionary for creating files
#   - sorted alphabetically dictionary that is being submitted to DB
def get_dataset_dictionary(ds_header_list, cfg_dict, sort=False, sort_by_field=''):

    # dict1 = OrderedDict()
    cfg = ConfigData(None, cfg_dict)

    fields = cfg.get_item_by_key('dict_tmpl_fields_node')  # name of the node in dictionary holding array of fields

    ds_dict = eval(cfg.get_item_by_key('dict_tmpl'))  # {fields:[]}
    fld_dict_tmp = ds_dict[fields][0]
    ds_dict[fields].clear()

    if ds_dict:
        hdrs = ds_header_list  # self.get_row_by_number(1).split(self.file_delim)

        # identify item delimiter to be used with config values
        val_delim = cfg.get_item_by_key('config_value_list_separator')  # read config file to get "value list separator"
        if not val_delim:
            val_delim = ''
        # if retrieved value is not blank, return it; otherwise return ',' as a default value
        val_delim = val_delim if len(val_delim.strip()) > 0 else gc.DEFAULT_CONFIG_VALUE_LIST_SEPARATOR  # ','

        upd_flds = cfg.get_item_by_key('dict_field_tmpl_update_fields').split(val_delim)

        for hdr in hdrs:
            # hdr = hdr.strip().replace(' ', '_') # this should prevent spaces in the name of the column headers
            fld_dict = fld_dict_tmp.copy()
            for upd_fld in upd_flds:
                if upd_fld in fld_dict:
                    fld_dict[upd_fld] = hdr.strip()
            ds_dict[fields].append(fld_dict)

    # sort dictionary if requested
    if sort:
        # identify name of the field to apply sorting on the dictionary
        if len(sort_by_field) == 0 or sort_by_field not in ds_dict[fields][0]:
            sort_by_field = cfg.get_item_by_key('dict_field_sort_by')
            if len(sort_by_field) == 0:
                sort_by_field = 'name'  # hardcoded default

        # apply sorting, if given field name present in the dictionary structure
        if sort_by_field in ds_dict[fields][0]:
            ds_dict[fields] = sorted(ds_dict[fields], key=lambda i: i[sort_by_field])

    return ds_dict

# Validate expected Environment variables; if some variable are not present, abort execution
# setup environment variable sources:
# windows: https://www.youtube.com/watch?v=IolxqkL7cD8
# linux: https://www.youtube.com/watch?v=5iWhQWVXosU
def validate_available_envir_variables (mlog, m_cfg, env_cfg_groups = None, app_path_to_report = None):
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
                env_vars = cm.extend_list_with_other_list(env_vars, env_var_confs[env_gr])
        # validate existence of the environment variables
        missing_env_vars = []
        for evar in env_vars:
            if not cm.validate_envir_variable(evar):
                missing_env_vars.append(evar)

        if missing_env_vars:
            # check if any environment variables were recorded as missing
            _str = 'The following environment variables were not found: {}. Aborting execution. '\
                       'Make sure that the listed variables exist before next run.'.format(missing_env_vars)
            mlog.error(_str)
            # send notification email alerting about the error case
            email_subject = 'Error occurred during running file_monitoring tool'
            email_body = 'The following error caused interruption of execution of the application<br/>' \
                         + (str(app_path_to_report) if app_path_to_report else '') \
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
        else:
            mlog.info('All required environment variables were found.')
