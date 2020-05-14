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
    m_cfg = ConfigData(gc.MAIN_CONFIG_FILE)

    # setup application level logger
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    mlog = cm.setup_logger(m_cfg, cur_dir)

    # validate expected environment variables; if some variable are not present, abort execution
    cm.validate_available_envir_variables(mlog, m_cfg, ['default', 'redcap'])

    mlog.info('Start processing API calls.')

    # get list of required API calls
    api_process_configs = m_cfg.get_value('API_process_configs')
    api_cfg = None
    if api_process_configs:
        mlog.info('The following api config files were loaded from the config file: {}'.format(api_process_configs))
        for api_cfg_file in api_process_configs:
            ap = ApiProcess(api_cfg_file, mlog)
            if ap.loaded:
                ap.process_api_call()
            else:
                mlog.warning('API process based on "{}" file has failed. Check error message posted earlier.'
                             .format(api_cfg_file))
        pass
    else:
        mlog.warning('No API config files were loaded from the config file.')