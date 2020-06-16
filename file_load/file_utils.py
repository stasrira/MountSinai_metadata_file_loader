import logging
import traceback
from utils import ConfigData
from utils import global_const as gc


class StudyConfig:
    config_loc = None
    config_glb = None
    study_logger_name = ''  # 'file_processing_log'
    study_logging_level = ''  # 'INFO'


class FieldIdMethod:
    field_id_methods = ['name', 'number']
    name = field_id_methods[0]
    number = field_id_methods[1]


def load_configuration(fl_class, loc_cfg_path):
    # load global configuration

    # m_cfg = ConfigData(gc.MAIN_CONFIG_FILE)
    m_logger_name = gc.MAIN_LOG_NAME  # m_cfg.get_value('Logging/main_log_name')
    m_logger = logging.getLogger(m_logger_name)

    m_logger.debug('Loading Global config file {} for file: {}'.format(gc.MAIN_CONFIG_FILE, fl_class.filepath))
    StudyConfig.config_glb = ConfigData(gc.MAIN_CONFIG_FILE)

    m_logger.info('Loading Study config file {} for file: {}'.format(loc_cfg_path, fl_class.filepath))
    # load local configuration
    try:
        StudyConfig.config_loc = ConfigData(loc_cfg_path)
    except Exception as ex:
        m_logger.error('Error "{}" occurred during loading study config file "{}"\n{}'.format(
            ex, loc_cfg_path, traceback.format_exc()))
        # raise
        return False

    # load global logging setting
    StudyConfig.study_logger_name =  gc.FILE_LOG_NAME # StudyConfig.config_glb.get_value(gc.STUDY_LOGGER_NAME_CFG_PATH)
    StudyConfig.study_logging_level = StudyConfig.config_glb.get_value(gc.STUDY_LOGGING_LEVEL_CFG_PATH)

    return True

def setup_common_basic_file_parameters(file_ob):
    replace_blanks_in_header = file_ob.cfg_file.get_item_by_key('replace_blanks_in_header')
    # set parameter to True or False, if it was set likewise in the config, otherwise keep the default value
    if replace_blanks_in_header:
        if replace_blanks_in_header.lower() in ['true', 'yes']:
            file_ob.replace_blanks_in_header = True
        if replace_blanks_in_header.lower() in ['false', 'no']:
            file_ob.replace_blanks_in_header = False

    # set header_row_number value, if provided in the config
    header_row_num = file_ob.cfg_file.get_item_by_key('header_row_number')
    if header_row_num and header_row_num.isnumeric():
        file_ob.header_row_num = int(header_row_num)