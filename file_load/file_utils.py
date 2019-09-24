import logging
import traceback
from utils import ConfigData
from utils import global_const as gc

class StudyConfig:
    config_loc = None
    config_glb = None
    study_logger_name =  ''# 'file_processing_log'
    study_logging_level = '' #'INFO'

class FieldIdMethod:
    field_id_methods = ['name', 'number']
    name = field_id_methods[0]
    number = field_id_methods[1]

def loadConfiguration (fl_class, loc_cfg_path):
    # load global configuration

    m_cfg = ConfigData(gc.main_config_file)
    m_logger_name = m_cfg.get_value('Logging/main_log_name')
    m_logger = logging.getLogger(m_logger_name)

    m_logger.debug('Loading Global config file {} for file: {}'.format(gc.main_config_file, fl_class.filepath))
    StudyConfig.config_glb = ConfigData(gc.main_config_file)

    m_logger.info('Loading Study config file {} for file: {}'.format(loc_cfg_path, fl_class.filepath))
    # load local configuration
    try:
        StudyConfig.config_loc = ConfigData(loc_cfg_path)
    except Exception as ex:
        m_logger.error('Error "{}" occurred during loading study config file "{}"\n{}'.format(
            ex, loc_cfg_path, traceback.format_exc()))
        #raise
        return False

    #load global logging setting
    StudyConfig.study_logger_name = StudyConfig.config_glb.get_value(gc.study_logger_name_cfg_path)
    StudyConfig.study_logging_level = StudyConfig.config_glb.get_value(gc.study_logging_level_cfg_path)

    return True
