# ========== config file names
# main config file name
MAIN_CONFIG_FILE = 'main_config.yaml'
# study level default name for the config file
DEFAULT_STUDY_CONFIG_FILE = 'study.cfg.yaml'


# name of the folder where all logs files will be stored
# name for the each type of log
MAIN_LOG_NAME = 'main_log'
FILE_LOG_NAME = 'file_processing_log'

#default folder names for logs and processed files
LOG_FOLDER_NAME = 'logs'
PROCESSED_FOLDER_NAME = 'processed'

# predefined paths in the main config file for various variables
# STUDY_LOGGER_NAME_CFG_PATH = 'Logging/file_log_name'
STUDY_LOGGING_LEVEL_CFG_PATH = 'Logging/file_log_level'

# default values for Study config file properties
DEFAULT_CONFIG_VALUE_LIST_SEPARATOR = ','

# default study config file extension
DEFAULT_STUDY_CONFIG_FILE_EXT = '.cfg.yaml'

# database related constants
# predefined paths in the main config file for database related parameters
CFG_DB_CONN = 'DB/mdb_conn_str'  # name of the config parameter storing DB connection string
CFG_DB_SQL_PROC = 'DB/mdb_sql_proc_load_sample'  # name of the config parameter storing DB name of the stored proc
# predefined names for stored procedure parameters that being passed to procedure specified in "CFG_DB_SQL_PROC"
CFG_FLD_TMPL_STUDY_ID = 'DB/fld_tmpl_study_id'
CFG_FLD_TMPL_SAMPLE_ID = 'DB/fld_tmpl_sample_id'
CFG_FLD_TMPL_ROW_JSON = 'DB/fld_tmpl_row_json'
CFG_FLD_TMPL_DICT_JSON = 'DB/fld_tmpl_dict_json'
CFG_FLD_TMPL_DICT_PATH = 'DB/fld_tmpl_dict_path'
CFG_FLD_TMPL_FILEPATH = 'DB/fld_tmpl_filepath'
CFG_FLD_TMPL_DICT_UPD = 'DB/fld_tmpl_dict_update'
CFG_FLD_TMPL_SAMPLE_UPD = 'DB/fld_tmpl_samlpe_update'

# predefined paths in the study config file for database related parameters
CFG_DB_STUDY_ID = 'mdb_study_id'  # name of the config parameter storing value of the MDB study id
CFG_DICT_PATH = 'dict_tmpl_fields_node'  # name of config parameter storing value of dictionary path to list of fields
CFG_DB_ALLOW_DICT_UPDATE = 'mdb_allow_dict_update'  # name of config parameter storing values for "allow dict updates"
CFG_DB_ALLOW_SAMPLE_UPDATE = 'mdb_allow_sample_update'  # name of config param storing values for "allow sample updates"

# Excel processing related
STUDY_EXCEL_WK_SHEET_NAME = 'wk_sheet_name'  # name of the worksheet name to be used for loading data from
