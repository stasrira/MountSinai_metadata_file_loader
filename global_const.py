# ========== config file names
# main config file name
main_config_file = 'main_config.yaml'
# study level default name for the config file
default_study_config_file = 'study.cfg.yaml'

# predefined paths in the main config file for various variables
study_logger_name_cfg_path = 'Logging/file_log_name'
study_logging_level_cfg_path = 'Logging/file_log_level'

# default values for Study config file properties
default_config_value_list_separator = ','

# default study config file extension
default_study_config_file_ext = '.cfg.yaml'

# database related constants
# predefined paths in the main config file for database related parameters
cfg_db_conn = 'DB/mdb_conn_str'  # name of the config parameter storing DB connection string
cfg_db_sql_proc = 'DB/mdb_sql_proc_load_sample'  # name of the config parameter storing DB name of the stored proc
# predefined names for stored procedure parameters that being passed to procedure specified in "cfg_db_sql_proc"
cfg_fld_tmpl_study_id ='DB/fld_tmpl_study_id'
cfg_fld_tmpl_sample_id ='DB/fld_tmpl_sample_id'
cfg_fld_tmpl_row_json ='DB/fld_tmpl_row_json'
cfg_fld_tmpl_dict_json ='DB/fld_tmpl_dict_json'
cfg_fld_tmpl_dict_path ='DB/fld_tmpl_dict_path'
cfg_fld_tmpl_filepath ='DB/fld_tmpl_filepath'
cfg_fld_tmpl_dict_upd ='DB/fld_tmpl_dict_update'
cfg_fld_tmpl_sample_upd ='DB/fld_tmpl_samlpe_update'

# predefined paths in the study config file for database related parameters
cfg_db_study_id = 'mdb_study_id'  # name of the config parameter storing value of the MDB study id
cfg_dict_path = 'dict_tmpl_fields_node' # name of the config parameter storing value of dictionary path to list of fields
cfg_db_allow_dict_update = 'mdb_allow_dict_update'  # name of the config parameter storing values for "allow dict updates"
cfg_db_allow_sample_update = 'mdb_allow_sample_update' # name of the config parameter storing values for "allow sample updates"

# Excel processing related
study_excel_wk_sheet_name = 'wk_sheet_name' # name of the worksheet name to be used for loading data from