import pyodbc
import traceback
from .configuration import ConfigData
from utils import global_const as gc


class MetadataDB:

    # CFG_DB_CONN = 'DB/mdb_conn_str'  # name of the config parameter storing DB connection string
    # CFG_DB_SQL_PROC = 'DB/mdb_sql_proc_load_sample'  # name of the config parameter storing DB name of the stored proc
    # CFG_DB_STUDY_ID = 'DB/mdb_study_id'  # name of the config parameter storing value of the MDB study id
    # CFG_DICT_PATH = 'DB/dict_tmpl_fields_node' # name of the config parameter storing value of dictionary path
    # to list of fields
    # CFG_DB_ALLOW_DICT_UPDATE = 'DB/mdb_allow_dict_update'  # name of the config parameter storing values
    # for "allow dict updates"
    # CFG_DB_ALLOW_SAMPLE_UPDATE = 'DB/mdb_allow_sample_update' # name of the config parameter storing values
    # for "allow sample updates"

    s_conn = ''
    conn = None

    def __init__(self, study_cfg):
        self.cfg = ConfigData(gc.MAIN_CONFIG_FILE)  # obj_cfg
        self.s_conn = self.cfg.get_item_by_key(gc.CFG_DB_CONN).strip()
        self.study_cfg = study_cfg

    def open_connection(self):
        self.conn = pyodbc.connect(self.s_conn, autocommit=True)

    def submit_row(self, row, file):  # sample_id, row_json, dict_json, filepath):

        dict_json = file.get_file_dictionary_json(True)
        filepath = str(file.filepath)
        sample_id = row.sample_id
        row_json = row.to_json()

        if not self.conn:
            self.open_connection()
        str_proc = self.cfg.get_item_by_key(gc.CFG_DB_SQL_PROC).strip()
        study_id = self.study_cfg.get_item_by_key(gc.CFG_DB_STUDY_ID).strip()
        dict_path = '$.' + self.study_cfg.get_item_by_key(gc.CFG_DICT_PATH).strip()
        dict_upd = self.study_cfg.get_item_by_key(gc.CFG_DB_ALLOW_DICT_UPDATE).strip()
        sample_upd = self.study_cfg.get_item_by_key(gc.CFG_DB_ALLOW_SAMPLE_UPDATE).strip()

        # prepare stored proc string to be executed
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_STUDY_ID), study_id)  # '{study_id}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_SAMPLE_ID), sample_id)  # '{sample_id}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_ROW_JSON), row_json)  # '{smpl_json}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_DICT_JSON), dict_json)  # '{dict_json}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_DICT_PATH), dict_path)  # '{dict_path}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_FILEPATH), filepath)  # '{filepath}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_DICT_UPD), dict_upd)  # '{dict_update}'
        str_proc = str_proc.replace(self.cfg.get_item_by_key(gc.CFG_FLD_TMPL_SAMPLE_UPD), sample_upd)
        # '{samlpe_update}'

        file.logger.info('SQL Procedure call = {}'.format(str_proc))
        # print ('procedure (str_proc) = {}'.format(str_proc))

        # TODO: if procedure execution does not fail but return back status saying "ERROR:", record an error for the row
        try:
            cursor = self.conn.cursor()
            cursor.execute(str_proc)
            # returned recordsets
            rs_out = []
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            rs_out.append(results)
            return rs_out

        except Exception as ex:
            # report an error if DB call has failed.
            _str = 'Error "{}" occurred during submitting a row (sample_id = "{}") to database; ' \
                   'used SQL script "{}". Here is the traceback: \n{} '.format(
                    ex, sample_id, str_proc, traceback.format_exc())
            row.error.add_error(_str)
            file.logger.error(_str)
