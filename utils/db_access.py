import pyodbc
import traceback
from .configuration import ConfigData
from utils import global_const as gc

class MetadataDB():
	'''
	cfg_db_conn = 'DB/mdb_conn_str'  # name of the config parameter storing DB connection string
	cfg_db_sql_proc = 'DB/mdb_sql_proc_load_sample'  # name of the config parameter storing DB name of the stored proc
	cfg_db_study_id = 'DB/mdb_study_id'  # name of the config parameter storing value of the MDB study id
	cfg_dict_path = 'DB/dict_tmpl_fields_node' # name of the config parameter storing value of dictionary path to list of fields
	cfg_db_allow_dict_update = 'DB/mdb_allow_dict_update'  # name of the config parameter storing values for "allow dict updates"
	cfg_db_allow_sample_update = 'DB/mdb_allow_sample_update' # name of the config parameter storing values for "allow sample updates"
	'''

	s_conn = ''
	conn = None

	def __init__(self, study_cfg):
		self.cfg = ConfigData(gc.main_config_file) # obj_cfg
		self.s_conn = self.cfg.getItemByKey(gc.cfg_db_conn).strip()
		self.study_cfg = study_cfg

	def openConnection(self):
		self.conn = pyodbc.connect(self.s_conn, autocommit=True)

	def submitRow(self, row, file): # sample_id, row_json, dict_json, filepath):

		dict_json = file.getFileDictionary_JSON(True)
		filepath = str(file.filepath)
		sample_id = row.sample_id
		row_json = row.toJSON()

		if not self.conn:
			self.openConnection()
		str_proc = self.cfg.getItemByKey(gc.cfg_db_sql_proc).strip()
		study_id = self.study_cfg.getItemByKey(gc.cfg_db_study_id).strip()
		dict_path = '$.' + self.study_cfg.getItemByKey(gc.cfg_dict_path).strip()
		dict_upd = self.study_cfg.getItemByKey(gc.cfg_db_allow_dict_update).strip()
		sample_upd = self.study_cfg.getItemByKey(gc.cfg_db_allow_sample_update).strip()

		#prepare stored proc string to be executed
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_study_id), study_id) # '{study_id}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_sample_id), sample_id) # '{sample_id}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_row_json), row_json) # '{smpl_json}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_dict_json), dict_json) # '{dict_json}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_dict_path), dict_path) # '{dict_path}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_filepath), filepath) # '{filepath}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_dict_upd), dict_upd) # '{dict_update}'
		str_proc = str_proc.replace(self.cfg.getItemByKey(gc.cfg_fld_tmpl_sample_upd), sample_upd) # '{samlpe_update}'

		# get currrent file_processing_log
		file.logger.debug('SQL Procedure call = {}'.format(str_proc))
		#print ('procedure (str_proc) = {}'.format(str_proc))

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
			_str = 'Error "{}" occurred during submitting a row (sample_id = "{}") to database; used SQL script "{}". Here is the traceback: \n{} '.format(
				ex, sample_id, str_proc, traceback.format_exc())
			row.error.addError(_str)
			file.logger.error (_str)

