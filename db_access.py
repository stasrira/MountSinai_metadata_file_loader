import pyodbc
import traceback

class MetadataDB():
	cfg_db_conn = 'mdb_conn_str'  # name of the config parameter storing DB connection string
	cfg_db_sql_proc = 'mdb_sql_proc_load_sample'  # name of the config parameter storing DB name of the stored proc
	cfg_db_study_id = 'mdb_study_id'  # name of the config parameter storing value of the MDB study id
	cfg_dict_path = 'dict_tmpl_fields_node' # name of the config parameter storing value of dictionary path to list of fields
	cfg_db_allow_dict_update = 'mdb_allow_dict_update'  # name of the config parameter storing values for "allow dict updates"
	cfg_db_allow_sample_update = 'mdb_allow_sample_update' # name of the config parameter storing values for "allow sample updates"
	s_conn = ''
	#s_sql_proc = ''
	conn = None
	cfg = None

	def __init__(self, obj_cfg):
		self.cfg = obj_cfg
		self.s_conn = self.cfg.getItemByKey(self.cfg_db_conn).strip()

	def openConnection(self):
		self.conn = pyodbc.connect(self.s_conn, autocommit=True)

	def submitRow(self, row, file): # sample_id, row_json, dict_json, filepath):

		dict_json = file.getFileDictionary_JSON(True)
		filepath = str(file.filepath)
		sample_id = row.sample_id
		row_json = row.toJSON()

		if not self.conn:
			self.openConnection()
		str_proc = self.cfg.getItemByKey(self.cfg_db_sql_proc).strip()
		study_id = self.cfg.getItemByKey(self.cfg_db_study_id).strip()
		dict_path = '$.' + self.cfg.getItemByKey(self.cfg_dict_path).strip()
		dict_upd = self.cfg.getItemByKey(self.cfg_db_allow_dict_update).strip()
		sample_upd = self.cfg.getItemByKey(self.cfg_db_allow_sample_update).strip()

		#prepare stored proc string to be executed
		str_proc = str_proc.replace('{study_id}', study_id)
		str_proc = str_proc.replace('{sample_id}', sample_id)
		str_proc = str_proc.replace('{smpl_json}', row_json)
		str_proc = str_proc.replace('{dict_json}', dict_json)
		str_proc = str_proc.replace('{dict_path}', dict_path)
		str_proc = str_proc.replace('{filepath}', filepath)
		str_proc = str_proc.replace('{dict_update}', dict_upd)
		str_proc = str_proc.replace('{samlpe_update}', sample_upd)

		print ('procedure (str_proc) = {}'.format(str_proc))

		#str_proc = 'select * from dw_studies'
		#str_proc = "exec usp_get_metadata '4'"
		#str_proc = "usp_test_stas1"

		try:
			cursor = self.conn.cursor()
			cursor.execute(str_proc)
			# returned recordsets
			rs_out = []
			rows = cursor.fetchall()
			columns = [column[0] for column in cursor.description]
			# printL (columns)
			results = []
			for row in rows:
				results.append(dict(zip(columns, row)))
			rs_out.append(results)
			return rs_out

		except Exception as ex:
			# report an error if DB call has failed.
			row.error.addError('Error "{}" occurred during submitting a row (sample_id = "{}") to database; used SQL script "{}". Here is the traceback: \n{} '.format(ex, sample_id, str_proc, traceback.format_exc()))
