# import pyodbc
import sys
import os
import time
import traceback
from pathlib import Path
import xlrd #installation: pip install xlrd
import json
from collections import OrderedDict
import file_errors as ferr # custom library containing all error processing related classes
import db_access as db # custom library containing all database related classes
import logging
import mdl_logging as ml
import main_cfg as mc
import global_const as gc

# common_logger_name = 'file_processing_log'
# logging_level = 'INFO'

class StudyConfig:
	config_loc = None
	config_glb = None
	study_logger_name =  ''# 'file_processing_log'
	study_logging_level = '' #'INFO'


class FieldIdMethod:
	field_id_methods = ['name', 'number']
	name = field_id_methods[0]
	number = field_id_methods[1]


#Text file class (used as a base)
class File:
	filepath = None
	wrkdir = None
	filename = None
	file_type = None #1:text, 2:excel
	file_delim = None # ','
	lineList = None # []
	__headers = None # []
	error = None  # FileErrors class reference holding all errors associated with the current file
	sample_id_field_names = None # []
	loaded = None
	logger = None

	def __init__(self, filepath, file_type = 1, file_delim = ','):
		self.filepath = filepath
		self.wrkdir = os.path.dirname(os.path.abspath(filepath))
		self.filename = Path(os.path.abspath(filepath)).name
		self.file_type = file_type
		self.file_delim = file_delim
		self.error = ferr.FileError(self)
		self.lineList = []
		self.__headers = []
		self.sample_id_field_names = []
		self.loaded = False

	@property
	def headers(self):
		if not self.__headers:
			self.getHeaders()
		return self.__headers

	def setup_logger(self, wrkdir, filename):

		lg = ml.setup_logger(StudyConfig.study_logger_name, StudyConfig.study_logging_level,
					Path(wrkdir) / 'Logs',
					filename + '_' + time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log')

		self.log_handler = lg['handler']
		return lg['logger']

	def getFileContent (self):
		if not self.logger:
			loc_log = logging.getLogger(StudyConfig.study_logger_name)
		else:
			loc_log = self.logger

		if not self.lineList:
			if self.fileExists (self.filepath):
				loc_log.debug ('Loading file content of "{}"'.format(self.filepath))
				fl = open(self.filepath, "r")
				self.lineList = [line.rstrip('\n') for line in fl]
				fl.close()
				self.loaded = True
			else:
				_str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(self.filepath)
				self.error.addError(_str)
				loc_log.error(_str)
				self.lineList = None
				self.loaded = False
		return self.lineList

	def fileExists(self, fn):
		try:
			open(fn, "r")
			return 1
		except IOError:
			return 0

	def getHeaders (self):
		if not self.__headers:
			hdrs = self.getRowByNumber(1).split(self.file_delim)
			self.__headers = [hdr.strip().replace(' ', '_') for hdr in hdrs]
		return self.__headers

	def getRowByNumber (self, rownum):
		lineList = self.getFileContent()
		#check that requested row is withing available records of the file and >0
		if not lineList == None and len(lineList) >= rownum and rownum > 0:
			return lineList[rownum-1]
		else:
			return ""

	def rowsCount(self, excludeHeader = False):
		num = len(self.getFileContent())
		if excludeHeader:
			num = num - 1
		return num

#Config file class
class ConfigFile(File):
	config_items = None
	# config_items_populated = False
	key_value_delim = None
	line_comment_sign = None

	def __init__(self, filepath, file_type=1, key_value_delim=':', line_comment_sign='##'):
		self.key_value_delim = key_value_delim
		self.line_comment_sign = line_comment_sign
		self.config_items = {}
		File.__init__(self, filepath, file_type, self.key_value_delim)
		self.loadConfigSettings()

	# loads config setting assuming that it is a dictionary ==> key: value
	# to identify all key/value pairs, it will split based on ":" for 1 delimiter only and will keep the rest as value of the key
	# any text after "##" will be considered a comment and will be ignored
	def loadConfigSettings(self):
		if not self.config_items: # self.config_items_populated:
			lns = File.getFileContent(self)

			if self.loaded:
				for l in lns:
					values = l.split(self.line_comment_sign, 1)[0].split(self.file_delim, 1)  # use only first delimiter

					if len(values) >= 2:
						# add items to a dictionary
						self.config_items[values[0].strip()] = values[1].strip()
		return self.config_items

	def getItemByKey(self, item_key):
		# check if config file was already loaded
		if not self.config_items:
			self.loadConfigSettings()
		# check if requested key exists
		if item_key in self.config_items:
			return self.config_items[item_key]
		else:
			return None

# metadata text file class
class MetaFileText(File):
	cfg_file = None
	file_dict = None # OrderedDict()
	rows = None # OrderedDict()

	def __init__(self, filepath, cfg_path = '', file_type = 1, file_delim = ','):
		File.__init__(self, filepath, file_type, file_delim)

		self.logger = self.setup_logger(self.wrkdir, self.filename)
		self.logger.info('Start working with file {}'.format(filepath))

		self.logger.info('Loading config file.')
		# identify name of the config file for a study
		if len(cfg_path) == 0:
			cfg_path = Path(self.wrkdir) / gc.default_study_config_file

		if self.fileExists(cfg_path):
			loadConfiguration(self, cfg_path)
			self.cfg_file = StudyConfig.config_loc
			self.file_dict = OrderedDict()
			self.rows = OrderedDict()
		else:
			_str = 'Study configuration file "{}" does not exist, configuration loading was aborted.'.format(cfg_path)
			self.error.addError(_str)
			self.logger.error(_str)

		# self.cfg_file = StudyConfig.config_loc



		'''
		cfg_file = self.getConfigInfo('') # cfg_filepath
		if not cfg_file.loaded:
			# report error for for failed config file loading
			_str = 'Neither the provided config file "{}" nor default "config.cfg" file could not be loaded.'.format(cfg_filepath)
			self.error.addError(_str)
			self.logger.error(_str)
		'''

		# self.file_dict = OrderedDict()
		# self.rows = OrderedDict()

	# read headers of the file and create a dictionary for it
	# dictionary for creating files should preserve columns order
	# dictionary to be submitted to DB has to be sorted alphabetically
	def getFileDictionary(self, sort = False, sort_by_field = ''):

		dict = OrderedDict()

		cfg = self.getConfigInfo()#get reference to config info class
		fields = cfg.getItemByKey('dict_tmpl_fields_node') # get name of the node in dictionary holding array of fields

		if not self.file_dict:
			dict = eval(cfg.getItemByKey('dict_tmpl')) #{fields:[]}
			fld_dict_tmp = dict[fields][0] #eval(cfg.getItemByKey('dict_field_tmpl'))
			dict[fields].clear()

			if dict:
				hdrs = self.getHeaders() # self.getRowByNumber(1).split(self.file_delim)

				upd_flds = cfg.getItemByKey('dict_field_tmpl_update_fields').split(self.configValueListSeparator())

				for hdr in hdrs:
					# hdr = hdr.strip().replace(' ', '_') # this should prevent spaces in the name of the column headers
					fld_dict = fld_dict_tmp.copy()
					for upd_fld in upd_flds:
						fld_dict[upd_fld] = hdr.strip()
					dict[fields].append(fld_dict)

				self.file_dict = dict

		dict = self.file_dict

		# sort dictionary if requested
		if sort:
			# identify name of the field to apply sorting on the dictionary
			if len(sort_by_field) == 0 or not sort_by_field in dict[fields][0]:
				sort_by_field = cfg.getItemByKey('dict_field_sort_by')
				if len(sort_by_field)== 0:
					sort_by_field = 'name' #hardcoded default

			# apply sorting, if given field name present in the dictionary structure
			if sort_by_field in dict[fields][0]:
				dict[fields] = sorted(dict[fields], key=lambda i: i[sort_by_field])

		return dict

	def getFileDictionary_JSON (self, sort = False, sort_by_field = ''):
		dict = self.getFileDictionary(sort, sort_by_field) #get dictionary object for the file dictionary
		return json.dumps(dict) #convert received dictionary to JSON

	def getConfigInfo(self, cfg_file_path = ''):
		return StudyConfig.config_loc
		'''
		if not self.cfg_file:
			if str(cfg_file_path).strip() == "":
				# if config file path is blank, an attempt to use "config.cfg" file located in the current file folder will be used
				cfg_file_path = Path(self.wrkdir) / "config.cfg"
			self.cfg_file = ConfigFile(cfg_file_path, 1)  # config file will use ':' by default
		return self.cfg_file
		'''

	def configValueListSeparator(self):
		val_delim = self.getConfigInfo().getItemByKey('config_value_list_separator') # read config file to get "value list separator"
		#self.logger.debug('configValueListSeparator() => val_delim = "{}"'.format(val_delim))
		if not val_delim:
			val_delim = ''
		# if retrieved value is not blank, return it; otherwise return ',' as a default value
		return val_delim if len(val_delim.strip()) > 0 else gc.default_config_value_list_separator # ','

	# this will convert each row to a JSON ready dictionary based on the headers of the file
	def getFileRow(self, rownum):

		out_dict = {'row':{},'error':None}

		hdrs = self.getHeaders() # self.getRowByNumber(1).split(self.file_delim) #get list of headers
		lst_content = self.getRowByNumber(rownum).split(self.file_delim) #get list of values contained by the row

		# print('file name through Error object - getFileRow() - before Row instance = {}'.format(self.error.entity.filepath))

		row = Row(self, rownum, lst_content, hdrs)
		row.error = ferr.RowErrors(row)

		if len(hdrs) == len (lst_content):
			self._validateMandatoryFieldsPerRow(row) # validate row for required fields being populated

			# create dictionary of the row, so it can be converted to JSON
			for hdr, cnt in zip(hdrs, lst_content):
				row.row_dict[hdr.strip()] = cnt.strip()

			# set sample id for the row
			row.assignSampleId()
		else:
			row.row_dict = None
			_str = 'Row #{}. Incorrect number of fields! The row contains {} field(s), while {} headers are present.'.format(rownum, len (lst_content), len(hdrs))
			row.error.addError(_str)
			self.logger.error(_str)

		return row #out_dict

	def _validateMandatoryFieldsPerRow(self, row):
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()
		mandatFields = cfg.getItemByKey('mandatory_fields').split(delim)
		mandatMethod = cfg.getItemByKey('mandatory_fields_method').split(delim)[0].strip()
		out_val = 0 # keeps number of found validation errors
		# mandatFieldUsed = []

		# validate every field of the row to make sure that all mandatory fields are populated
		i = 0 #keeps field count
		for hdr, cnt in zip(row.header, row.row_content):
			i += 1
			# identify appropriate mandatory field (mf) for the current header field (hdr)
			for mf in mandatFields:
				# identify comparision method
				if mandatMethod == 'name':
					chk_val = hdr.strip()
				elif mandatMethod == 'number':
					chk_val = i
				else:
					chk_val = None

				# proceed if header is matching mandatory field
				if str(chk_val) == mf.strip():
					# validate if the value of the field for the current row is blank and report error if so
					if len(cnt.strip()) == 0:
						out_val += 1  # increase number of found errors
						# report error for mandatory field being empty
						_str = 'Row #{}. Mandatory field "{}" (column #{}) has no value provided.'.format(row.row_number, hdr, i)
						row.error.addError(_str)
						self.logger.error(_str)

		return out_val # return count found validation error

	def _verify_id_method (self, method, process_verified_desc = 'Unknown'):
		if not method in FieldIdMethod.field_id_methods:
			# incorrect method was provided
			_str = 'Configuration issue - unexpected identification method "{}" was provided for "{}". Expected methods are: {}'.format(method, process_verified_desc, ', '.join(FieldIdMethod.field_id_methods))
			self.error.addError(_str)
			self.logger.error(_str)

	# this verifies that if method of identificatoin fields set as "number", list of fields contains only numeric values
	def _verify_field_id_type_vs_method (self, method, fields, process_verified_desc = 'Unknown'):
		if method in FieldIdMethod.field_id_methods:
			if method == FieldIdMethod.number: # 'number'
				#check that all provided fields are numbers
				for f in fields:
					if not f.strip().isnumeric():
						# report error
						_str = 'Configuration issue - provided value "{}" for a field number of "{}" is not numeric while the declared method is "{}".'.format(f, process_verified_desc, method)
						self.error.addError(_str)
						self.logger.error(_str)

	def _validate_fields_vs_headers (self, fields_to_check, field_id_method,
									 fields_to_check_param_name, field_id_method_param_name):
		fields = fields_to_check
		method = field_id_method
		fieldUsed = []
		fieldMissed = []

		self._verify_id_method(method, fields_to_check_param_name) # check that provided methods exist
		self._verify_field_id_type_vs_method(method, fields, field_id_method_param_name) # check field ids vs method

		hdrs = self.headers  # self.getHeaders()

		i = 0  # keeps field count
		for hdr in hdrs:
			i += 1
			for mf in fields:
				# check method
				if method == FieldIdMethod.name:  # 'name':
					hdr_val = hdr.strip()
				elif method == FieldIdMethod.number:  # 'number':
					hdr_val = i
				else:
					hdr_val = None

				if str(hdr_val) == mf.strip():
					fieldUsed.append(mf.strip())

		if len(fields) != len(fieldUsed): # if not all fields from the list were matched to header
			for mf in fields:
				if not mf.strip() in fieldUsed:
					fieldMissed.append(mf.strip())
		return fieldMissed

	# this verifies that all fields passed in the "fields_to_check" list are utilized in the "expression_to_check"
	def _validate_fields_vs_expression (self, fields_to_check, expression_to_check):
		fieldMissed = []

		for fd in fields_to_check:
			if not '{{{}}}'.format(fd.strip()) in expression_to_check:
				fieldMissed.append(fd.strip())

		return fieldMissed

	def _validateMandatoryFieldsExist(self):
		self.logger.info('Validating that all mandatory fields exist.')
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()

		# names of config parameters to get config values
		fields_param_name = 'mandatory_fields'
		method_param_name = 'mandatory_fields_method'

		# retrieve config values
		fields = cfg.getItemByKey(fields_param_name).split(delim)
		method = cfg.getItemByKey(method_param_name).split(delim)[0].strip()

		# validated fields against headers of the metafile
		fieldMissed = self._validate_fields_vs_headers(
			fields, method, fields_param_name, method_param_name)

		if fieldMissed:
			# report error for absent mandatory field
			_str = 'File {}. Mandatory field {}(s): {} - was(were) not found in the file.'.format(self.filename, method,','.join(fieldMissed))
			self.error.addError(_str)
			self.logger.error(_str)

	def _validateSampleIDFields(self):
		self.logger.info('Validating that all fields required for identifying sample id exist.')
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()

		# names of config parameters to get config values
		fields_param_name = 'sample_id_fields'
		method_param_name = 'sample_id_method'
		expr_name = 'sample_id_expression'

		# retrieve config values
		fields = cfg.getItemByKey(fields_param_name).split(delim)
		method = cfg.getItemByKey(method_param_name).strip() #split(delim)[0].
		expr_str = cfg.getItemByKey(expr_name).strip()

		# validated fields against headers of the metafile
		fieldMissed = self._validate_fields_vs_headers(
			fields, method, fields_param_name, method_param_name)
		if fieldMissed:
			# report error if some sample_id component fields do not match header names or numbers (depending on the method)
			_str = 'File {}. Sample ID field {}(s): {} - was(were) not found in the file.'.format(self.filename, method,','.join(fieldMissed))
			self.error.addError(_str)
			self.logger.error(_str)
		else:
			fieldMissed2 = self._validate_fields_vs_expression (fields, expr_str)
			if fieldMissed2:
				# report error if some sample_id component fields were not found in the sample_id_expression
				_str = 'Configuration issue - Sample ID field(s) "{}" was(were) not found in the "sample_id_expression" parameter - {}.'.format(','.join(fieldMissed2), expr_str)
				self.error.addError(_str)
				self.logger.error(_str)

	def processFile(self):
		# validate file for "file" level errors (assuming that config file was loaded)
		if self.cfg_file and self.cfg_file.loaded:
			self._validateMandatoryFieldsExist()
			self._validateSampleIDFields()

		#TODO: validate MDB study_id. If it not set, attempt to create a study. If this process fails, report a File lever error.


		if self.error.errorsExist():
			# report file level errors to Log and do not process rows
			self.logger.error('File level errors we identified! Aborting the file processing.')
			self.logger.error('Summary of File lever errors: {}'.format(self.error.getErrorsToStr()))
			# print('=====>>>> File ERROR reported!!!')
			# print ('File Error Content: {}'.format(self.error.getErrorsToStr()))
		else:
			# proceed with processing file, if no file-level errors were found
			self.logger.info ('Proceeding with processing rows of the file.')
			numRows = self.rowsCount()
			for i in range(1, numRows):
				self.logger.debug('Processing row #{} out of {}.'.format(i, numRows))
				row = self.getFileRow(i+1)
				self.rows[row.row_number] = row # add Row class reference to the list of all rows

				if not row.error.errorsExist():
					# print ('No Errors - Saving to DB, Row Info: {}'.format (row.toStr()))
					self.logger.info('Row #{}. No Row level errors were identified. Saving it to database. Row data: {}'.format(
						row.row_number, row.toStr()))
					mdb = db.MetadataDB(self.cfg_file)

					mdb_resp = mdb.submitRow(row, self) # row.sample_id, row.toJSON(), self.getFileDictionary_JSON(True), str(self.filepath))
					if not row.error.errorsExist():
						_str = 'Row #{}. Sample Id "{}" was submitted to MDB. Status: {}; Description: {}'.format(
							row.row_number, row.sample_id, mdb_resp[0][0]['status'], mdb_resp[0][0]['description'])
						self.logger.info(_str)
						# for r in mdb_resp:
						#	 print(r[0]['status'])
						#	 print(r[0]['description'])
					else:
						_str = 'Error occured during submitting sample Id "{}" to MDB. Error details: {}'.format(
							row.sample_id, row.error.getErrorsToStr())
						self.logger.error(_str)
				else:
					# report to log file if Row level errros were identified
					_str = 'Row #{}. Row level errors were identified. Errors: {}; Row data: {}'.format(
						row.row_number, row.error.getErrorsToStr(), row.row_content)
					self.logger.error(_str)
					# print ('Add row to Error file - Errors Present {}, Row Info: {}'.format(row.error.getErrorsToStr(), row.row_content))

		# report error summary of processing the file
		self.logger.info('SUMMARY OF ERRORS ==============>')
		self.logger.info('File level errors: {}'.format(self.error.getErrorsToStr())) if self.error.errorsExist() else self.logger.info('No File level errors were identified.')
		# print ('------> Summary of errors for file {}'.format(self.filename))
		# print('Summary of File level errors: {}'.format(self.error.getErrorsToStr())) if self.error.errorsExist() else print('No File level errors!')

		row_err_cnt = self.error.rowErrorsCount()
		if row_err_cnt == 0:
			# print('No Row level errors found for this file!')
			self.logger.info('No Row level errors were identified.')
		else:
			for d in self.rows.values():
				if (d.error.errorsExist()):
					# print ('Row level error: {}'.format(d.error.getErrorsToStr()))
					self.logger.info('Row level error: {}'.format(d.error.getErrorsToStr()))

		# release the log handler for the current file
		#self.logger.removeHandler(self.log_handler)
		ml.deactivate_logger(self.logger, self.log_handler)

# TODO: for Text and Excel files - handling commas a part of the field values provided.
#  		Idea is to accomodate double quotes as text identifier; however double quotes should not be considered a value

# metadata Excel file class
class MetaFileExcel(MetaFileText):
	# cfg_file = None
	# file_dict = None  # OrderedDict()
	# rows = None  # OrderedDict()
	sheet_name = None

	def __init__(self, filepath, cfg_path = '', file_type=2, sheet_name = ''):

		#loadConfiguration (main_cfg_obj) # load global and local configureations

		File.__init__(self, filepath, file_type)

		self.logger = self.setup_logger(self.wrkdir, self.filename)
		self.logger.info('Start working with file {}'.format(filepath))

		self.logger.info('Loading config file.')
		# identify name of the config file for a study
		if len(cfg_path) == 0:
			cfg_path = Path(self.wrkdir) / gc.default_study_config_file

		if self.textFileExists(cfg_path):
			loadConfiguration(self, cfg_path)
			self.cfg_file = StudyConfig.config_loc
			self.file_dict = OrderedDict()
			self.rows = OrderedDict()

			self.sheet_name = ''
			# self.sheet_name = sheet_name.strip()
			if not self.sheet_name or len(self.sheet_name) == 0:
				# if sheet name was not passed as a parameter, try to get it from config file
				self.sheet_name = self.cfg_file.getItemByKey(gc.study_excel_wk_sheet_name) # 'wk_sheet_name'
			# print (self.sheet_name)
			self.logger.info('Sheet name that data will be loaded from: "{}"'.format(self.sheet_name))
		else:
			_str = 'Study configuration file "{}" does not exist, configuration loading was aborted.'.format(cfg_path)
			self.error.addError(_str)
			self.logger.error(_str)

		'''	
		# self.cfg_file = None
		cfg_file = self.getConfigInfo ('') # '(cfg_filepath)
		if not cfg_file.loaded:
			# report error for for failed config file loading
			_str = 'Neither the provided config file "{}" nor default "config.cfg" file could not be loaded.'.format(cfg_filepath)
			self.error.addError(_str)
			self.logger.error(_str)

		self.file_dict = OrderedDict()
		self.rows = OrderedDict()
		self.sheet_name = ''
		# self.sheet_name = sheet_name.strip()
		if not self.sheet_name or len(self.sheet_name) == 0:
			# if sheet name was not passed as a parameter, try to get it from config file
			self.sheet_name = self.cfg_file.getItemByKey('wk_sheet_name')
			# print (self.sheet_name)
		self.logger.info('Sheet name that data will be loaded from: "{}"'.format(self.sheet_name))
		'''

	def getFileContent (self):
		if not self.lineList:
			if self.fileExists (self.filepath):
				self.logger.debug('Loading file content of "{}"'.format(self.filepath))

				wb = xlrd.open_workbook(self.filepath)
				if not self.sheet_name or len(self.sheet_name) == 0:
					# by default retrieve the first sheet in the excel file
					sheet = wb.sheet_by_index(0)
				else:
					# if sheet name was provided
					sheets = wb.sheet_names() # get list of all sheets
					if (self.sheet_name in sheets):
						# if given sheet name in the list of available sheets, load the sheet
						sheet = wb.sheet_by_name(self.sheet_name)
					else:
						# report an error if given sheet name not in the list of available sheets
						_str = 'Given sheet name "{}" was not found in the file "{}". Verify that the sheet name exists in the file.'.format(self.sheet_name, self.filepath)
						self.error.addError(_str)
						self.logger.error(_str)

						self.lineList = None
						self.loaded = False
						return self.lineList

				sheet.cell_value(0, 0)

				for i in range(sheet.nrows):
					# ln = sheet.row_values(i)
					# print (ln)

					ln = []
					for j in range(sheet.ncols):
						# print(sheet.cell_value(i, j))
						# ln.append('"' + sheet.cell_value(i,j) + '"')
						cell = sheet.cell(i, j)
						cell_value = cell.value
						# take care of number and dates received from Excel and converted to float by default
						if cell.ctype == 2 and int(cell_value) == cell_value:
							cell_value = str(int(cell_value))
						# convert date back to human readable date format
						# print ('cell_value = {}'.format(cell_value))
						if cell.ctype == 3:
							cell_value_date = xlrd.xldate_as_datetime(cell_value, wb.datemode)
							cell_value = cell_value_date.strftime("%Y-%m-%d")
						ln.append(cell_value)

					self.lineList.append (','.join(ln))

				wb.unload_sheet(sheet.name)
				self.loaded = True
			else:
				_str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(self.filepath)
				self.error.addError(_str)
				self.logger.error(_str)

				self.lineList = None
				self.loaded = False
		return self.lineList

	def textFileExists(self, fn):
		return MetaFileText.fileExists(self, fn)

	def fileExists(self, fn):
		try:
			wb = xlrd.open_workbook(fn)
			return 1
		except Exception as ex: # IOError
			# print (ex)
			return 0

class Row ():
	file = None #reference to the file object that this row belongs to
	row_number = None #row number - header = row #1, next line #2, etc.
	row_content = None #[] #list of values from a file for this row
	_row_dict = None #OrderedDict() of a headers
	header = None # [] #list of values from a file for the first row (headers)
	__error = None #RowErrors class reference holding all errors associated with the current row
	__sample_id = None #it stores a sample Id value for the row.

	def __init__(self, file, row_num, row_content, header):
		self.file = file
		self.row_number = row_num
		self.row_content = row_content
		self.header = header
		self.row_dict = OrderedDict()

	@property
	def sample_id(self):
		return self.__sample_id

	@sample_id.setter
	def sample_id(self, value):
		self.__sample_id = value

	@property
	def row_dict (self):
		return self._row_dict

	@row_dict.setter
	def row_dict(self, value):
		self._row_dict = value

	@property
	def error(self):
		return self.__error

	@error.setter
	def error(self, value):
		self.__error = value

	def toJSON(self):
		# print ('From withing toJSON - Dictionary source:{}'.format(self.row_dict))
		return json.dumps(self.row_dict)

	def toStr(self):
		row = {
			'file':str(self.file.filepath),
			'row_number':self.row_number,
			'sample_id':self.sample_id,
			'row_JSON':self.toJSON(),
			'errors': self.error.errors
		}
		return row

	def assignSampleId(self):
		self.file.logger.debug('Row #{}. Assigning sample id value.'.format(self.row_number))

		cfg = self.file.getConfigInfo()
		delim = self.file.configValueListSeparator()

		# retrieve config values for sample id retrieval
		sid = cfg.getItemByKey('sample_id_expression').strip()
		fields = cfg.getItemByKey('sample_id_fields').split(delim)
		method = cfg.getItemByKey('sample_id_method').strip() #split(delim)[0].

		for sf in fields:
			i = 0  # keeps field count
			for hdr, cnt in zip(self.header, self.row_content):
				i += 1
				# check sample_id fields
				if method == FieldIdMethod.name:  # self.field_id_methods[0]: # 'name':
					smp_val = hdr.strip()
				elif method == FieldIdMethod.number: # self.field_id_methods[1]: # 'number':
					smp_val = i
				else:
					smp_val = None

				if str(smp_val) == sf.strip():
					sid = sid.replace('{{{}}}'.format(str(smp_val)), cnt)

		self.file.logger.debug ('Row #{}. Expression for sample id evaluation: "{}"'.format(self.row_number, sid))
		try:
			smp_evaled = eval(sid) # attempt to evaluate expression for sample id
		except Exception as ex:
			# report an error if evaluation has failed.
			_str = 'Error "{}" occurred during evaluating sample id expression: {}\n{} '.format(ex, sid, traceback.format_exc())
			self.error.addError(_str)
			self.file.logger.error(_str)
			self.file.logger.debug(sys.exc_info()[1])

		self.__sample_id = str(smp_evaled).strip()
		return self.__sample_id

def loadConfiguration (fl_class, loc_cfg_path):
	# load global configuration

	#loc_log = logging.getLogger(StudyConfig.study_logger_name)
	m_cfg = mc.ConfigData(gc.main_config_file)
	m_logger_name = m_cfg.get_value('Logging/main_log_name')
	m_logger = logging.getLogger(m_logger_name)

	m_logger.debug('Loading Global config file {} for file: {}'.format(gc.main_config_file, fl_class.filepath))
	StudyConfig.config_glb = mc.ConfigData(gc.main_config_file)

	m_logger.info('Loading Study config file {} for file: {}'.format(loc_cfg_path, fl_class.filepath))
	# load local configuration
	try:
		StudyConfig.config_loc = mc.ConfigData(loc_cfg_path)
	except Exception as ex:
		m_logger.error('Error "{}" occurred during loading study config file "{}"\n{}'.format(
			ex, loc_cfg_path, traceback.format_exc()))
		#raise
		return False

	#load global logging setting
	StudyConfig.study_logger_name = StudyConfig.config_glb.get_value(gc.study_logger_name_cfg_path)
	StudyConfig.study_logging_level = StudyConfig.config_glb.get_value(gc.study_logging_level_cfg_path)

	return True


	# populate values from local config file


# if executed by itself, do the following
if __name__ == '__main__':

	'''
	# Testing: sorting of dictionaries
	lis = [{"name": "Nandini", "age": 20},
		   {"name": "Manjeet", "age": 21},
		   {"name": "Nikhil", "age": 19}]

	lisA = {"field":[{"name": "Nandini", "age": 20},
		   {"name": "Manjeet", "age": 21},
		   {"name": "Nikhil", "age": 19}]}

	# using sorted and lambda to print list sorted
	# by age
	print ("The list printed sorting by age: ")
	print (sorted(lis, key=lambda i: i['age']))
	print('==================')
	print(lisA)
	lisA['field']=sorted(lisA['field'], key=lambda i: i['age'])
	print (sorted(lisA['field'], key=lambda i: i['age']))
	print(lisA)

	#sys.exit()
	'''


	data_folder = Path("E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles/study01")
	#print (data_folder)
	file_to_open = data_folder / "test1.txt"
	#print (file_to_open)

	#read file
	#fl = File(file_to_open)
	#fl = ConfigFile(file_to_open)

	#fl.readFileLineByLine()

	#fl = None


	#read config file
	file_to_open_cfg = data_folder / "config.cfg"
	#print('file_to_open_cfg = {}'.format(file_to_open_cfg))
	#fl = ConfigFile(file_to_open_cfg, 1) #config file will use ':' by default
	##fl.loadConfigSettings()

	#print('Setting12 = {}'.format(fl.getItemByKey('Setting12')))
	#fl = None

	#sys.exit()

	#read metafile #1
	file_to_open = data_folder / "test1.txt"
	fl1 = MetaFileText(file_to_open, file_to_open_cfg)
	print('Process metafile: {}'.format(fl1.filename))
	fl1.processFile()
	fl1 = None

	#sys.exit()  # =============================

	data_folder = Path(
		"E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles/study02")
	# print (data_folder)
	file_to_open = data_folder / "test01.xlsx"
	# fl3 = MetaFileExcel (file_to_open,'',2,'TestSheet1')
	fl3 = MetaFileExcel(file_to_open)
	# print('File row from excel: {}'.format(fl3.getFileRow(2)))
	fl3.processFile()
	fl3 = None

	sys.exit()  # =============================

	# read metafile #2
	file_to_open = data_folder / "test2.txt"
	fl = MetaFileText(file_to_open)
	print('Process metafile: {}'.format(fl.filename))
	fl.processFile()
	fl = None

	print ('\nTest sorting of dictionary-----------')
	# read metafile #1
	file_to_open = data_folder / "test1.txt"
	fl = MetaFileText(file_to_open)
	#test dictionary extracts and sorting
	#dict_json = fl.getFileDictionary_JSON()
	print ('Not Sorted dictionary ====>{}'.format(fl.getFileDictionary_JSON()))
	#print (dict_json)
	#dict_json_sorted = fl.getFileDictionary_JSON(True, "type")
	print ('Sorted dictionary by "type" field ====>{}'.format(fl.getFileDictionary_JSON(True, "type")))
	#print (dict_json_sorted)
	#dict_json_sorted = fl.getFileDictionary_JSON(True)
	print ('Sorted by default field ====>{}'.format(fl.getFileDictionary_JSON(True)))
	#print (dict_json_sorted)
	fl = None


	sys.exit() # =============================

	#read excel file
	file_to_open = data_folder / "ISSMS_ECHO_PBMC_SEALFON.xlsx"
	wb = xlrd.open_workbook(file_to_open)
	#sheet = wb.sheet_by_index(0)
	sheet = wb.sheet_by_name("Sheet1")

	headers = sheet.row_values(0)
	print(headers)
	print(sheet.row_values(1))

	headers.sort()

	for x in headers:
		print (x)





