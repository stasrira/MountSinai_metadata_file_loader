import pyodbc
import sys
import os
from pathlib import Path
import xlrd #installation: pip install xlrd
import json
from collections import OrderedDict
import file_errors as ferr #custom library containing all error processing related classes

def printL (m):
	if __name__ == '__main__':
		print (m)

class FieldIdMethod:
	field_id_methods = ['name', 'number']
	name = field_id_methods[0]
	number = field_id_methods[1]

#Text file class (used as a base)
class File:
	filepath = ''
	wrkdir = ''
	filename = ''
	file_type = 1 #1:text, 2:excel
	file_delim = ','
	lineList = []
	__headers = []
	__error = None  # FileErrors class reference holding all errors associated with the current row
	__sample_id_field_names = []

	def __init__(self, filepath, file_type = 1, file_delim = ','):
		self.filepath = filepath
		self.wrkdir = os.path.dirname(os.path.abspath(filepath))
		self.filename = Path(os.path.abspath(filepath)).name
		self.file_type = file_type
		self.file_delim = file_delim
		#headers = self.getHeaders() #self.getRowByNumber(1).split(self.file_delim) #save header of the file to a list
		self.__error = ferr.FileError(self)
		print('----------Init for file {}'.format(self.filename))

	@property
	def headers(self):
		if not self.__headers:
			self.getHeaders()
		return self.__headers

	# @headers.setter
	# def headers(self, value):
	# 	self.__headers = value

	@property
	def error(self):
		return self.__error

	@error.setter
	def error(self, value):
		self.__error = value

	@property
	def sample_id_field_names(self):
		return self.__sample_id_field_names

	@sample_id_field_names.setter
	def sample_id_field_names(self, value):
		self.__sample_id_field_names = value

	def getFileContent (self):
		if not self.lineList:
			fl = open(self.filepath, "r")
			self.lineList = [line.rstrip('\n') for line in fl]
			fl.close()
		return self.lineList

	def getHeaders (self):
		if not self.__headers:
			self.__headers = self.getRowByNumber(1).split(self.file_delim)
		return self.__headers

	def getRowByNumber (self, rownum):
		lineList = self.getFileContent()
		#check that requested row is withing available records of the file and >0
		if len(lineList) >= rownum and rownum > 0:
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
	config_items = {}
	config_items_populated = False
	key_value_delim = None
	line_comment_sign = None

	def __init__(self, filepath, file_type=1, key_value_delim=':', line_comment_sign='##'):
		self.key_value_delim = key_value_delim
		self.line_comment_sign = line_comment_sign
		File.__init__(self, filepath, file_type, self.key_value_delim)
		self.loadConfigSettings()

	#loads config setting assuming that it is a dictionary ==> key: value
	#to identify all key/value pairs, it will split based on ":" for 1 delimiter only and will keep the rest as value of the key
	#any text after "##" will be considered a comment and will be ignored
	def loadConfigSettings(self):
		if not self.config_items_populated:
			lns = File.getFileContent(self)

			for l in lns:
				#values = l.split(self.file_delim, 1) #use only first delimiter - deprecated code

				#print(l.split(self.line_comment_sign, 1)[0])
				values = l.split(self.line_comment_sign, 1)[0].split(self.file_delim, 1)  # use only first delimiter

				#print(values)
				if len(values) >= 2:
					#add items to a dictionary
					self.config_items[values[0].strip()] = values[1].strip()
			self.config_items_populated = True

		return self.config_items

	def getItemByKey(self, item_key):
		#check if config file was already loaded
		if not self.config_items:
			self.loadConfigSettings()
		#check if requested key exists
		#print('item_key = {}'.format(item_key))
		#print('self.config_items.get(item_key, None) => {}'.format(self.config_items.get(item_key, None)))
		#print('item_key in self.config_items = {}'.format(item_key in self.config_items))
		if item_key in self.config_items:
			#print('Item Exists')
			return self.config_items[item_key]
		else:
			#print('Item Does Not Exist')
			return None

	def getAllItems(self, item_key):
		if not self.config_items:
			self.loadConfigSettings()
		return self.config_items

#metadata text file class
class MetaFileText(File):
	cfg_file = None
	file_dict = OrderedDict()
	rows = OrderedDict()

	#read headers of the file and create a dictionary for it
	#dictionary for creating files should preserve columns order
	#dictionary to be submitted to DB has to be sorted alphabetically
	def getFileDictionary(self, sort = False, sort_by_field = ''):

		dict = OrderedDict()

		cfg = self.getConfigInfo()#get reference to config info class
		fields = cfg.getItemByKey('dict_tmpl_fields_node')#get name of the node in dictionary holding array of fields

		if not self.file_dict:
			dict = eval(cfg.getItemByKey('dict_tmpl')) #{fields:[]}
			fld_dict_tmp = dict[fields][0] #eval(cfg.getItemByKey('dict_field_tmpl'))
			dict[fields].clear()

			if dict:
				hdrs = self.getRowByNumber(1).split(self.file_delim)
				upd_flds = cfg.getItemByKey('dict_field_tmpl_update_fields').split(self.configValueListSeparator())

				for hdr in hdrs:
					fld_dict = fld_dict_tmp.copy()
					for upd_fld in upd_flds:
						fld_dict[upd_fld] = hdr
					dict[fields].append(fld_dict)

				self.file_dict = dict

		dict = self.file_dict

		#sort dictionary if requested
		if sort:
			#identify name of the field to apply sorting on the dictionary
			if len(sort_by_field) == 0 or not sort_by_field in dict[fields][0]:
				sort_by_field = cfg.getItemByKey('dict_field_sort_by')
				if len(sort_by_field)== 0:
					sort_by_field = 'name' #hardcoded default

			#apply sorting, if given field name present in the dictionary structure
			if sort_by_field in dict[fields][0]:
				dict[fields] = sorted(dict[fields], key=lambda i: i[sort_by_field])

		return dict

	def getFileDictionary_JSON (self, sort = False, sort_by_field = ''):
		dict = self.getFileDictionary(sort, sort_by_field) #get dictionary object for the file dictionary
		return json.dumps(dict) #convert received dictionary to JSON

	def getConfigInfo(self, cfg_file_name = 'config.cfg', wrkdir = ''):
		# print('file name through Error object - beginning getConfigInfo() = {}'.format(self.error.entity.filepath))
		if not self.cfg_file:
			if wrkdir == "":
				wrkdir = self.wrkdir
			cfg_path = Path(wrkdir) / cfg_file_name
			#print('cfg_path = {}'.format(cfg_path))
			self.cfg_file = ConfigFile(cfg_path, 1)  # config file will use ':' by default
		# print('file name through Error object - end getConfigInfo() = {}'.format(self.error.entity.filepath))
		return self.cfg_file

	def configValueListSeparator(self):
		val_delim = self.getConfigInfo().getItemByKey('config_value_list_separator') #read config file to get "value list separator"
		#print ('val_delim = "{}"'.format(val_delim))
		if not val_delim:
			val_delim = ''
		return val_delim if len(val_delim.strip()) > 0 else ',' #if retrieved value is not blank, return it; otherwise return ',' as a default value

	def submitDictionaryToDB(self):
		#TODO: implement
		pass

	#this will convert each row to a JSON ready dictionary based on the headers of the file
	def getFileRow(self, rownum):

		out_dict = {'row':{},'error':None}

		hdrs = self.getRowByNumber(1).split(self.file_delim) #get list of headers
		lst_content = self.getRowByNumber(rownum).split(self.file_delim) #get list of values contained by the row

		# print('file name through Error object - getFileRow() - before Row instance = {}'.format(self.error.entity.filepath))

		row = Row(self, rownum, lst_content, hdrs)
		# print('file name through Error object - getFileRow() - after Row instance = {}'.format(self.error.entity.filepath))
		# print ('self.error.entity (before Row.error assignment) = {}'.format(self.error.entity))
		row.error = ferr.RowErrors(row)
		# print ('self.error.entity (after  Row.error assignment) = {}'.format(self.error.entity))
		# print('file name through Error object - getFileRow() - after Row.error assignment = {}'.format(self.error.entity.filepath))

		if len(hdrs) == len (lst_content):
			self._validateMandatoryFieldsPerRow(row) # validate row for required fields being populated

			#create dictionary of the row, so it can be converted to JSON
			for hdr, cnt in zip(hdrs, lst_content):
				row.row_dict[hdr.strip()] = cnt.strip()
		else:
			row.row_dict = None
			row.error.addError('Incorrect number of fields! The row contains {} field(s), while {} headers are present.'.format(len (lst_content), len(hdrs)))

		return row #out_dict

	def _validateMandatoryFieldsPerRow(self, row):
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()
		mandatFields = cfg.getItemByKey('file_row_mandatory_fields').split(delim)
		mandatMethod = cfg.getItemByKey('file_row_mandatory_fields_id_method').split(delim)[0].strip()
		out_val = 0 #keeps number of found validation errors
		mandatFieldUsed = []
		mandatFieldMissed = []

		#validate every field of the row to make sure that all mandatory fields are populated
		i = 0 #keeps field count
		for hdr, cnt in zip(row.header, row.row_content):
			i += 1
			for mf in mandatFields:
				#TODO create custom type to store possible values of mandatMethod
				if mandatMethod == 'name':
					chk_val = hdr.strip()
				elif mandatMethod == 'number':
					chk_val = i
				else:
					chk_val = None

				if str(chk_val) == mf.strip():
					mandatFieldUsed.append(mf.strip())
					if len(cnt.strip()) == 0:
						#report error for mandatory field being empty
						row.error.addError('Row #{}. Mandatory field "{}" (column #{}) has no value provided.'.format(row.row_number, hdr, i))
						out_val +=1 #increase number of found errors

		return out_val

	def _verify_id_method (self, method, process_verified_desc = 'Unknown'):
		if not method in FieldIdMethod.field_id_methods:
			# incorrect method was provided
			self.error.addError('Configuration issue - unexpected identification method "{}" was provided for "{}". Expected methods are: {}'
								.format(method, process_verified_desc, ', '.join(FieldIdMethod.field_id_methods)))

	def _verify_field_id_type_vs_method (self, method, fields, process_verified_desc = 'Unknown'):
		if method in FieldIdMethod.field_id_methods:
			if method == FieldIdMethod.number: # 'number'
				#check that all provided fields are numbers
				for f in fields:
					if not f.isnumeric():
						# report error
						self.error.addError(
							'Configuration issue - provided value "{}" for a field number of "{}" is not numeric while the declared method is "{}".'
							.format(f, process_verified_desc, method))


	def _validateMandatoryFieldsExist(self):
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()
		mandatFields = cfg.getItemByKey('file_row_mandatory_fields').split(delim)
		mandatMethod = cfg.getItemByKey('file_row_mandatory_fields_id_method').split(delim)[0].strip()
		#out_val = 0  # keeps number of found validation errors
		mandatFieldUsed = []
		mandatFieldMissed = []

		self._verify_id_method(mandatMethod, 'file_row_mandatory_fields_id_method')
		self._verify_field_id_type_vs_method(mandatMethod, mandatFields, 'file_row_mandatory_fields')

		hdrs = self.headers #self.getHeaders()
		#print('>>>>>>>>>>>>>>>>>fileName through file object = {}'.format(self.filename))
		err = self.error #reference to the FileError class of this file
		#print('>>>>>>>>>>>>>>>>>fileName through error object = {}'.format(err.entity.filename))

		i = 0  # keeps field count
		for hdr in hdrs:
			i += 1
			for mf in mandatFields:
				# TODO create custom type to store possible values of mandatMethod
				#check mandatory fields
				if mandatMethod == FieldIdMethod.name:  # 'name':
					hdr_val = hdr.strip()
				elif mandatMethod == FieldIdMethod.number: # 'number':
					hdr_val = i
				else:
					hdr_val = None

				if str(hdr_val) == mf.strip():
					mandatFieldUsed.append(mf.strip())

		if len(mandatFields) != len(mandatFieldUsed):
			for mf in mandatFields:
				#print ('mandatory field (by {}) = {}'.format(mandatMethod, mf))
				if not mf.strip() in mandatFieldUsed:
					mandatFieldMissed.append(mf.strip())
			# report error for absent mandatory field
			# print('File ERROR reported!!!')
			err.addError('File {}. Mandatory field {}(s): {} - was(were) not found in the file.'.format(self.filename, mandatMethod,','.join(mandatFieldMissed)))
			# print ('File Error Content: {}'.format(err.getErrorsToStr()))

	def _validateSampleIDFields(self):
		cfg = self.getConfigInfo()
		delim = self.configValueListSeparator()
		sampleIdFields = cfg.getItemByKey('sample_id_fields').split(delim)
		sampleIdMethod = cfg.getItemByKey('sample_id_method').split(delim)[0].strip()
		samlpeIdFieldUsed = []
		samlpeIdFieldMissed = []

		hdrs = self.headers # self.getHeaders()
		err = self.error # reference to the FileError class of this file

		self._verify_id_method(sampleIdMethod, 'sample_id_method')
		self._verify_field_id_type_vs_method(sampleIdMethod, sampleIdFields, 'sample_id_fields')

		i = 0  # keeps field count
		for hdr in hdrs:
			i += 1
			for sf in sampleIdFields:
				# check sample_id fields
				if sampleIdMethod == FieldIdMethod.name:  # self.field_id_methods[0]: # 'name':
					smp_val = hdr.strip()
				elif sampleIdMethod == FieldIdMethod.number: # self.field_id_methods[1]: # 'number':
					smp_val = i
				else:
					smp_val = None

				if str(smp_val) == sf.strip():
					samlpeIdFieldUsed.append(sf.strip())

		if len(sampleIdFields) != len(samlpeIdFieldUsed):
			for sf in sampleIdFields:
				if not sf.strip() in samlpeIdFieldUsed:
					# print('Missed Sample ID field = {}'.format(sf.strip()))
					samlpeIdFieldMissed.append(sf.strip())
			err.addError('File {}. Sample ID field {}(s): {} - was(were) not found in the file.'.format(self.filename, sampleIdMethod,','.join(samlpeIdFieldMissed)))

	def processFile(self):
		# print('file name through Error object - inside processFile() before validateMandatoryFieldsExist = {}'.format(self.error.entity.filepath)) #self.file.error.entity.filepath
		self._validateMandatoryFieldsExist()
		self._validateSampleIDFields()

		if self.error.errorsExist():
			# report file level errors
			print('=====>>>> File ERROR reported!!!')
			print ('File Error Content: {}'.format(self.error.getErrorsToStr()))
		# print('file name through Error object - inside processFile() after validateMandatoryFieldsExist = {}'.format(self.error.entity.filepath))

		numRows = self.rowsCount()
		for i in range(1, numRows):
			row = self.getFileRow(i+1)
			self.rows[row.row_number] = row #add Row class reference to the list of all rows

			if not row.error.errorsExist():
				#TODO: Implement action to save good records to DB
				print ('No Errors, Row Info: {}'.format (row.toJSON()))
			else:
				# TODO: Implement action to save error records to log files
				#print ('Errors Present: {}, Row Info: {}'.format(row.error.getErrors(), row.row_content))
				print ('Errors Present: {}, Row Info: {}'.format(row.error.getErrorsToStr(), row.row_content))

		# summary of errors in file
		print ('------> Summary of errors for file {}'.format(self.filename))
		print('Summary of File level errors: {}'.format(self.error.getErrorsToStr())) if self.error.errorsExist() else print('No File level errors!')

		row_err_cnt = self.error.rowErrorsCount()
		if row_err_cnt == 0:
			print('No Row level errors found for this file!')
		else:
			for d in self.rows.values():
				if (d.error.errorsExist()):
					print ('Row level error: {}'.format(d.error.getErrorsToStr()))

	# it will submit given row to DB using config settings
	def submitSampleToDB(self, row):
		# TODO: implement
		pass

class Row ():
	file = None #reference to the file object that this row belongs to
	row_number = None #row number - header = row #1, next line #2, etc.
	row_content = [] #list of values from a file for this row
	_row_dict = None #OrderedDict()
	header = [] #list of values from a file for the first row (headers)
	__error = None #RowErrors class reference holding all errors associated with the current row
	__sample_id = None #it stores a sample Id value for the row.

	def __init__(self, file, row_num, row_content, header):
		self.file = file
		self.row_number = row_num
		self.row_content = row_content
		self.header = header
		self.row_dict = OrderedDict()

	@property
	def samlpe_id(self):
		return self.__sample_id

	@samlpe_id.setter
	def row_dict(self, value):
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
		#print ('From withing toJSON - Dictionary source:{}'.format(self.row_dict))
		return json.dumps(self.row_dict)



#if executed by itself, do the following
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

	data_folder = Path("E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/study01")
	#print (data_folder)
	file_to_open = data_folder / "test1.txt"
	#print (file_to_open)

	#read file
	fl = File(file_to_open)
	#fl = ConfigFile(file_to_open)

	#fl.readFileLineByLine()

	printL (fl.getFileContent())
	printL('Headers===> {}'.format(fl.headers)) #getHeaders()
	printL(fl.getRowByNumber(3))
	printL (fl.getRowByNumber(2))
	printL(fl.getRowByNumber(1))


	#read config file
	file_to_open = data_folder / "config.cfg"
	fl = ConfigFile(file_to_open, 1) #config file will use ':' by default
	#fl.loadConfigSettings()

	print('Setting12 = {}'.format(fl.getItemByKey('Setting12')))

	#sys.exit()

	#read metafile #1
	file_to_open = data_folder / "test1.txt"
	fl1 = MetaFileText(file_to_open)
	#print('file name through Error object = {}'.format(fl1.error.entity.filepath))
	#print(isinstance(fl1.error.entity, MetaFileText), isinstance(fl1.error.entity, ConfigFile))
	#print ('Testing "getFileRow (row_dict)" => {}'.format(fl1.getFileRow(2).row_dict))
	#print ('Testing "getFileRow.toJSON" => {}'.format(fl1.getFileRow(2).toJSON()))
	#print(isinstance(fl1.error.entity, MetaFileText), isinstance(fl1.error.entity, ConfigFile))
	print('Process metafile: {}'.format(fl1.filename))
	fl1.processFile()

	# sys.exit()

	# read metafile #2
	file_to_open = data_folder / "test2.txt"
	fl = MetaFileText(file_to_open)
	print('Process metafile: {}'.format(fl.filename))
	fl.processFile()

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


	sys.exit()#=============================

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





