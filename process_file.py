#import pyodbc
import sys
import os
from pathlib import Path
import xlrd #installation: pip install xlrd
import json
from collections import OrderedDict

def printL (m):
	if __name__ == '__main__':
		print (m)

#Text file class (used as a base)
class File:
	filepath = ''
	wrkdir = ''
	file_type = 1 #1:text, 2:excel
	file_delim = ','
	lineList = []

	def __init__(self, filepath, file_type = 1, file_delim = ','):
		self.filepath = filepath
		self.wrkdir = os.path.dirname(os.path.abspath(filepath))
		self.file_type = file_type
		self.file_delim = file_delim

	def GetFileContent (self):
		if not self.lineList:
			fl = open(self.filepath, "r")
			self.lineList = [line.rstrip('\n') for line in fl]
			fl.close()
		return self.lineList

	def GetHeaders (self):
		return self.GetRowByNumber (1)

	def GetRowByNumber (self, rownum):
		lineList = self.GetFileContent()
		#check that requested row is withing available records of the file and >0
		if len(lineList) >= rownum and rownum > 0:
			return lineList[rownum-1]
		else:
			return ""

	def RowsCount(self, excludeHeader = False):
		num = len(self.GetFileContent())
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
			lns = File.GetFileContent(self)

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
			loadConfigSettings()
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
			loadConfigSettings()
		return self.config_items

#metadata text file class
class MetaFileText(File):
	cfg_file = None
	file_dict = OrderedDict()

	#read headers of the file and create a dictionary for it
	#dictionary for creating files should preserve columns order
	#dictionary to be submitted to DB has to be sorted alphabetically
	def getFileDictionary(self, sort = False, sort_by_field = ''):

		dict = OrderedDict()

		cfg = self.getConfigInfo()#get reference to config info class
		fields = cfg.getItemByKey('dict_tmpl_fields_node')#get name of the node in dictionary holding array of fields

		if not self.file_dict:
			dict = eval(cfg.getItemByKey('dict_tmpl')) #{fields:[]}
			#print('Dictionary Step1=> {}'.format(dict))
			if dict:
				hdrs = self.GetRowByNumber(1).split(self.file_delim)
				fld_dict_tmp = eval(cfg.getItemByKey('dict_field_tmpl'))
				#print('Dictionary Step2.0=> {}'.format(fld_dict_tmp))
				upd_flds = cfg.getItemByKey('dict_field_tmpl_update_fields').split(',')
				#print('upd_flds => {}'.format(upd_flds))

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
				#print('cfg.getItemByKey("dict_field_sort_by") = {}'.format(cfg.getItemByKey('dict_field_sort_by')))
				sort_by_field = cfg.getItemByKey('dict_field_sort_by')
				#print('sort_by_field #2 ===>>>> {}'.format(len(sort_by_field)))
				if len(sort_by_field)== 0:
					sort_by_field = 'name' #hardcoded default

			#print('sort_by_field = {}'.format(sort_by_field))

			#apply sorting, if given field name present in the dictionary structure
			if sort_by_field in dict[fields][0]:
				#print('Inside SORT IF; sorting by {} ======>'.format(sort_by_field))
				#print(dict[fields])
				#print("name" in dict[fields][0])
				#update fields element of dictionary with the sorted list of dictionaries
				dict[fields] = sorted(dict[fields], key=lambda i: i[sort_by_field])

		return dict

	def getFileDictionary_JSON (self, sort = False, sort_by_field = ''):
		dict = self.getFileDictionary(sort, sort_by_field) #get dictionary object for the file dictionary
		return json.dumps(dict) #convert received dictionary to JSON

	def getConfigInfo(self, cfg_file_name = 'config.cfg', wrkdir = ''):
		if not self.cfg_file:
			if wrkdir == "":
				wrkdir = self.wrkdir
			cfg_path = Path(wrkdir) / cfg_file_name
			#print('cfg_path = {}'.format(cfg_path))
			self.cfg_file = ConfigFile(cfg_path, 1)  # config file will use ':' by default
		return self.cfg_file

	def submitDictionaryToDB(self):
		#TODO: implement
		pass

	#this will convert each row to a JSON ready dictionary based on the headers of the file
	def getFileRow(self, rownum):

		#cfg = self.getConfigInfo() #get configuration
		#fields = cfg.getItemByKey('dict_tmpl_fields_node')  # get name of the node in dictionary holding array of fields
		row_dict = OrderedDict()
		out_dict = {'row':{},'error':None}

		hdrs = self.GetRowByNumber(1).split(self.file_delim) #get list of headers
		lst_content = self.GetRowByNumber(rownum).split(self.file_delim) #get list of values contained by the row

		if len(hdrs) == len (lst_content):
			for hdr, cnt in zip(hdrs, lst_content):
				#TODO: validate row for required fields
				row_dict[hdr.strip()] = cnt.strip()
		else:
			row_dict = None
			err = RowError(self, rownum, lst_content, hdrs)
			err.addError('Incorrect number of fields!')
			#err.addError('Additional Error for the same line') #test error
			out_dict['error'] = err

		out_dict['row'] = row_dict
		#print (row_dict)
		#print (json.dumps(row_dict))
		return out_dict #row_dict

	#TODO: is this method needed?
	def getFileRow_JSON(self, rownum):
		return json.dumps(self.getFileRow(rownum)['row'])

	def processFile(self):
		numRows = self.RowsCount()
		for i in range(1, numRows):
			row = self.getFileRow(i+1)
			if not row['error']:
				#TODO: Implement action to save good records to DB
				print ('No Errors, Row Info: {}'.format (json.dumps(row['row'])))
			else:
				# TODO: Implement action to save error records to log files
				print ('Errors Present: {}, Row Info: {}'.format(row['error'].getErrors(), row['row']))

	#it will validate provided row against collected config settings
	def validateSample(self, row):
		# TODO: implement
		pass

	# it will submit given row to DB using config settings
	def submitSampleToDB(self, row):
		# TODO: implement
		pass

class RowError():
	file = None
	#file_path = ''
	row_number = None
	row = None
	header = None
	errors = []

	def __init__(self, file, row_num, row, header):
		self.file = file
		self.row_number = row_num
		self.row = row
		self.header = header
		#self.file_path = file.filepath
		#print ('File passed to Error: {}'.format(file.filepath))

	def addError(self, error_desc ):
		error = {
			'error_desc' : error_desc
		}
		self.errors.append(error)

	def getErrors(self):
		error = {
			'file':  str(self.file.filepath),
			'row_number':self.row_number,
			'row':self.row,
			'header':self.header,
			'errors':self.errors
		}
		return error
		#print('File generated error = {}'.format(self.file.filepath))
		#print (self.errors)

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

	data_folder = Path("E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/LoadMetadataFromFile/study01")
	#print (data_folder)
	file_to_open = data_folder / "test1.txt"
	#print (file_to_open)

	#read file
	fl = File(file_to_open)
	#fl = ConfigFile(file_to_open)

	#fl.readFileLineByLine()

	printL (fl.GetFileContent())
	printL('Headers===> {}'.format(fl.GetHeaders()))
	printL(fl.GetRowByNumber(3))
	printL (fl.GetRowByNumber(2))
	printL(fl.GetRowByNumber(1))


	#read config file
	file_to_open = data_folder / "config.cfg"
	fl = ConfigFile(file_to_open, 1) #config file will use ':' by default
	#fl.loadConfigSettings()

	print('Setting12 = {}'.format(fl.getItemByKey('Setting12')))


	#sys.exit()

	#read metafile #1
	file_to_open = data_folder / "test1.txt"
	fl = MetaFileText(file_to_open)
	print ('Testing "getFileRow" => {}'.format(fl.getFileRow(2)))
	print ('Testing "getFileRow_JSON" => {}'.format(fl.getFileRow_JSON(2)))
	fl.processFile()

	# read metafile #2
	file_to_open = data_folder / "test2.txt"
	fl = MetaFileText(file_to_open)
	fl.processFile()

	'''
	dict_json = fl.getFileDictionary_JSON()
	print ('Not Sorted=================>')
	print (dict_json)
	dict_json_sorted = fl.getFileDictionary_JSON(True, "type")
	print ('Sorted by "type" field=================>')
	print (dict_json_sorted)
	dict_json_sorted = fl.getFileDictionary_JSON(True)
	print ('Sorted by default field=================>')
	print (dict_json_sorted)
	'''


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





