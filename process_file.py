#import pyodbc
import sys
import os
from pathlib import Path
import xlrd #installation: pip install xlrd
import json

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
		self.fl = open(self.filepath, "r")
		#print('file delimiter==> {}'.format(self.file_delim))

	def GetFileContent (self):
		#lineList = [line.rstrip('\n') for line in open(self.filepath)]
		if not self.lineList:
			self.lineList = [line.rstrip('\n') for line in self.fl]
		return self.lineList

	def GetHeaders (self):
		#with open(self.filepath, "r") as cur_file:
		#	line = fl.readline().rstrip('\n')
		#return fl.readline().rstrip('\n')
		return self.GetRowByNumber (1)

	def GetRowByNumber (self, rownum):
		#lineList = [line.rstrip('\n') for line in open(self.filepath)]
		lineList = self.GetFileContent()
		if len(lineList) >= rownum and rownum > 0:
			return lineList[rownum-1]
		else:
			return ""

#Config file class
class ConfigFile(File):
	config_items = {}
	config_items_populated = False
	key_value_delim = ':'

	def __init__(self, filepath, file_type=1, file_delim=','):
		File.__init__(self, filepath, file_type, self.key_value_delim)
		self.loadConfigSettings()
	#loads config setting assuming that it is a dictionary ==> key: value
	#to identify all key/value pairs, it will split based on ":" for 1 delimiter only and will keep the rest as value of the key
	def loadConfigSettings(self):
		if not self.config_items_populated:
			lns = File.GetFileContent(self)

			for l in lns:
				values = l.split(self.file_delim, 1) #use only first delimiter

				#print(values)
				#print(len(values))
				if len(values) >= 2:
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

class MetaFileText(File):
	cfg_file = None

	#read headers of the file and create a dictionary for it
	#dictionary for creating files should preserve columns order
	#dictionary to be submitted to DB has to be sorted alphabetically
	def getFileDictionary(self, sort = False, sort_by_field = ''):
		cfg = self.getConfigInfo()
		fields = cfg.getItemByKey('dict_tmpl_fields_node')
		#print (cfg.getItemByKey('dict_tmpl'))
		dict = eval(cfg.getItemByKey('dict_tmpl')) #{fields:[]}
		#print('Dictionary Step1=> {}'.format(dict))
		if dict:
			hdrs = self.GetRowByNumber(1).split(self.file_delim)
			fld_dict_tmp = eval(cfg.getItemByKey('dict_field_tmpl'))
			#print('Dictionary Step2.0=> {}'.format(fld_dict_tmp))
			upd_flds = cfg.getItemByKey('dict_field_tmpl_update_fields').split(',')
			#print('upd_flds => {}'.format(upd_flds))

			for hdr in hdrs:
				#print ('fld_dict_tmp => {}'.format(fld_dict_tmp))
				fld_dict = fld_dict_tmp.copy()
				#print('Dictionary Step2.01=> {}'.format(fld_dict))
				#print('Dictionary Step3 (before append) => {}'.format(dict))
				for upd_fld in upd_flds:
					#print ('Update field: {}, Update value: {}'.format(upd_fld, hdr))
					fld_dict[upd_fld] = hdr
					#print('Dictionary Step2.1 (inside update loop) => {}'.format(fld_dict))
				#print('Dictionary Step2.2 (after field updates) => {}'.format(fld_dict))
				dict[fields].append(fld_dict)
				#print('Dictionary Step3.1 (after append) => {}'.format(dict))
			#sort dictionary if requested

			#print('cfg.getItemByKey("dict_field_sort_by") = {}'.format(cfg.getItemByKey('dict_field_sort_by')))

			if sort:
				if len(sort_by_field) == 0 or not sort_by_field in dict[fields][0]:
					#print('cfg.getItemByKey("dict_field_sort_by") = {}'.format(cfg.getItemByKey('dict_field_sort_by')))
					sort_by_field = cfg.getItemByKey('dict_field_sort_by')
					#print('sort_by_field #2 ===>>>> {}'.format(len(sort_by_field)))
					if len(sort_by_field)== 0:
						sort_by_field = 'name' #hardcoded default

				#print('sort_by_field = {}'.format(sort_by_field))

				if sort_by_field in dict[fields][0]:
					print('Inside SORT IF; sorting by {} ======>'.format(sort_by_field))
					#print(dict[fields])
					#print("name" in dict[fields][0])
					#update fields element of dictionary with the sorted list of dictionaries
					dict[fields] = sorted(dict[fields], key=lambda i: i[sort_by_field])

		#print('Dictionary Final - Step3x => {}'.format(dict))
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

	#this will convert each row of the file to a JSON based on the file headers
	def convertRowsToJSON(self):
		# TODO: implement
		pass

	#collect list of mandatory fields to be verified
	def loadConfigSettings(self):
		# TODO: implement
		pass

	#it will validate provided row against collected config settings
	def validateSample(self, row):
		# TODO: implement
		pass

	# it will submit given row to DB using config settings
	def submitSampleToDB(self, row):
		# TODO: implement
		pass

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

	'''
	#read config file
	file_to_open = data_folder / "config.cfg"
	fl = ConfigFile(file_to_open, 1) #config file will use ':' by default
	fl.loadConfigSettings()

	if fl.config_items: # fl.config_items_populated:
		#print(fl.config_items['Setting2'])
		print(fl.getItemByKey('dict_field_template'))
		print(fl.getItemByKey('dict_template'))
	else:
		print ('No config data loaded!!!!')
	'''

	#read metafile
	file_to_open = data_folder / "test1.txt"
	fl = MetaFileText(file_to_open)
	dict_json = fl.getFileDictionary_JSON()
	print ('Not Sorted=================>')
	print (dict_json)
	dict_json_sorted = fl.getFileDictionary_JSON(True, "type1")
	print ('Sorted by "type1" field=================>')
	print (dict_json_sorted)
	dict_json_sorted = fl.getFileDictionary_JSON(True)
	print ('Sorted by default field=================>')
	print (dict_json_sorted)



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





