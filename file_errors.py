class Error():
	__desc = None
	__num = None

	def __init__(self, err_desc, err_num = None):
		#print ('=============>>Inside Error class, err_desc = {}'.format(err_desc))
		self.__desc = err_desc
		#print ('=============>>Inside Error class, reading back, err_desc = {}'.format(self.__desc))
		#print ('=============>>Inside Error class, reading back from property, err_desc = {}'.format(self.error_desc))
		self.__num = err_num

	@property
	def error_desc(self):
		return self.__desc

	@error_desc.setter
	def error_desc(self, value):
		self.__desc = value

	@property
	def error_number(self):
		return self.__num

	@error_number.setter
	def error_number(self, value):
		self.__num = value

class EntityErrors():
	entity = None #link to an object that these errors belongs to.
	__errors = None

	def __init__(self, new_entity):
		self.entity = new_entity
		#print ('Entity Row number From Entity Errors class: {}'.format(self.entity.row_number))
		self.__errors = []

	# @property
	# def entity(self):
	# 	return self.__entity
	#
	# @entity.setter
	# def entity(self, value):
	# 	self.__entity = value

	@property
	def errors(self):
		return self.__errors

	@errors.setter
	def errors(self, value):
		self.__errors = value

	def addError(self, error_desc, error_number = None):
		#print ('------------>Adding request - error_desc: {}'.format(error_desc))
		#TODO: leave only assignment for error1 or error
		#keep error assignment in dictionary
		# error1 = {
		# 	'error_desc': error_desc,
		# 	'error_number': error_number
		# }
		#keep error assignment in class
		error = Error(error_desc, error_number)
		#print ('---------> Assignment to class - error_desc= {}'.format(error.error_desc))
		self.__errors.append(error)

	def errorsExist(self):
		return (len(self.__errors) > 0)

	def getErrors(self):
		return self.__errors

class FileError(EntityErrors):
	#_entity = None #link to an object that these errors belongs to.

	def __init__(self, file):
		# self._entity = file
		# print('File ----> Before assignment EntityErrors.entity = {}'.format(self.entity))
		# print ('EntityErrors id = {}'.format(id(EntityErrors)))
		EntityErrors.__init__(self, file)
		# EntityErrors.entity = file
		# print('File ----> After assignment EntityErrors.entity = {}'.format(self.entity))

	# @property
	# def entity(self):
	# 	# return self._entity
	# 	return EntityErrors.entity
	#
	# @entity.setter
	# def entity(self, value):
	# 	# self._entity = value
	# 	# print('File ----> Before assignment EntityErrors.entity = {}'.format(EntityErrors.entity))
	# 	EntityErrors.entity = value
	# 	# print('File ----> After assignment EntityErrors.entity = {}'.format(EntityErrors.entity))


	def getErrors(self):
		return EntityErrors.getErrors(self)

	def rowErrorsCount (self):
		row_err_cnt = 0
		for d in self.entity.rows.values():
			if (d.error.errorsExist()):
				row_err_cnt += 1
		return row_err_cnt

	def getErrorsToStr(self):
		err_lst = []
		for er in EntityErrors.getErrors(self):
			# print ('er.error_desc = {}'.format(er.error_desc))
			err_lst.append({'error_desc': er.error_desc, 'error_number': er.error_number})

		error = {
			'file': str(self.entity.filepath),
			'header': self.entity.headers,
			'errors': err_lst
		}
		return error

class RowErrors(EntityErrors):
	#_entity = None  # link to an object that these errors belongs to.

	def __init__(self, row):
		# self.row = row
		# print ('row number from RowErrors class: {}'.format(row.row_number))

		# self.entity = row
		# print('Row ----> Before assignment EntityErrors.entity = {}'.format(EntityErrors.entity))
		# print ('EntityErrors id = {}'.format(id(EntityErrors)))
		# EntityErrors.entity = row
		EntityErrors.__init__(self, row)
		# print('Row ----> After assignment EntityErrors.entity = {}'.format(EntityErrors.entity))

	@property
	def entity(self):
		# return self._entity
		return EntityErrors.entity

	@entity.setter
	def entity(self, value):
		# self._entity = value
		# print('Row ----> Before assignment EntityErrors.entity = {}'.format(EntityErrors.entity))
		EntityErrors.entity = value
		# print('Row ----> After assignment EntityErrors.entity = {}'.format(EntityErrors.entity))

	def getErrors(self):
		return EntityErrors.getErrors(self)

	def getErrorsToStr(self):
		err_lst = []
		for er in EntityErrors.getErrors(self):
			#print ('er.error_desc = {}'.format(er.error_desc))
			err_lst.append({'error_desc':er.error_desc, 'error_number':er.error_number})

		error = {
			'file': str(self.entity.file.filepath),
			'row_number':self.entity.row_number,
			'row':self.entity.row_content,
			'header':self.entity.header,
			'errors': err_lst #EntityErrors.getErrors(self)
		}
		return error
