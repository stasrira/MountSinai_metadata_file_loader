class Error():
	_desc = None
	_num = None

	def __init__(self, err_desc, err_num = None):
		#print ('=============>>Inside Error class, err_desc = {}'.format(err_desc))
		self._desc = err_desc
		#print ('=============>>Inside Error class, reading back, err_desc = {}'.format(self._desc))
		#print ('=============>>Inside Error class, reading back from property, err_desc = {}'.format(self.error_desc))
		self._num = err_num

	@property
	def error_desc(self):
		return self._desc

	@error_desc.setter
	def error_desc(self, value):
		self._desc = value

	@property
	def error_number(self):
		return self._num

	@error_number.setter
	def error_number(self, value):
		self._num = value

class EntityErrors():
	# _entity = None #link to an object that these errors belongs to.
	_errors = None

	def __init__(self):
		#self.entity = entity
		#print ('Entity Row number From Entity Errors class: {}'.format(self.entity.row_number))
		self._errors = []

	# @property
	# def entity(self):
	# 	return self._entity
	#
	# @entity.setter
	# def entity(self, value):
	# 	self._entity = value

	@property
	def errors(self):
		return self._errors

	@errors.setter
	def errors(self, value):
		self._errors = value

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
		self._errors.append(error)

	def errorsExist(self):
		return (len(self._errors) > 0)

	def getErrors(self):
		#error = {
		#	#'entity': str(self.entity),
		#	'errors': self.errors
		#}
		return self._errors

class FileError(EntityErrors):
	_entity = None #link to an object that these errors belongs to.

	@property
	def entity(self):
		return self._entity

	@entity.setter
	def entity(self, value):
		self._entity = value

	def __init__(self, file):
		EntityErrors.__init__(self)
		self._entity = file

	def getErrors(self):
		return EntityErrors.getErrors(self)

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
	_entity = None  # link to an object that these errors belongs to.

	@property
	def entity(self):
		return self._entity

	@entity.setter
	def entity(self, value):
		self._entity = value

	def __init__(self, row):
		#self.row = row
		#print ('row number from RowErrors class: {}'.format(row.row_number))
		EntityErrors.__init__(self)
		self.entity = row

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
