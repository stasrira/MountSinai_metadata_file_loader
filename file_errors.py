class Error():
	__desc = None
	__num = None

	def __init__(self, err_desc, err_num = None):
		self.__desc = err_desc
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
	errors = None

	def __init__(self, new_entity):
		self.entity = new_entity
		self.__errors = []

	def addError(self, error_desc, error_number = None):
		error = Error(error_desc, error_number)
		self.__errors.append(error)

	def errorsExist(self):
		return (len(self.__errors) > 0)

	def getErrors(self):
		return self.__errors

class FileError(EntityErrors):

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

	def getErrorsToStr(self):
		err_lst = []
		for er in self.getErrors(): # EntityErrors.getErrors
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
