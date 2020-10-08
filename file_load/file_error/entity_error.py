from .error import Error


class EntityErrors:
    entity = None  # link to an object that these errors belongs to.
    errors = None

    def __init__(self, new_entity):
        self.entity = new_entity
        self.__errors = []

    def add_error(self, error_desc, error_number=None):
        error = Error(error_desc, error_number)
        self.__errors.append(error)

    def errors_exist(self):
        return len(self.__errors) > 0

    def get_errors(self):
        return self.__errors