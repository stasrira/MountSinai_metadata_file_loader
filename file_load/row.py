import sys
import traceback
import json
from collections import OrderedDict
from .file_utils import FieldIdMethod

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
                    sid = sid.replace('{{{}}}'.format(str(smp_val)), '"' + cnt + '"')

        self.file.logger.debug ('Row #{}. Expression for sample id evaluation: "{}"'.format(self.row_number, sid))
        try:
            smp_evaled = eval(sid) # attempt to evaluate expression for sample id
        except Exception as ex:
            # report an error if evaluation has failed.
            _str = 'Error "{}" occurred during evaluating sample id expression: {}\n{} '.format(ex, sid, traceback.format_exc())
            self.error.addError(_str)
            self.file.logger.error(_str)
            self.file.logger.debug(sys.exc_info()[1])
            smp_evaled = ''

        # TODO: add validation for duplicated Sample IDs. If duplication is found, reject the row. This rule is to be confirmed

        self.__sample_id = str(smp_evaled).strip()
        return self.__sample_id