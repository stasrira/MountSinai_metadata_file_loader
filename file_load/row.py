import sys
import traceback
import json
from collections import OrderedDict
from .file_utils import FieldIdMethod


class Row:
    file = None  # reference to the file object that this row belongs to
    row_number = None  # row number - header = row #1, next line #2, etc.
    row_content = None  # [] #list of values from a file for this row
    _row_dict = None  # OrderedDict() of a headers
    header = None  # [] #list of values from a file for the first row (headers)
    __error = None  # RowErrors class reference holding all errors associated with the current row
    __sample_id = None  # it stores a sample Id value for the row.

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
    def row_dict(self):
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

    def to_json(self):
        # print ('From withing to_json - Dictionary source:{}'.format(self.row_dict))
        return json.dumps(self.row_dict)

    def to_str(self):
        row = {
            'file': str(self.file.filepath),
            'row_number': self.row_number,
            'sample_id': self.sample_id,
            'row_JSON': self.to_json(),
            'errors': self.error.errors
        }
        return row

    def assign_sample_id(self):
        self.file.logger.debug('Row #{}. Assigning sample id value.'.format(self.row_number))

        cfg = self.file.get_config_info()
        delim = self.file.config_value_list_separator()

        # retrieve config values for sample id retrieval
        sid = cfg.get_item_by_key('sample_id_expression').strip()
        fields = cfg.get_item_by_key('sample_id_fields').split(delim)
        method = cfg.get_item_by_key('sample_id_method').strip()  # split(delim)[0].
        try:
            sid_eval_req = eval(cfg.get_item_by_key('sample_id_eval_required'))
        except Exception:
            sid_eval_req = False

        for sf in fields:
            i = 0  # keeps field count
            for hdr, cnt in zip(self.header, self.row_content):
                i += 1
                # check sample_id fields
                if method == FieldIdMethod.name:  # self.field_id_methods[0]: # 'name':
                    smp_val = hdr.strip()
                elif method == FieldIdMethod.number:  # self.field_id_methods[1]: # 'number':
                    smp_val = i
                else:
                    smp_val = None

                if str(smp_val) == sf.strip():
                    # if match is found proceed here
                    cnt = str(cnt).strip()
                    if sid_eval_req:
                        # if evaluation of the expression is required, proceed here.
                        try:
                            evl_cnt = eval(cnt)
                            # this tests only values that can be evaluated as a number and after evaluation != source
                            if evl_cnt != cnt and str(evl_cnt).isdigit():
                                cnt = '"' + cnt + '"'
                        except Exception:
                            # ignore errors raised during evaluation of the cnt, this can be a case for strings
                            pass

                    # insert cnt value into sid expression
                    sid = sid.replace('{{{}}}'.format(str(smp_val)), cnt)
                    # sid = sid.replace('{{{}}}'.format(str(smp_val)), str(cnt).strip())

        self.file.logger.debug('Row #{}. Expression for sample id: "{}"; evaluation flag is set to "{}".'.format(self.row_number, sid, sid_eval_req))
        if sid_eval_req:
            #proceed here if evaluation is required
            try:
                smp_evaled = eval(sid)  # attempt to evaluate expression for sample id
            except Exception as ex:
                # report an error if evaluation has failed.
                _str = 'Error "{}" occurred during evaluating sample id expression: {}\n{} '.format(ex, sid,
                                                                                                    traceback.format_exc())
                self.error.add_error(_str)
                self.file.logger.error(_str)
                self.file.logger.debug(sys.exc_info()[1])
                smp_evaled = ''
        else:
            smp_evaled = sid

        self.__sample_id = str(smp_evaled).strip()
        return self.__sample_id
