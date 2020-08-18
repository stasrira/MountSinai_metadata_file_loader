from pathlib import Path
import json
from collections import OrderedDict
from file_load.file_error import RowError
from utils import MetadataDB, common2 as cm2  # database connectivity related class
from utils.log_utils import deactivate_logger_common
from utils import global_const as gc
from .file import File
from .file_utils import StudyConfig, FieldIdMethod, load_configuration, setup_common_basic_file_parameters
from .row import Row


# metadata text file class
class MetaFileText(File):
    def __init__(self, filepath, cfg_path='', file_type=None, file_delim=None):
        # setup default parameters
        if not file_type:
            file_type = 1
        if not file_delim:
            file_delim = ','

        File.__init__(self, filepath, file_type, file_delim)

        self.db_response_alerts = None  # keeps list of notifications form DB submissions that returned not OK status
        self.db_submitted_count = 0 #keeps count of submitted to DB rows
        self.cfg_file = None
        self.file_dict = None
        self.rows = None

        self.logger = self.setup_logger(self.wrkdir, self.filename)
        self.logger.info('Start working with file {}'.format(filepath))

        self.logger.info('Loading config file.')
        # identify name of the config file for a study
        if len(cfg_path) == 0:
            cfg_path = Path(self.wrkdir) / gc.DEFAULT_STUDY_CONFIG_FILE

        if self.file_exists(cfg_path):
            load_configuration(self, cfg_path)
            self.cfg_file = StudyConfig.config_loc
            self.file_dict = OrderedDict()
            self.rows = OrderedDict()

            setup_common_basic_file_parameters(self)
            """
            replace_blanks_in_header = self.cfg_file.get_item_by_key('replace_blanks_in_header')
            # set parameter to True or False, if it was set likewise in the config, otherwise keep the default value
            if replace_blanks_in_header.lower() in ['true', 'yes']:
                self.replace_blanks_in_header = True
            if replace_blanks_in_header.lower() in ['false', 'no']:
                self.replace_blanks_in_header = False
            header_row_num = self.cfg_file.get_item_by_key('header_row_number')
            if header_row_num and header_row_num.isnumeric():
                self.header_row_num = int(header_row_num)
            """
        else:
            _str = 'Study configuration file "{}" does not exist, configuration loading was aborted.'.format(cfg_path)
            self.error.add_error(_str)
            self.logger.error(_str)

    def get_file_dictionary(self, sort=None, sort_by_field=None):
        if not sort:
            sort = False
        if not sort_by_field:
            sort_by_field = ''

        # get configuration object reference
        cfg = self.get_config_info()
        # get file's headers
        hdrs = self.get_headers()

        return cm2.get_dataset_dictionary (hdrs, cfg.get_all_data(), sort, sort_by_field)

    def get_file_dictionary_json(self, sort=False, sort_by_field=''):
        dict1 = self.get_file_dictionary(sort, sort_by_field)  # get dictionary object for the file dictionary
        return json.dumps(dict1)  # convert received dictionary to JSON

    def get_config_info(self):
        return StudyConfig.config_loc

    def config_value_list_separator(self):
        val_delim = self.get_config_info().get_item_by_key(
            'config_value_list_separator')  # read config file to get "value list separator"
        # self.logger.debug('config_value_list_separator() => val_delim = "{}"'.format(val_delim))
        if not val_delim:
            val_delim = ''
        # if retrieved value is not blank, return it; otherwise return ',' as a default value
        return val_delim if len(val_delim.strip()) > 0 else gc.DEFAULT_CONFIG_VALUE_LIST_SEPARATOR  # ','

    # this will convert each row to a JSON ready dictionary based on the headers of the file
    def get_file_row(self, rownum):

        # out_dict = {'row':{},'error':None}

        hdrs = self.get_headers()  # self.get_row_by_number(1).split(self.file_delim) #get list of headers
        # lst_content = self.get_row_by_number(rownum).split(self.file_delim)  # get list of values contained by the row
        lst_content = self.get_row_by_number_to_list(rownum)

        row = Row(self, rownum, lst_content, hdrs)
        row.error = RowError(row)

        if not row.isempty():
            if len(hdrs) == len(lst_content):
                self._validate_mandatory_fields_per_row(row)  # validate row for required fields being populated

                # create dictionary of the row, so it can be converted to JSON
                for hdr, cnt in zip(hdrs, lst_content):
                    row.row_dict[hdr.strip()] = cnt.strip().replace('\'','\'\'')

                # set sample id for the row
                row.assign_sample_id()
            else:
                row.row_dict = None
                _str = ('Row #{}. Incorrect number of fields! The row contains {} field(s), while {} '
                        'headers are present.').format(rownum, len(lst_content), len(hdrs))
                row.error.add_error(_str)
                self.logger.error(_str)
        else:
            row.row_dict = None
            _str = ('Row #{}. The row is empty and will be ignored.').format(rownum)
            self.logger.warning(_str)

        return row  # out_dict

    def _validate_mandatory_fields_per_row(self, row):
        cfg = self.get_config_info()
        delim = self.config_value_list_separator()
        mandat_fields = cfg.get_item_by_key('mandatory_fields').split(delim)
        mandat_method = cfg.get_item_by_key('mandatory_fields_method').split(delim)[0].strip()
        out_val = 0  # keeps number of found validation errors
        # mandatFieldUsed = []

        # validate every field of the row to make sure that all mandatory fields are populated
        i = 0  # keeps field count
        for hdr, cnt in zip(row.header, row.row_content):
            i += 1
            # identify appropriate mandatory field (mf) for the current header field (hdr)
            for mf in mandat_fields:
                # identify comparision method
                if mandat_method == 'name':
                    chk_val = hdr.strip()
                elif mandat_method == 'number':
                    chk_val = i
                else:
                    chk_val = None

                # proceed if header is matching mandatory field
                if str(chk_val) == mf.strip():
                    # validate if the value of the field for the current row is blank and report error if so
                    if len(cnt.strip()) == 0:
                        out_val += 1  # increase number of found errors
                        # report error for mandatory field being empty
                        _str = 'Row #{}. Mandatory field "{}" (column #{}) has no value provided.'.format(
                            row.row_number, hdr, i)
                        row.error.add_error(_str)
                        self.logger.error(_str)

        return out_val  # return count found validation error

    def _verify_id_method(self, method, process_verified_desc='Unknown'):
        if method not in FieldIdMethod.field_id_methods:
            # incorrect method was provided
            _str = ('Configuration issue - unexpected identification method "{}" was provided for "{}". '
                    'Expected methods are: {}').format(
                method, process_verified_desc, ', '.join(FieldIdMethod.field_id_methods))
            self.error.add_error(_str)
            self.logger.error(_str)

    # this verifies that if method of identificatoin fields set as "number", list of fields contains only numeric values
    def _verify_field_id_type_vs_method(self, method, fields, process_verified_desc='Unknown'):
        if method in FieldIdMethod.field_id_methods:
            if method == FieldIdMethod.number:  # 'number'
                # check that all provided fields are numbers
                for f in fields:
                    if not f.strip().isnumeric():
                        # report error
                        _str = ('Configuration issue - provided value "{}" for a field number of "{}" is not numeric '
                                'while the declared method is "{}".').format(
                            f, process_verified_desc, method)
                        self.error.add_error(_str)
                        self.logger.error(_str)

    def _validate_fields_vs_headers(self, fields_to_check, field_id_method,
                                    fields_to_check_param_name, field_id_method_param_name):
        fields = fields_to_check
        method = field_id_method
        field_used = []
        field_missed = []

        self._verify_id_method(method, fields_to_check_param_name)  # check that provided methods exist
        self._verify_field_id_type_vs_method(method, fields, field_id_method_param_name)  # check field ids vs method

        hdrs = self.headers()  # self.get_headers()

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
                    field_used.append(mf.strip())

        if len(fields) != len(field_used):  # if not all fields from the list were matched to header
            for mf in fields:
                if not mf.strip() in field_used:
                    field_missed.append(mf.strip())
        return field_missed

    # this verifies that all fields passed in the "fields_to_check" list are utilized in the "expression_to_check"
    def _validate_fields_vs_expression(self, fields_to_check, expression_to_check):
        field_missed = []

        for fd in fields_to_check:
            if not '{{{}}}'.format(fd.strip()) in expression_to_check:
                field_missed.append(fd.strip())

        return field_missed

    def _validate_mandatory_fields_exist(self):
        self.logger.info('Validating that all mandatory fields exist.')
        cfg = self.get_config_info()
        delim = self.config_value_list_separator()

        # names of config parameters to get config values
        fields_param_name = 'mandatory_fields'
        method_param_name = 'mandatory_fields_method'

        # retrieve config values
        fields = cfg.get_item_by_key(fields_param_name).split(delim)
        method = cfg.get_item_by_key(method_param_name).split(delim)[0].strip()

        # validated fields against headers of the metafile
        field_missed = self._validate_fields_vs_headers(
            fields, method, fields_param_name, method_param_name)

        if field_missed:
            # report error for absent mandatory field
            _str = 'File {}. Mandatory field {}(s): {} - was(were) not found in the file.'.format(self.filename, method,
                                                                                                  ','.join(
                                                                                                      field_missed))
            self.error.add_error(_str)
            self.logger.error(_str)

    def _validate_sample_id_fields(self):
        self.logger.info('Validating that all fields required for identifying sample id exist.')
        cfg = self.get_config_info()
        delim = self.config_value_list_separator()

        # names of config parameters to get config values
        fields_param_name = 'sample_id_fields'
        method_param_name = 'sample_id_method'
        expr_name = 'sample_id_expression'

        # retrieve config values
        fields = cfg.get_item_by_key(fields_param_name).split(delim)
        method = cfg.get_item_by_key(method_param_name).strip()  # split(delim)[0].
        expr_str = cfg.get_item_by_key(expr_name).strip()

        # validated fields against headers of the metafile
        field_missed = self._validate_fields_vs_headers(
            fields, method, fields_param_name, method_param_name)
        if field_missed:
            # report error if some sample_id component fields do not match header names or numbers (depending on method)
            _str = 'File {}. Sample ID field {}(s): {} - was(were) not found in the file.'.format(self.filename, method,
                                                                                                  ','.join(
                                                                                                      field_missed))
            self.error.add_error(_str)
            self.logger.error(_str)
        else:
            field_missed2 = self._validate_fields_vs_expression(fields, expr_str)
            if field_missed2:
                # report error if some sample_id component fields were not found in the sample_id_expression
                _str = ('Configuration issue - Sample ID field(s) "{}" was(were) not found in the '
                        '"sample_id_expression" parameter - {}.').format(','.join(field_missed2), expr_str)
                self.error.add_error(_str)
                self.logger.error(_str)

    def process_file(self):
        # validate file for "file" level errors (assuming that config file was loaded)
        if self.cfg_file and self.cfg_file.loaded:
            self._validate_mandatory_fields_exist()
            self._validate_sample_id_fields()

        # TODO: validate MDB study_id. If it not set, attempt to create a study.
        # If this process fails, report a File lever error.

        # verify that number of rows in the file bigger than the header row number
        num_rows = self.rows_count()
        if self.header_row_num >= num_rows:
            _str = "There are no records to process since header row (#{}) more than total number " \
                   "of rows in the file - {}.".format(self.header_row_num, num_rows)
            self.error.add_error(_str)
            self.logger.error(_str)

        if self.error.errors_exist():
            # report file level errors to Log and do not process rows
            self.logger.error('File level errors we identified! Aborting the file processing.')
            self.logger.error('Summary of File lever errors: {}'.format(self.error.get_errors_to_str()))
            # print('=====>>>> File ERROR reported!!!')
            # print ('File Error Content: {}'.format(self.error.get_errors_to_str()))
        else:
            # proceed with processing file, if no file-level errors were found
            self.logger.info('Proceeding with processing rows of the file.')
            for i in range(self.header_row_num, num_rows):  # for i in range(1, num_rows):
                self.logger.debug('Processing row #{} out of {}.'.format(i, num_rows))
                row = self.get_file_row(i + 1)
                self.rows[row.row_number] = row  # add Row class reference to the list of all rows

                if not row.error.errors_exist() and not row.isempty():
                    # print ('No Errors - Saving to DB, Row Info: {}'.format (row.to_str()))
                    self.logger.info(
                        'Row #{}. No Row level errors were identified. Saving it to database. Row data: {}'.format(
                            row.row_number, row.to_str()))

                    # increase count of submitted to DB rows
                    self.db_submitted_count += 1

                    mdb = MetadataDB(self.cfg_file)

                    mdb_resp = mdb.submit_row(
                        row.sample_id,
                        row.to_json(),
                        self.get_file_dictionary_json(True),
                        self.filepath,
                        self.logger,
                        row.error
                    )
                    if not row.error.errors_exist():
                        _str = 'Row #{}. Sample Id "{}" was submitted to MDB. Status: {}; Description: {}'.format(
                            row.row_number, row.sample_id, mdb_resp[0][0]['status'], mdb_resp[0][0]['description'])
                        # depending on a status from MDB response, logging entry will be set as Info or Error
                        if mdb_resp[0][0]['status'] == 'OK':
                            self.logger.info(_str)
                        else:
                            self.logger.error(_str)

                        if mdb_resp[0][0]['status'] != 'OK':
                            if not self.db_response_alerts:
                                self.db_response_alerts = []
                            self.db_response_alerts.append(
                                {'sample_id': row.sample_id,
                                 'status': mdb_resp[0][0]['status'],
                                 'description': mdb_resp[0][0]['description']}
                            )

                    else:
                        _str = 'Error occured during submitting sample Id "{}" to MDB. Error details: {}'.format(
                            row.sample_id, row.error.get_errors_to_str())
                        self.logger.error(_str)
                else:
                    if row.error.errors_exist():
                        # report to log file if Row level errros were identified
                        _str = 'Row #{}. Row level errors were identified. Errors: {}; Row data: {}'.format(
                            row.row_number, row.error.get_errors_to_str(), row.row_content)
                        self.logger.error(_str)
                    if row.isempty():
                        _str = 'Row #{}. The row is empty and was not submitted to DB.'.format(row.row_number)
                        self.logger.warning(_str)


        # report error summary of processing the file
        self.logger.info('SUMMARY OF ERRORS ==============>')
        self.logger.info('File level errors: {}'.format(
            self.error.get_errors_to_str())) if self.error.errors_exist() else self.logger.info(
            'No File level errors were identified.')
        # print ('------> Summary of errors for file {}'.format(self.filename))
        # print('Summary of File level errors: {}'.
        # format(self.error.get_errors_to_str())) if self.error.errors_exist() else print('No File level errors!')

        row_err_cnt = self.error.row_errors_count()
        if row_err_cnt == 0:
            # print('No Row level errors found for this file!')
            self.logger.info('No Row level errors were identified.')
        else:
            for d in self.rows.values():
                if d.error.errors_exist():
                    # print ('Row level error: {}'.format(d.error.get_errors_to_str()))
                    self.logger.info('Row level error: {}'.format(d.error.get_errors_to_str()))

        # release the log handler for the current file
        # self.logger.removeHandler(self.log_handler)
        deactivate_logger_common(self.logger, self.log_handler)

