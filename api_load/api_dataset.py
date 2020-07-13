import traceback
import json
import pandas as pd
from utils import common as cm, common2 as cm2, MetadataDB

class ApiDataset():
    def __init__(self, api_dataset_str, cfg_obj, log_obj, err_obj, api_process_name):
        self.cfg = cfg_obj
        self.logger = log_obj
        self.error = err_obj
        self.api_process_name = api_process_name

        self.loaded = False

        self.api_dataset_str = api_dataset_str
        self.validation_rules = None
        self.validation_alerts = None  # stores all generated validation alerts to be reported to users through email
        self.db_response_alerts = None  # stores all generated response alerts to be reported to users through email
        self.dataset_dictionary = None

        try:
            # load received json into a data frame object
            self.ds = pd.read_json(api_dataset_str)
        except Exception as ex:
            _str = 'Error "{}" occurred while converting API output string value "{}" to a python pandas dataframe object.\n{} ' \
                .format(ex, api_dataset_str, traceback.format_exc())
            self.logger.error(_str)
            self.error.add_error(_str)

        # check if unpivoting is required
        self.unpivot_cfg = self.cfg.get_value('DATA/transform/unpivot')
        if self.unpivot_cfg and self.unpivot_cfg['apply']:
            self.unpivot_dataset()

        # print(self.ds)

        self.prepare_dataset_validation_columns()
        # print(self.ds)

        # set dataframe option, so it prints all rows
        pd.set_option('display.max_rows', None)

        # report records that do not satisfy validation rules
        for valid_rule in self.validation_rules:
            if len(self.ds) > 0:
                ds_failed = self.ds[self.ds[valid_rule['name']] == False]
                # check if failed validation rule has to be reported
                if 'report_failure' in valid_rule.keys() and valid_rule['report_failure']:
                    if 'report_failure_details' in valid_rule.keys() and valid_rule['report_failure_details']:
                        # proceed here if details of failed validation rules have to be reported
                        _str = 'Dataset validation failed for {} record(s). Error message: "{}". ' \
                               'Data extract for affected rows:\n{}'\
                            .format(ds_failed.shape[0], valid_rule['message'],
                                    ds_failed[valid_rule['report_columns']]
                                    if 'report_columns' in valid_rule.keys()
                                    else 'No columns to display were provided for the current rule...')
                        df_html = ds_failed[valid_rule['report_columns']].to_html()
                        df_html = df_html.replace('<table', '<table style="color: red"')
                        _str_html = 'Dataset validation <font color="red">failed for {}</font> record(s). ' \
                                    'Error message: "<font color="red">{}</font>".' \
                                    '<br/>Data extract for affected rows:<br/>{}' \
                            .format(ds_failed.shape[0], valid_rule['message'],
                                    df_html  # ds_failed[valid_rule['report_columns']].to_html()
                                    if 'report_columns' in valid_rule.keys()
                                    else 'No columns to display were provided for the current rule...')
                    else:
                        # proceed here if only summary of failed validation rules have to be reported
                        _str = 'Dataset validation failed for {} record(s). Error message: "{}". ' \
                            .format(ds_failed.shape[0], valid_rule['message'])
                        _str_html = 'Dataset validation <font color="red">failed for {}</font> record(s). ' \
                                    'Error message: "<font color="red">{}</font>". ' \
                            .format(ds_failed.shape[0], valid_rule['message'])

                    self.logger.warning(_str)
                    # add validation alert to the validation alert list
                    if not self.validation_alerts:
                        self.validation_alerts = []
                    self.validation_alerts.append({'text': _str, 'html': _str_html})
            else:
                break

        # select only records that satisfy all validation rules
        for valid_rule in self.validation_rules:
            # filter out records that do not satisfy the current validation rule
            self.ds = self.ds[self.ds[valid_rule['name']] == True]  # TODO: Must be un-commented!!!!

        # print (self.ds)

        # check that final dataset has some rows
        if len(self.ds) > 0:
            self.loaded = True
        else:
            self.logger.warning('The dataset contains no valid records to be processed.')

    def submit_rows_to_db(self):
        # select only columns that has to be reported to MDB
        columns_to_db = self.cfg.get_value('DATA/output_dataset/columns_to_db')
        if columns_to_db:
            # if columns_to_db are provided, filter dataset based on the list of given columns
            ds_db = self.ds[columns_to_db]
        else:
            ds_db = self.ds

        sample_id_to_db = self.cfg.get_value('DATA/output_dataset/sample_id')

        # verify that sample id was provided and exists in the dataset
        if sample_id_to_db:
            if not sample_id_to_db in ds_db.columns:
                _str = 'Provided through configuration sample id column ({}) was not found in the current dataset. ' \
                       'Aborting the process.' \
                    .format(sample_id_to_db)
                self.logger.error(_str)
                self.error.add_error(_str)
                return False
        else:
            _str = 'No sample id column name was provided in the current configuration file (key: "{}"). ' \
                   'Aborting the process.' \
                .format('DATA/output_dataset/sample_id')
            self.logger.error(_str)
            self.error.add_error(_str)
            return False

        # get dataset's headers converted to a dictionary format
        row_dict = self.get_dataset_dictionary(ds_db, True, "name")
        # prepare config object to pass to MetadataDB object
        # dict2.update(dict1)
        cfg_mdb = self.cfg.get_value('DATA/record_dictionary')
        cfg_mdb.update(self.cfg.get_value('DB'))

        mdb = MetadataDB(None, cfg_mdb)

        r_cnt = 0
        # loop through the final dataset
        for row in ds_db.to_dict(orient='records'):
            # print(row)
            # print(json.dumps(row))
            r_cnt += 1

            if not self.error.errors_exist():
                self.logger.info(
                    'Record #{}. Proceeding to save it to database. Row data: {}'.format(r_cnt, row))
                mdb_resp = mdb.submit_row(
                    row[sample_id_to_db],
                    json.dumps(row),
                    json.dumps(row_dict),
                    self.api_process_name,
                    self.logger,
                    self.error
                )

                if not self.error.errors_exist():
                    _str = 'Record #{}. Sample Id "{}" was submitted to MDB. Status: {}; Description: {}'.format(
                        r_cnt, row[sample_id_to_db], mdb_resp[0][0]['status'], mdb_resp[0][0]['description'])
                    self.logger.info(_str)
                    if mdb_resp[0][0]['status'] != 'OK':
                        # if db response status is not OK, record it to the db_response_alerts list
                        if not self.db_response_alerts:
                            self.db_response_alerts = []
                        self.db_response_alerts.append(
                            {'sample_id':row[sample_id_to_db],
                             'status': mdb_resp[0][0]['status'],
                             'description': mdb_resp[0][0]['description'] }
                        )

                else:
                    _str = 'Record #{}. Error occured during submitting sample Id "{}" to MDB. Error details: {}'.format(
                        r_cnt, row[sample_id_to_db], self.error.get_errors_to_str())
                    self.logger.error(_str)

        # report error summary of processing api dataset
        self.logger.info('SUMMARY OF ERRORS and Alerts ==============>')
        self.logger.info('Critical errors: {}'.format(
            self.error.get_errors_to_str())) if self.error.errors_exist() else self.logger.info(
            'No critical errors were identified.')

        if self.validation_alerts:
            self.logger.warning('The following are {} identified validation alert(s):\n{}'
                             .format(len(self.validation_alerts),
                                     '\n'.join([str(va['text']) for va in self.validation_alerts])))
        else:
            self.logger.info('No validation alerts were identified during processing.')

        if self.db_response_alerts:
            self.logger.warning('The following are {} received database response alert(s):\n{}'
                             .format(len(self.db_response_alerts),
                                     '\n'.join([str(da) for da in self.db_response_alerts])))
        else:
            self.logger.info('No validation alerts were identified during processing.')

        pass

    def get_dataset_dictionary(self, dataset, sort=None, sort_by_field=None):
        if not self.dataset_dictionary:
            if not sort:
                sort = False
            if not sort_by_field:
                sort_by_field = ''

            # get configuration object reference
            cfg = self.cfg.get_value('DATA/record_dictionary')
            # get dataset's headers
            hdrs = dataset.columns
            self.dataset_dictionary = cm2.get_dataset_dictionary (hdrs, cfg, sort, sort_by_field)
        return self.dataset_dictionary

    # retrieve a reference for a column object from a dataframe
    def get_df_col_obj(self, df_col_name, data_frame = None):
        if not data_frame:
            data_frame = self.ds
        try:
            val_out = data_frame[df_col_name]
        except Exception:
            val_out = None
            self.logger.warning('Column "{}" cannot be retrieved from the currently being processed dataset.'
                                .format(df_col_name))
        return val_out

    # prepares a separate column for each of the provided validation rules where target columns stores True of False
    def prepare_dataset_validation_columns(self):
        # function will go over validaton rules specified for the current api processing config
        # it will create one column per a rule
        self.validation_rules = []
        valid_rules = self.cfg.get_value('DATA/validation_rules')
        cnt = 0
        if valid_rules: # if validation rules were provided, loop through those
            for valid_rule in valid_rules:
                cnt += 1
                valid_rule['name'] = '_rule{}_'.format(cnt)
                self.validation_rules.append(valid_rule)
                self.ds[valid_rule['name']] = cm.eval_cfg_value(valid_rule['rule'], self.logger, self.error, self)

        pass

    # applies unpivoting transformation to the dataset based on the configuration settings
    def unpivot_dataset(self):
        unpivot_vars = self.unpivot_cfg['unpivot_vars']
        unpivot_var_name = self.unpivot_cfg['unpivot_var_name']
        unpivot_value_name = self.unpivot_cfg['unpivot_value_name']
        unpivot_preserved_columns = self.unpivot_cfg['unpivot_preserved_columns']

        if unpivot_vars and unpivot_var_name and unpivot_value_name and unpivot_preserved_columns:
            # if configuration values were provided try to apply those
            try:
                # melt functions in pandas perform unpivot procedure
                # result table will contain
                #    - columns passed to id_vars unchanged
                #    - values from columns passed into value_vars will be combined into a single column, where
                #       - actual values will be stored in the column passed to value_name
                #       - and the original column names will be listed in the column passed to var_name parameter.
                # Note: Total number of rows will increased based on the combination of values of the fields
                # passed to the value_vars
                self.ds = pd.melt(self.ds,
                              id_vars= unpivot_preserved_columns,
                              value_vars= unpivot_vars,
                              var_name= unpivot_var_name,
                              value_name= unpivot_value_name
                              )
                # print(self.ds)
            except Exception as ex:
                _str = 'Error "{}" occurred while unpivoting dataset received from the API call. ' \
                       'Here is the unpivot columns loaded from the config file "{}".\n{} ' \
                    .format(ex, unpivot_vars, traceback.format_exc())
                self.logger.error(_str)
                self.error.add_error(_str)