import traceback
import json
import pandas as pd
from utils import common as cm

class ApiDataset():
    def __init__(self, api_dataset_str, cfg_obj, log_obj, err_obj):
        self.cfg = cfg_obj
        self.logger = log_obj
        self.error = err_obj

        self.api_dataset_str = api_dataset_str
        self.validation_rules = None
        self.validation_alerts = None  # TODO: add functionality to report alerts to users trhough email

        try:
            # self.json_obj = json.loads(api_dataset_str)
            # self.ds = pd.DataFrame.from_dict(self.json_obj[0], orient="index")
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
        """
        self.ds = pd.melt(self.ds,
                      id_vars = ['subject_id', 'redcap_event_name', 'visit_complete'],
                      value_vars=['sst_id','paxgene_id', 'cpt_id_1', 'tempus_id', 'saliva_id'],
                      var_name='sample_type',
                      value_name='sample_id'
                      )
        """
        # ds2 = ds1[(ds1['subject_id'] == "H001")]
        print(self.ds)

        self.prepare_dataset_validation_columns()
        print(self.ds)

        # TODO: loop through all validation rules and report records that fail those rules
        # select only records that satisfy all validation rules
        for valid_rule in self.validation_rules:
            ds_failed = self.ds[self.ds[valid_rule['name']] == False]
            # check if failed validation rule has to be reported
            if 'report_failure' in valid_rule.keys() and valid_rule['report_failure']:
                _str = 'Dataset validation failed for {} records. Error message: "{}". ' \
                       'Data extract for affected rows:\n{}'\
                    .format(ds_failed.shape[0], valid_rule['message'],
                            ds_failed[valid_rule['report_columns']]
                            if 'report_columns' in valid_rule.keys()
                            else 'No columns to display were provided for the current rule...')

                self.logger.warning(_str)
                # add validation alert to the validation alert list
                if not self.validation_alerts:
                    self.validation_alerts = []
                self.validation_alerts.append(_str)
            # print(ds_failed)

        # select only records that satisfy all validation rules
        for valid_rule in self.validation_rules:
            # commented for TESTING ONLY, un-comment!!!
            # self.ds = self.ds[self.ds[valid_rule['name']] == True]
            # print(self.ds)
            pass

        print (self.ds)

        # select only columns that has to be reported to MDB
        columns_to_db = self.cfg.get_value('DATA/output_dataset/columns_to_db')
        if columns_to_db:
            # if columns_to_db are provided, filter dataset based on the list of given columns
            self.ds = self.ds[columns_to_db]
        sample_id_to_db = self.cfg.get_value('DATA/output_dataset/sample_id')
        # TODO: verify that sample id was provided

        # TODO: Submit record by record from the result dataset to MDB
        # loop through the final dataset
        for index, row in self.ds.iterrows():
            print(row["sample_id"], row["subject_id"])

        # print (self.ds[self.ds._rule3_ == True])
        # print(self.ds[self.ds._rule3_ == False])

        pass

        self.headers = None
        self.get_headers()
        pass

    def get_headers(self):
        """
        if not self.headers:
            if self.ds and len(self.ds) > 0:
                self.headers = [k for k in self.ds[0].keys()]
            else:
                self.headers = None
        return self.headers
        """

    # retrieve a reference for a column object from a dataframe
    def get_df_col_obj(self, df_col_name, data_frame = None):
        if not data_frame:
            data_frame = self.ds
        try:
            val_out = data_frame[df_col_name]
        except Exception:
            val_out = None
            # TODO: Add a log message about error

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
                _str = 'Error "{}" occurred while unpivoting dataset received from the API call. Here is the unpivot columns loaded from the config file "{}".\n{} ' \
                    .format(ex, unpivot_vars, traceback.format_exc())
                self.logger.error(_str)
                self.error.add_error(_str)