import traceback
import json
import pandas as pd

class ApiDataset():
    def __init__(self, api_dataset_str, cfg_obj, log_obj, err_obj):
        self.cfg = cfg_obj
        self.logger = log_obj
        self.error = err_obj
        self.api_dataset_str = api_dataset_str
        try:
            # self.ds = eval(api_dataset_str)
            self.json_obj = json.loads(api_dataset_str)
            # self.ds = pd.DataFrame.from_dict(self.json_obj[0], orient="index")
            self.ds = pd.read_json(api_dataset_str)

            self.ds = pd.melt(self.ds,
                          id_vars = ['subject_id', 'redcap_event_name', 'visit_complete'],
                          value_vars=['sst_id','paxgene_id', 'cpt_id_1', 'tempus_id', 'saliva_id'],
                          var_name='sample_type',
                          value_name='sample_id'
                          )
            # ds2 = ds1[(ds1['subject_id'] == "H001")]
            print(self.ds)

            # f1 = self.get_df_col_obj('visit_complete')  # self.ds['visit_complete']

            # self.ds = self.ds[eval("self.get_df_col_obj('visit_complete') == 'Unverified'")]  #
            cond = "(self.get_df_col_obj('visit_complete') == 'Unverified')"
            self.ds['_rule1_'] = eval(cond)
            cond = "(self.get_df_col_obj('registration_complete') == 'Complete')"
            self.ds['_rule2_'] = eval(cond)

            # self.ds = self.ds[(self.ds['visit_complete'] == "Unverified")]
            print(self.ds)

            # self.ds['expected_sid'] = 'M'+ self.ds.subject_id + \
            #                           'V' + self.ds.redcap_event_name.str[-2:].str.replace(' ', '0')
            """
            self.ds['expected_sid'] = 'M'+ self.ds.subject_id + \
                                      'V' + self.ds.redcap_event_name.str[-2:].str.replace(' ', '0') + \
                                      self.ds.sample_type.apply(
                                          lambda x:
                                          'SST' if x == 'sst_id' else
                                          'PAX' if x == 'paxgene_id' else
                                          'CPT' if x == 'cpt_id_1' else
                                          'TEM' if x == 'tempus_id' else
                                          'SLV' if x == 'saliva_id' else ''
                                      )

            print(self.ds)
            """

            cond = "self.ds.sample_id == " \
                   "'M' + self.ds.subject_id + " \
                   "'V' + self.ds.redcap_event_name.str[-2:].str.replace(' ', '0') + " \
                   "self.ds.sample_type.apply(" \
                   " lambda x:" \
                   " 'SST' if x == 'sst_id' else" \
                   " 'PAX' if x == 'paxgene_id' else" \
                   " 'CPT' if x == 'cpt_id_1' else" \
                   " 'TEM' if x == 'tempus_id' else" \
                   " 'SLV' if x == 'saliva_id' else ''" \
                   ")"

            # self.ds = self.ds[eval(cond)]
            self.ds['_rule3_'] = eval(cond)
            print (self.ds)
            print (self.ds[self.ds._row_status == True])
            print(self.ds[self.ds._row_status == False])

            """
            self.ds = self.ds[
                self.ds.sample_id ==
                'M' + self.ds.subject_id + \
                'V' + self.ds.redcap_event_name.str[-2:].str.replace(' ', '0') + \
                self.ds.sample_type.apply(
                  lambda x:
                  'SST' if x == 'sst_id' else
                  'PAX' if x == 'paxgene_id' else
                  'CPT' if x == 'cpt_id_1' else
                  'TEM' if x == 'tempus_id' else
                  'SLV' if x == 'saliva_id' else ''
                )
            ]
            """
            print(self.ds)
            pass
        except Exception as ex:
            _str = 'Error "{}" occurred while converting API output string value "{}" to python object.\n{} '\
                .format(ex, api_dataset_str, traceback.format_exc())
            self.logger.error(_str)
            self.error.add_error(_str)
            self.ds = []
        self.headers = None
        self.get_headers()
        pass

    def get_headers(self):
        if not self.headers:
            if self.ds and len(self.ds) > 0:
                self.headers = [k for k in self.ds[0].keys()]
            else:
                self.headers = None
        return self.headers

    def get_df_col_obj(self, df_col_name, data_frame = None):
        if not data_frame:
            data_frame = self.ds
        try:
            val_out = data_frame[df_col_name]
        except Exception:
            val_out = None
            # TODO: Add a log message about error

        return val_out
