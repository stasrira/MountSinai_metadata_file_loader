from utils import ConfigData, common as cm, global_const as gc
from file_load.file_error import ApiError
from api_load import ApiDataset

class ApiProcess():
    def __init__(self, api_cfg_file, log_obj):
        self.loaded = False
        # set logger object
        self.logger = log_obj

        # set error object
        self.error = ApiError(self)

        self.logger.info('Start processing API call for the following conig file: {}'.format(api_cfg_file))

        # load config file for the current api process
        cfg_file_path = gc.CONFIGS_DIR + api_cfg_file
        self.api_cfg = ConfigData(cfg_file_path)

        if not self.api_cfg.loaded:
            _str = 'Cannot load the config file: "{}"'.format(cfg_file_path)
            self.logger.error(_str)
            self.error.add_error(_str)
            return

        # get values from the config file
        self.api_name = self.api_cfg.get_value('API/name')
        self.api_url = self.api_cfg.get_value('API/url')
        self.post_fields = self.api_cfg.get_value('API/post_fields')

        # verify if "eval" is present in any of the post fields and perform the evaluation, if needed
        if self.post_fields:
            for pf in self.post_fields:
                self.post_fields[pf] = cm.eval_cfg_value(self.post_fields[pf], self.logger, self.error)

        # if no errors were generated during init, set loaded = True
        if not self.error.errors_exist():
            self.loaded = True

    def process_api_call(self):
        api_output, errors_reported = cm.perform_api_call(self.api_url, self.post_fields, self.logger, self.error)
        if errors_reported:
            # stop processing of API is error is reported
            self.logger.warning('Aborting processing current API call, since errors were reported (see earlier entries)')
            return
        if not api_output and len(api_output.strip()) != 0:
            # proceed with processing an API dataset
            api_ds = ApiDataset(api_output, self.api_cfg, self.logger, self.error)
        else:
            # stop processing API if returned dataset is empty
            self.logger.warning('API call returned an empty dataset, aborting processing the current API call')
            return
        pass