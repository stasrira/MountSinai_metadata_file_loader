import traceback

class ApiDataset():
    def __init__(self, api_dataset_str, cfg_obj, log_obj, err_obj):
        self.cfg = cfg_obj
        self.logger = log_obj
        self.error = err_obj
        # self.dataset = eval(api_dataset_str)
        try:
            self.dataset = eval(api_dataset_str)
        except Exception as ex:
            _str = 'Error "{}" occurred while converting API output string value "{}" to python object.\n{} '\
                .format(ex, api_dataset_str, traceback.format_exc())
            self.logger.error(_str)
            self.error.add_error(_str)
            self.dataset = []
        self.headers = None
        self.get_headers()
        pass

    def get_headers(self):
        if not self.headers:
            if self.dataset and len(self.dataset) > 0:
                self.headers = [k for k in self.dataset[0].keys()]
            else:
                self.headers = None
        return self.headers