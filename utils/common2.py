# from pathlib import Path
# import os
# import time
# import traceback
from utils import global_const as gc

# from utils import setup_logger_common TODO: figure out how to import setup_logger_common from utils module
# from utils import setup_logger_common
from utils import ConfigData


# convert headers of a dataset (file or api dataset) to a predefined dictionary structure
# ds_header_list - list of headers to be converted
# cfg_dict - part of configuration file containing predefined dictionary parameters used by the function
# use "sort" parameter to differentiate between 2 scenarios:
#   - preserve columns order of dictionary for creating files
#   - sorted alphabetically dictionary that is being submitted to DB
def get_dataset_dictionary(ds_header_list, cfg_dict, sort=False, sort_by_field=''):

    # dict1 = OrderedDict()
    cfg = ConfigData(None, cfg_dict)

    fields = cfg.get_item_by_key('dict_tmpl_fields_node')  # name of the node in dictionary holding array of fields

    ds_dict = eval(cfg.get_item_by_key('dict_tmpl'))  # {fields:[]}
    fld_dict_tmp = ds_dict[fields][0]
    ds_dict[fields].clear()

    if ds_dict:
        hdrs = ds_header_list  # self.get_row_by_number(1).split(self.file_delim)

        # identify item delimiter to be used with config values
        val_delim = cfg.get_item_by_key('config_value_list_separator')  # read config file to get "value list separator"
        if not val_delim:
            val_delim = ''
        # if retrieved value is not blank, return it; otherwise return ',' as a default value
        val_delim = val_delim if len(val_delim.strip()) > 0 else gc.DEFAULT_CONFIG_VALUE_LIST_SEPARATOR  # ','

        upd_flds = cfg.get_item_by_key('dict_field_tmpl_update_fields').split(val_delim)

        for hdr in hdrs:
            # hdr = hdr.strip().replace(' ', '_') # this should prevent spaces in the name of the column headers
            fld_dict = fld_dict_tmp.copy()
            for upd_fld in upd_flds:
                if upd_fld in fld_dict:
                    fld_dict[upd_fld] = hdr.strip()
            ds_dict[fields].append(fld_dict)

    # sort dictionary if requested
    if sort:
        # identify name of the field to apply sorting on the dictionary
        if len(sort_by_field) == 0 or sort_by_field not in ds_dict[fields][0]:
            sort_by_field = cfg.get_item_by_key('dict_field_sort_by')
            if len(sort_by_field) == 0:
                sort_by_field = 'name'  # hardcoded default

        # apply sorting, if given field name present in the dictionary structure
        if sort_by_field in ds_dict[fields][0]:
            ds_dict[fields] = sorted(ds_dict[fields], key=lambda i: i[sort_by_field])

    return ds_dict