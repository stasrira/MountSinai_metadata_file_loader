import os
import time
from pathlib import Path
import logging
from file_load.file_error import FileError
from utils.mdl_logging import setup_logger_common
from utils import global_const as gc
from .file_utils import StudyConfig


#  Text file class (used as a base)
class File:
    filepath = None
    wrkdir = None
    filename = None
    file_type = None  # 1:text, 2:excel
    file_delim = None  # ','
    lineList = None  # []
    __headers = None  # []
    error = None  # FileErrors class reference holding all errors associated with the current file
    sample_id_field_names = None  # []
    loaded = None
    logger = None

    def __init__(self, filepath, file_type=1, file_delim=','):
        self.filepath = filepath
        self.wrkdir = os.path.dirname(os.path.abspath(filepath))
        self.filename = Path(os.path.abspath(filepath)).name
        self.file_type = file_type
        self.file_delim = file_delim
        self.error = FileError(self)
        self.lineList = []
        self.__headers = []
        self.sample_id_field_names = []
        self.loaded = False

    @property
    def headers(self):
        if not self.__headers:
            self.get_headers()
        return self.__headers

    def setup_logger(self, wrkdir, filename):

        log_folder_name = gc.log_folder_name

        lg = setup_logger_common(StudyConfig.study_logger_name, StudyConfig.study_logging_level,
                                 Path(wrkdir) / log_folder_name,
                                 filename + '_' + time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log')

        self.log_handler = lg['handler']
        return lg['logger']

    def get_file_content(self):
        if not self.logger:
            loc_log = logging.getLogger(StudyConfig.study_logger_name)
        else:
            loc_log = self.logger

        if not self.lineList:
            if self.file_exists(self.filepath):
                loc_log.debug('Loading file content of "{}"'.format(self.filepath))
                with open(self.filepath, "r") as fl:
                    self.lineList = [line.rstrip('\n') for line in fl]
                    fl.close()
                    self.loaded = True
            else:
                _str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(
                    self.filepath)
                self.error.add_error(_str)
                loc_log.error(_str)
                self.lineList = None
                self.loaded = False
        return self.lineList

    def file_exists(self, fn):
        try:
            with open(fn, "r"):
                return 1
        except IOError:
            return 0

    def get_headers(self):
        if not self.__headers:
            hdrs = self.get_row_by_number(1).split(self.file_delim)
            self.__headers = [hdr.strip().replace(' ', '_') for hdr in hdrs]
        return self.__headers

    def get_row_by_number(self, rownum):
        line_list = self.get_file_content()
        # check that requested row is withing available records of the file and >0
        if line_list is not None and len(line_list) >= rownum > 0:
            return line_list[rownum - 1]
        else:
            return ""

    def rows_count(self, exclude_header=False):
        num = len(self.get_file_content())
        if exclude_header:
            num = num - 1
        return num
