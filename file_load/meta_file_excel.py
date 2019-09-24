from collections import OrderedDict
from pathlib import Path
import xlrd #installation: pip install xlrd
from .file_utils import StudyConfig, loadConfiguration
from .file import File
from .meta_file_text import MetaFileText
from utils import global_const as gc


# metadata Excel file class
class MetaFileExcel(MetaFileText):
    # cfg_file = None
    # file_dict = None  # OrderedDict()
    # rows = None  # OrderedDict()
    sheet_name = None

    def __init__(self, filepath, cfg_path = '', file_type=2, sheet_name = ''):

        #loadConfiguration (main_cfg_obj) # load global and local configureations

        File.__init__(self, filepath, file_type)

        self.logger = self.setup_logger(self.wrkdir, self.filename)
        self.logger.info('Start working with file {}'.format(filepath))

        self.logger.info('Loading config file.')
        # identify name of the config file for a study
        if len(cfg_path) == 0:
            cfg_path = Path(self.wrkdir) / gc.default_study_config_file

        if self.textFileExists(cfg_path):
            loadConfiguration(self, cfg_path)
            self.cfg_file = StudyConfig.config_loc
            self.file_dict = OrderedDict()
            self.rows = OrderedDict()

            self.sheet_name = ''
            # self.sheet_name = sheet_name.strip()
            if not self.sheet_name or len(self.sheet_name) == 0:
                # if sheet name was not passed as a parameter, try to get it from config file
                self.sheet_name = self.cfg_file.getItemByKey(gc.study_excel_wk_sheet_name) # 'wk_sheet_name'
            # print (self.sheet_name)
            self.logger.info('Sheet name that data will be loaded from: "{}"'.format(self.sheet_name))
        else:
            _str = 'Study configuration file "{}" does not exist, configuration loading was aborted.'.format(cfg_path)
            self.error.addError(_str)
            self.logger.error(_str)

        '''	
        # self.cfg_file = None
        cfg_file = self.getConfigInfo ('') # '(cfg_filepath)
        if not cfg_file.loaded:
            # report error for for failed config file loading
            _str = 'Neither the provided config file "{}" nor default "config.cfg" file could not be loaded.'.format(cfg_filepath)
            self.error.addError(_str)
            self.logger.error(_str)

        self.file_dict = OrderedDict()
        self.rows = OrderedDict()
        self.sheet_name = ''
        # self.sheet_name = sheet_name.strip()
        if not self.sheet_name or len(self.sheet_name) == 0:
            # if sheet name was not passed as a parameter, try to get it from config file
            self.sheet_name = self.cfg_file.getItemByKey('wk_sheet_name')
            # print (self.sheet_name)
        self.logger.info('Sheet name that data will be loaded from: "{}"'.format(self.sheet_name))
        '''

    def getFileContent (self):
        if not self.lineList:
            if self.fileExists (self.filepath):
                self.logger.debug('Loading file content of "{}"'.format(self.filepath))

                wb = xlrd.open_workbook(self.filepath)
                if not self.sheet_name or len(self.sheet_name) == 0:
                    # by default retrieve the first sheet in the excel file
                    sheet = wb.sheet_by_index(0)
                else:
                    # if sheet name was provided
                    sheets = wb.sheet_names() # get list of all sheets
                    if (self.sheet_name in sheets):
                        # if given sheet name in the list of available sheets, load the sheet
                        sheet = wb.sheet_by_name(self.sheet_name)
                    else:
                        # report an error if given sheet name not in the list of available sheets
                        _str = 'Given sheet name "{}" was not found in the file "{}". Verify that the sheet name exists in the file.'.format(self.sheet_name, self.filepath)
                        self.error.addError(_str)
                        self.logger.error(_str)

                        self.lineList = None
                        self.loaded = False
                        return self.lineList

                sheet.cell_value(0, 0)

                for i in range(sheet.nrows):
                    # ln = sheet.row_values(i)
                    # print (ln)

                    ln = []
                    for j in range(sheet.ncols):
                        # print(sheet.cell_value(i, j))
                        # ln.append('"' + sheet.cell_value(i,j) + '"')
                        cell = sheet.cell(i, j)
                        cell_value = cell.value
                        # take care of number and dates received from Excel and converted to float by default
                        if cell.ctype == 2 and int(cell_value) == cell_value:
                            # the value is integer
                            cell_value = str(int(cell_value))
                        elif cell.ctype == 2:
                            # the value is float
                            cell_value = str(cell_value)
                        # convert date back to human readable date format
                        # print ('cell_value = {}'.format(cell_value))
                        if cell.ctype == 3:
                            cell_value_date = xlrd.xldate_as_datetime(cell_value, wb.datemode)
                            cell_value = cell_value_date.strftime("%Y-%m-%d")
                        ln.append(cell_value)

                    self.lineList.append (','.join(ln))

                wb.unload_sheet(sheet.name)
                self.loaded = True
            else:
                _str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(self.filepath)
                self.error.addError(_str)
                self.logger.error(_str)

                self.lineList = None
                self.loaded = False
        return self.lineList

    def textFileExists(self, fn):
        return MetaFileText.fileExists(self, fn)

    def fileExists(self, fn):
        try:
            wb = xlrd.open_workbook(fn)
            return 1
        except Exception as ex: # IOError
            # print (ex)
            return 0

