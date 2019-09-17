import process_file as pf
from pathlib import Path
import sys
import os
from os import walk
import time
import logging
import traceback

# if executed by itself, do the following
if __name__ == '__main__':

    '''
    ts = time.localtime()
    print(time.strftime("%Y%m%d_%H%M%S", ts))

    sys.exit()
    '''

    datafiles_path = 'E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles'
    df_path = Path(datafiles_path)

    # get current location of the script and create Log folder
    logPath = Path(os.path.dirname(os.path.abspath(__file__))) / 'Logs'
    os.makedirs (logPath,exist_ok = True)
    logFile = logPath / (time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log')

    # ------------------
    mlog = logging.getLogger(__name__)
    mlog.setLevel(logging.DEBUG)

    # create a file handler
    handler = logging.FileHandler(logFile)
    handler.setLevel(logging.DEBUG)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the file handler to the logger
    mlog.addHandler(handler)
    # -----------------
    '''
    mlog = logging.getLogger ("mainLog")

    mlog.setLevel(logging.DEBUG)
    mlog.setFormat

    mlog.basicConfig(filename=logFile,
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    '''

    mlog.debug("Test debug msg")
    mlog.info('Start processing files in "{}"'.format(df_path))

    try:

        (_, dirstudies, _) = next(walk(df_path))
        print('Study dirs: {}'.format(dirstudies))

        for dir in dirstudies:
            st_path = Path(datafiles_path) / dir
            (_, _, proc_files) = next(walk(Path(st_path)))

            print ('Study dir files: {}'.format(proc_files))

            for fl in proc_files:
                if fl[-4:] != '.cfg':
                    try:
                        fl_path = Path(st_path) / fl
                        print('--------->Process file {}'.format(fl_path))
                        mlog.info('File {} was selected for processing.'.format(fl_path))
                        if fl[-4:] == '.xls' or fl[-5:] == '.xlsx':
                            # identify excel file and create appropriate object to handle it
                            mlog.info('File {} was identified as Excel file'.format(fl_path))
                            fl_ob = pf.MetaFileExcel(fl_path)
                        else:
                            # create an object to process text files
                            mlog.info('File {} was identified as Text file'.format(fl_path))
                            fl_ob = pf.MetaFileText(fl_path)

                        # save timestamp of beginning of the file processing
                        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())

                        mlog.info('Start processing {} file.'.format(fl_path))

                        # process selected file
                        fl_ob.processFile()

                        mlog.info('Finish processing {} file.'.format(fl_path))

                        # identify if any errors were identified and set status variable accordingly
                        if not fl_ob.error.errorsExist() and fl_ob.error.rowErrorsCount() == 0:
                            fl_status = 'OK'
                        else:
                            fl_status = 'ERROR'

                        mlog.info('Processing status of file {} is {}'.format(fl_path, fl_status))

                        #print('=============>>File level errors: {}'.format(fl_ob.error.errorsExist()))
                        #print('=============>>Row level errors: {}'.format(fl_ob.error.rowErrorsCount()))

                        processed_dir = Path(st_path) / 'Processed'
                        if not os.path.exists(processed_dir):
                            # if Processed folder does not exist in the current study folder, create it
                            mlog.info('Creating directory for processed files "{}"'.format(processed_dir))
                            os.mkdir(processed_dir)

                        fl_processed_name = ts + '_' + fl_status + '_' + fl
                        print('New file name: {}'.format(ts + '_' + fl_status + '_' + fl))
                        # move processed files to Processed folder
                        os.rename (fl_path, processed_dir / fl_processed_name)
                        mlog.info('Processed file "{}" was moved and renamed as: "{}"'.format(fl_path, processed_dir / fl_processed_name))

                        fl_ob = None
                    except Exception as ex:
                        # report an error to log file and proceed to next file.
                        mlog.error('Error "{}" occurred during processing file: {}\n{} '.format(ex, fl_path, traceback.format_exc()))

    except Exception as ex:
        # report unexpected error to log file
        mlog.critical('Unexpected Error "{}" occurred during processing file: {}\n{} '.format(ex, os.path.abspath(__file__), traceback.format_exc()))

    sys.exit()

    '''
    # Testing: sorting of dictionaries
    lis = [{"name": "Nandini", "age": 20},
           {"name": "Manjeet", "age": 21},
           {"name": "Nikhil", "age": 19}]
    
    lisA = {"field":[{"name": "Nandini", "age": 20},
           {"name": "Manjeet", "age": 21},
           {"name": "Nikhil", "age": 19}]}
    
    # using sorted and lambda to print list sorted
    # by age
    print ("The list printed sorting by age: ")
    print (sorted(lis, key=lambda i: i['age']))
    print('==================')
    print(lisA)
    lisA['field']=sorted(lisA['field'], key=lambda i: i['age'])
    print (sorted(lisA['field'], key=lambda i: i['age']))
    print(lisA)
    
    #sys.exit()
    '''
    data_folder = Path(
        "E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles/study02")
    # print (data_folder)
    file_to_open_cfg = data_folder / "config.cfg"
    file_to_open = data_folder / "test01.xlsx"

    print ('Data file: {}; Config file: {}'.format (file_to_open, file_to_open_cfg))

    # fl3 = MetaFileExcel (file_to_open,'',2,'TestSheet1')
    fl3 = pf.MetaFileExcel(file_to_open,file_to_open_cfg)
    # print('File row from excel: {}'.format(fl3.getFileRow(2)))
    fl3.processFile()
    fl3 = None

    # sys.exit()

    data_folder = Path("E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles/study01")
    #print (data_folder)
    file_to_open = data_folder / "test1.txt"
    #print (file_to_open)

    #read file
    #fl = File(file_to_open)
    #fl = ConfigFile(file_to_open)

    #fl.readFileLineByLine()

    #printL (fl.getFileContent())
    #printL('Headers===> {}'.format(fl.headers)) #getHeaders()
    #printL(fl.getRowByNumber(3))
    #printL (fl.getRowByNumber(2))
    #printL(fl.getRowByNumber(1))
    #fl = None


    #read config file
    file_to_open_cfg = data_folder / "config.cfg"
    #print('file_to_open_cfg = {}'.format(file_to_open_cfg))
    #fl = ConfigFile(file_to_open_cfg, 1) #config file will use ':' by default
    ##fl.loadConfigSettings()

    #print('Setting12 = {}'.format(fl.getItemByKey('Setting12')))
    #fl = None



    #read metafile #1
    file_to_open = data_folder / "test1.txt"
    fl1 = pf.MetaFileText(file_to_open, file_to_open_cfg)
    print('Process metafile: {}'.format(fl1.filename))
    fl1.processFile()
    fl1 = None

    #sys.exit()  # =============================



    sys.exit()  # =============================

    # read metafile #2
    file_to_open = data_folder / "test2.txt"
    fl = MetaFileText(file_to_open)
    print('Process metafile: {}'.format(fl.filename))
    fl.processFile()
    fl = None

    print ('\nTest sorting of dictionary-----------')
    # read metafile #1
    file_to_open = data_folder / "test1.txt"
    fl = MetaFileText(file_to_open)
    #test dictionary extracts and sorting
    #dict_json = fl.getFileDictionary_JSON()
    print ('Not Sorted dictionary ====>{}'.format(fl.getFileDictionary_JSON()))
    #print (dict_json)
    #dict_json_sorted = fl.getFileDictionary_JSON(True, "type")
    print ('Sorted dictionary by "type" field ====>{}'.format(fl.getFileDictionary_JSON(True, "type")))
    #print (dict_json_sorted)
    #dict_json_sorted = fl.getFileDictionary_JSON(True)
    print ('Sorted by default field ====>{}'.format(fl.getFileDictionary_JSON(True)))
    #print (dict_json_sorted)
    fl = None


    sys.exit() # =============================

    #read excel file
    file_to_open = data_folder / "ISSMS_ECHO_PBMC_SEALFON.xlsx"
    wb = xlrd.open_workbook(file_to_open)
    #sheet = wb.sheet_by_index(0)
    sheet = wb.sheet_by_name("Sheet1")

    headers = sheet.row_values(0)
    print(headers)
    print(sheet.row_values(1))

    headers.sort()

    for x in headers:
        print (x)