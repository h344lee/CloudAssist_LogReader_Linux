"""
input file: 01-Adapter/log
output file: 00-Data Model
program: 01-Adapter
"""
import csv
import os
import platform
import pandas as pd
import logging
import time
import re
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_log_content(file_path):
    file_object = open(file_path)
    file_content = file_object.read()
    file_object.close()
    return file_content


def ext_db_checker(record_content, library_reference_list, engine_name_list, library_statement):
    external_engine_tuple = (
        'redshift', 'aster', 'db2' 'bigquery', 'greenplm', 'hadoop', 'hawq', 'impala', 'informix', 'jdbc', 'sqlsvr',
        'mysql', 'netezza', 'odbc', 'oledb', 'oracle', 'postgres', 'sapase', 'saphana', 'sapiq', 'snow', 'spark',
        'teradata', 'vertica', 'ybrick', 'mongo', 'sforce'
    )

    # case 1 base engine
    lib_base_regex = re.compile(r" libname (\w+?) (\'|\"|\()(.*?);", re.IGNORECASE)
    lib_base_list = lib_base_regex.findall(record_content)

    if lib_base_list:
        for lib_db in lib_base_list:

            library_reference_list.append(lib_db[0])
            engine_name_list.append('Base')
            library_statement.append("libname "+lib_db[0] + " " + lib_db[1]+lib_db[2] + ";")

    # case 2 external engine
    lib_ext_regex = re.compile(r" libname\s+(\w+?)\s+(\w+?)\s+(.*?);", re.IGNORECASE)
    lib_ext_list = lib_ext_regex.findall(record_content)

    if lib_ext_list:
        for lib_db in lib_ext_list:
            library_reference_list.append(lib_db[0])
            engine_name_list.append(lib_db[1])
            library_statement.append("libname " + lib_db[0] + " " + lib_db[1] + " " + lib_db[2] + ";")


def getInventory(current_path, current_folder, visited, file_list):
    if platform.system() == 'Windows':
        current_path = current_path + '\\' + current_folder
    else:
        current_path = current_path + '/' + current_folder

    visited[current_path] = True
    logging.debug("current path is " + current_path)
    folders = []
    current_path_files = os.listdir(current_path)
    for file_or_folder in current_path_files:
        if platform.system() == 'Windows':
            child_path = current_path + '\\' + file_or_folder
        else:
            child_path = current_path + '/' + file_or_folder

        if os.path.isdir(child_path):
            folders.append(file_or_folder)
        else:
            file_list.append((current_path, file_or_folder))

    for child_folder in folders:

        if platform.system() == 'Windows':
            child_path = current_path + '\\' + child_folder
        else:
            child_path = current_path + '/' + child_folder

        if visited.get(child_path) is None:
            logging.debug("go down to " + child_folder)
            getInventory(current_path, child_folder, visited, file_list)


if __name__ == '__main__':

    logging.disable(logging.ERROR)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-%(levelname)s-%(message)s')
    logging.info('start of the program')

    if not os.path.isdir('logs'):
        os.makedirs('logs')
        logging.debug('"logs" folder is just created on current location.\nplease put log files in the directory')
        logging.debug('the location is ' + str(os.getcwd()) + "\\logs")
        raise Exception('please put log files in the "\\logs" folder')

    current_path = os.getcwd()
    current_folder = 'logs'
    visited = dict()
    file_list = []

    lib_df = pd.DataFrame(columns=['LIB_ID', 'LIBREF', 'LIBRARY_ENGINE', 'LIB_STATEMENT'])
    LIB_ID, LIBREF, LIBRARY_ENGINE, LIB_STATEMENT = "", "", "", ""

    getInventory(current_path, current_folder, visited, file_list)

    logging.debug(file_list)

    library_reference_list = []
    engine_name_list = []
    library_statement = []

    for file_path, file_name in file_list:

        if file_name[-3:] == 'log':
            file_full_path = file_path + '\\' + file_name
            log_content = get_log_content(file_full_path)
            ext_db_checker(log_content, library_reference_list, engine_name_list, library_statement)

    lib_id = 1
    for libref, library_engine, lib_statment in zip( library_reference_list, engine_name_list, library_statement):
        file_record = [lib_id, libref, library_engine, lib_statment]
        lib_df = lib_df.append(pd.Series(file_record, index=lib_df.columns), ignore_index=True)
        logging.debug(file_record)
        lib_id += 1

    print(lib_df.iloc[1])
    print(lib_df.iloc[10])

    # get an absolute path of parent folder
    path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    # write the result to the 00-Data Model directory
    if platform.system() == 'Windows':
        if not os.path.isdir(path + "\\00-Data Model"):
            os.makedirs(path + "\\00-Data Model")
        lib_df.to_excel(path + "\\00-Data Model\\D_CLDASST_DISC_LIB_ENG.xlsx", index=False)
        lib_df.to_csv(path + "\\00-Data Model\\D_CLDASST_DISC_LIB_ENG.csv", index=False, quoting=csv.QUOTE_NONE)

    else:
        if not os.path.isdir(path + "/00-Data Model"):
            os.makedirs(path + "/00-Data Model")
        lib_df.to_excel(path + "/00-Data Model/D_CLDASST_DISC_LIB_ENG.xlsx", index=False)
        lib_df.to_csv(path + "/00-Data Model/D_CLDASST_DISC_LIB_ENG.csv", index=False, quoting=csv.QUOTE_NONE)
    logging.info('end of the program')


