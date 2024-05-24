### Importing necessary libraries

#import arcpy
#import os
import pandas as pd
#from arcgis.gis import GIS
#from datetime import date
#import requests
import importlib
import urllib3
#import sys
import config
#import openpyxl as pxl
#from openpyxl.utils.dataframe import dataframe_to_rows
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
print(urllib3.__version__)
import utils

importlib.reload(utils)
importlib.reload(config)

print("Done importing libraries...")

@utils.time_decorator(f"asyn_timing_log_{utils.return_today()}.txt")
def main_function():
    date_ = utils.return_today()

    out_path = config.login_dict['out_path'] + fr'\GIS WebServices Connection {date_}.xlsx'
    #log_path = config.login_dict['out_path'] + fr'\timing_{date_}.txt'

    ### Log in to org

    gis_source = utils.loggin_agol("config.py")

    ### Get Token from GIS Source
    ### If issues getting a token, please manually input your token here.

    token_ = config.login_dict['token']

    if token_ is None:
        print(f"Token is not found in config file, ill pull from GIS Source...")
        token_ = utils.request_token(gis_source)
    else:
        print("Token found...")
        pass

    ### Log in to portal and start the query

    params = {'f': 'json', 'token': token_}

    ### Creating alist of all feature services, max_items_returned to 1000

    item_list = utils.get_gis_content(gis_source)

    print("We have {} feature services to process.".format(len(item_list)))

    item_list = utils.pop_empty_urls(item_list)
    item_list = utils.pop_gdb_urls(item_list)
    item_list = utils.pop_repeated_urls(item_list)
    item_list = utils.clean_urls(item_list)

    dict_, url_dict_ = utils.pull_json(item_list, params)

    main_list, hosted_list, service_counter = utils.iterate_json(dict_, url_dict_)

    ### Creating pandas dataframes and corresponding columns

    columns_to_pd = ['TITLE', 
                    'OWNER', 
                    'URL',
                    'UPDATED_URL',
                    'ON_SER_INSTANCE', 
                    'ON_SER_DB_CLIENT', 
                    'ON_SER_CONNPROP', 
                    'ON_SER_DATABASE', 
                    'ON_SER_USER', 
                    'ON_SER_AUTH', 
                    'ON_SER_BRANCH_VERSION', 
                    'ON_PREM_INSTANCE',
                    'ON_PREM_DB_CLIENT',
                    'ON_PREM_DB_CONN', 
                    'ON_PREM_DATABASE', 
                    'ON_PREM_USER', 
                    'ON_PREM_AUTH', 
                    'ON_PREM_BRANCH_VERSION']

    hosted_columns = ['TITLE','OWNER','URL','HOSTED DATABASE']

    ### Setting up extended field names
    for ser_ in range(service_counter):
        columns_to_pd.append("Services_" + str(ser_))

    ### Setting up field names for extended columns
    output_df = pd.DataFrame(main_list, columns = columns_to_pd)
    hosted_df = pd.DataFrame(hosted_list, columns = hosted_columns)

    utils.output_to_excel(out_path, output_df, hosted_df)



if __name__ == '__main__':
    main_function()

    