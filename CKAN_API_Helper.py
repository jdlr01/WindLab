#!/usr/bin/env python
from ckanapi import RemoteCKAN
import requests
import yaml  # needs pyyaml
import xarray as xr
import os.path
import os 
import json  
import copy
import sys
import zipfile
from netCDF4 import Dataset
import pandas as pd

 
from jsonschema import ValidationError
from yml_utils import validate_yaml
import yml_utils

def dataset_schema_label():
    return 'schema_compliance'

def resource_schema_label():
    return 'framework'

def resource_label():
    return 'resource'


data_format_list        = ['txt', 'csv', 'netcdf', 'pdf', 'yaml']

# Required general parameter
general_requ_list       = ['title', 'notes', 'license_name', 'owner_org_name', 
                           'url', 'version', 'author', 'maintainer', 'maintainer_email', 
                           'subject', 'conditions', 'variable', 'spatial', ]
general_requ_list_type  = [str,  str, str, str, 
                           str, str, str, str, str, 
                           list, list, list, dict]

# Optional general parameter
general_opt_list        = ['group', 'private', 'funder', 'related_item', 'type',  
                           'date', dataset_schema_label(), 'model_type', 'resource_start_date', 'resource_end_date', 
                           'resource_start_time', 'resource_end_time', 'temporal_resolution', 'temporal_aggregation_method', 'spatial_resolution', 
                           'spatial_aggregation_method', 'data_instruments', 'background', ]
general_opt_list_type   = [str, bool, str, str, str, 
                           str, str, str, str, str, 
                           str, str, float, str, float, 
                           str, list, str]
type_dict = {'data-model':'data', 'data-experiment':'data', 'publication':'knowledge', 'process':'application', 'code':'application', 'model-chain':'application', }

# Required resource parameter
resources_list          = ['url','name', 'description', 'type', resource_schema_label(), 
                           'format', 'source']

resources_list_type     = [str, str, str, str,  str,
                           str, str]




def buffer_tabs(level_num):
    for ii in range(level_num):
        print("  ", end="")
    return

def read_resource(ckan,
                  name,
                  resource_id,
                  write_to_file=False,
                  dir_name='', 
                  error = True,
                  verbose=False,
                  level_num = 0):
    '''
    Actual working horce to get resource from Dataset.  

    Parameters
    ----------
    ckan : RemoteCKAN
        Connection to a CKAN data base through ckan = RemoteCKAN(ckan_url)
    name : TYPE, String
        String indicating the name of the Dataset.
    resource_id : String
        String, indicating the id of the resource to get.
    write_to_file : Boolean, optional, with default False.
        Boolean, indicating if resource to be written to file or returned as 
        function output
        Options:
            False: Resource will be returned as a function return
            True : Filename to localy data will be returned.
    dir_name : String, default is ''
        String, containing the absolute or relative path, where to drop files
        to, in case that setting for write_to_file == True. Example is 
        './temp/data_01/'
    verbose : TYPE, Boolean, optional, with default False
        Boolean for writing comments on the fly to screen. 

    Returns
    -------
    List
        List of dictionaries, with the following elements:
            'name': name of the Dataset
            'data': Either the data of the resource, or the file name with path
    '''
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Started: read_resource()')
        level_num = level_num + 1

    # Retrieve the resource details using resource_show
    try:
        resource = ckan.action.resource_show(id=resource_id)

        # Get the URL of the Resource
        csv_url = resource['url']

        # Download the Resource
        respo = requests.get(csv_url)

        # Check if the request was successful
        if respo.status_code != 200:
            if verbose:
                buffer_tabs(level_num)
                print(f"Failed to get data from data base. Status code: {respo.status_code}")
                if verbose:
                    buffer_tabs(level_num)
                    print('Exiting: read_resource()')

            return []

        # make local directory if needed
        # check if dir name supplied from user
        if dir_name != '':
            dir_name = os.path.join(dir_name, name )

        if os.path.isdir(dir_name) is False:
            try:
                os.makedirs(dir_name)
            except Exception:
                pass

        # check if name given for resource
        if resource['name'] == '':
            print('Warning: get_data_from_host: resource in ',
                  name, ' does not have a name. Name made up here.')
            resource['name'] = resource['id'][0:5] + '.' + resource['format']

        # create destination file name
        resource_file_name = os.path.join(dir_name, resource['name'])

        # Permutate through different file types
        # CSV files
        if resource['format'].lower() == 'csv':
            if write_to_file is False:
                data = respo.content
            else:
                # Save the CSV file locally
                if resource_file_name[-4:].lower() != '.csv':
                    resource_file_name = resource_file_name + '.csv'

                with open(resource_file_name, 'wb') as f:
                    f.write(respo.content)
                if verbose:
                    buffer_tabs(level_num)
                    print(f"CSV file saved as {resource_file_name}")
                data = resource_file_name
            if verbose:
                buffer_tabs(level_num)
                print('Exiting: read_resource()')
            return data

        # netCDF files
        elif ((resource['format'].lower() == 'netcdf') or
              (resource['format'].lower() == 'nc') or
              (resource['format'].lower() == 'application/x-hdf5')):

            # Save the file locally 
            # (optional step if you need to work with the file from disk)
            with open(resource_file_name, 'wb') as f:
                f.write(respo.content)

            if write_to_file is False:
                # Step 3: Open the NetCDF file using netCDF4.Dataset
                with xr.open_dataset(resource_file_name) as ds:
                    data = ds
                # Step 4: removing file
                os.remove(resource_file_name)
            else:

                # Open a .nc file ("file_name")
                data = Dataset(resource_file_name)
                print(data)
                #data = resource_file_name

            if verbose:
                buffer_tabs(level_num)
                print('Exiting: read_resource()')
            return data

        # YAML files
        elif resource['format'].lower() == 'yaml':

            if write_to_file:
                # Save the CSV file locally
                if resource_file_name[-5:].lower() != '.yaml':
                    resource_file_name = resource_file_name + '.yaml'

                with open(resource_file_name, 'wb') as f:
                    f.write(respo.content)
                if verbose:
                    buffer_tabs(level_num)
                    print(f"YAML file saved as {resource_file_name}")
                data = resource_file_name

            else:
                # get data as is
                resource_file_name = resource_file_name.replace('\\', '/')
                with open(resource_file_name, 'r') as file:
                    windlab_yaml = yaml.safe_load(file)
                print(data)

            if verbose:
                buffer_tabs(level_num)
                print('Exiting: read_resource()')
            return data

        # ZIP files
        elif resource['format'].lower() == 'zip':
            if write_to_file is False:
                data = respo.content
            else:
                # Save the zip file locally
                if resource_file_name[-4:].lower() != '.zip':
                    resource_file_name = resource_file_name + '.zip'

                with open(resource_file_name, 'wb') as f:
                    f.write(respo.content)
                if verbose:
                    buffer_tabs(level_num)
                    print(f"ZIP file saved as {resource_file_name}")
                data = resource_file_name

            return data
        elif resource['format'].lower() == 'txt':
            if write_to_file is False:
                data = respo.content
            else:
                # Save the CSV file locally
                if resource_file_name[-4:].lower() != '.txt':
                    resource_file_name = resource_file_name + '.txt'

                with open(resource_file_name, 'wb') as f:
                    f.write(respo.content)
                if verbose:
                    buffer_tabs(level_num)
                    print(f"CSV file saved as {resource_file_name}")
                data = resource_file_name
            return data
        else:
            if verbose:
                buffer_tabs(level_num)
                print("The resource is not a CSV, netCDF nor YAML file.")

    except Exception as e:
        print('Exception ', e)
        if error:
            raise ValueError('Not dataset_id')
        else:
            return None





# def get_org_id(ckan_url, api_key, thisdata):
#     """
    

#     Parameters
#     ----------
#     ckan_url : TYPE
#         DESCRIPTION.
#     api_key : TYPE
#         DESCRIPTION.
#     thisdata : TYPE
#         DESCRIPTION.

#     Returns
#     -------
#     org_id : TYPE
#         DESCRIPTION.

#     """
#     if 'org_id' in thisdata:
#         org_id = thisdata['org_id']
#     elif 'owner_name' in thisdata:
#         # CKAN organization list API endpoint
#         # Options are:
#         # - group_list
#         # - package_list
#         # - tag_list
#         print('not working 23l;kasrj')

#         org_list_url = f'{ckan_url}/api/3/action/group_list'

#         # Headers for CKAN API request (include API key if required)
#         headers = {
#             'Authorization': api_key,  # Omit this if the CKAN instance does not require authentication for this API
#             'Content-Type': 'application/json',
#         }

#         # Send a GET request to retrieve the list of organizations
#         respo = requests.get(org_list_url, headers=headers)

#         # Check the response status
#         if respo.status_code == 200:
#             orgs = respo.json()['result']
#             org_ids = []
#             # Extract organization IDs and print them
#             for org in orgs:
#                 org_ids.append(org['id'])
#         else:
#             print(f"Failed to retrieve organizations. Status Code: {respo.status_code}")
#             print("Error:", respo.json())
#     else:
#         print('ERROR: Code not written in get_org_id()')
#         return None

#     return org_id

def list_2_string(thisList):
    """
    Converting a list of strings into a string, where each list element is separated by a comma
    
    """

    if len(thisList) == 0:
        return ''
    
    thisString = str(thisList[0])

    for ii in thisList:
        thisString = thisString  + ',' + str(ii)

    return thisString



def check_meta_data(thisdata_in, 
                    verbose = False, 
                    error = True,
                    level_num = 0):
    
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: check_meta_data()')
    level_num = level_num + 1

    for thisdata in thisdata_in:
        # general_requ
        for ii in range(len(general_requ_list)):
            if verbose:
                buffer_tabs(level_num)
                print('Doing: "', general_requ_list[ii],'"')
            if general_requ_list[ii] in thisdata['general_requ']:
                if thisdata['general_requ'][general_requ_list[ii]] == None:
                    if general_requ_list_type[ii] == str:
                        thisdata['general_requ'][general_requ_list[ii]] = ""
                    elif general_requ_list_type[ii] == float:
                        thisdata['general_requ'][general_requ_list[ii]] = None
                    elif general_requ_list_type[ii] == list:
                        thisdata['general_requ'][general_requ_list[ii]] = []
                    elif  general_requ_list_type[ii] ==  json:
                        thisdata['general_requ'][general_requ_list[ii]] = {}
                    else:
                        print('ERROR: Part not coded for "',  general_requ_list_type[ii], '"')
                        sys.exit()
                elif general_requ_list[ii] == 'version':
                    thisdata['general_requ'][general_requ_list[ii]] = str(thisdata['general_requ'][general_requ_list[ii]])
                    if verbose:
                        buffer_tabs(level_num)
                        print('passed: general_requ."',general_requ_list[ii],'"')
                elif isinstance(thisdata['general_requ'][general_requ_list[ii]], general_requ_list_type[ii]):
                    if general_requ_list_type[ii] == list:
                        thisdata['general_requ'][general_requ_list[ii]] = list_2_string(thisdata['general_requ'][general_requ_list[ii]])
                    if verbose:
                        buffer_tabs(level_num)
                        print('passed: general_requ."',general_requ_list[ii],'"')
                else:
                    print('ERROR: In general_requ: Type of "' + general_requ_list[ii] + '" not correct. Needs to be ' + str(general_requ_list_type[ii]))
                    sys.exit()
            else:
                print('ERROR: In general_requ: No key found for: "' + general_requ_list[ii],'"')
                sys.exit()
        
        # general_opt
        # set up thisdata['general_opt'] if not given
        if 'general_opt' not in thisdata:
            thisdata['general_opt'] = {}
            for ii in range(len(general_opt_list)):
                thisdata['general_opt'][general_opt_list[ii]] = None

        # set up thisdata['general_opt'] if None
        elif thisdata['general_opt'] == None:
            thisdata['general_opt'] = {}
            for ii in range(len(general_opt_list)):
                thisdata['general_opt'][general_opt_list[ii]] = None

        # check thisdata['general_opt']
        else:
            for ii in range(len(general_opt_list)):
                if verbose:
                    buffer_tabs(level_num)
                    print('Doing: "', general_opt_list[ii],'"')

                if general_opt_list[ii] in thisdata['general_opt']:
                    # converting geographic_location into spatial
                    # type
                    if general_opt_list[ii] == 'type':
                        if thisdata['general_opt'][general_opt_list[ii]] == None:
                            pass
                        else:
                            temp = thisdata['general_opt'][general_opt_list[ii]]
                            temp = temp.replace(' ', '').lower()
                            thisdata['general_opt'][general_opt_list[ii]] = type_dict[temp]
                        if verbose:
                            buffer_tabs(level_num+1)
                            print('passed: general_opt."',general_opt_list[ii],'"')
                    # resource_start_time
                    elif general_opt_list[ii] == 'resource_start_time':
                        if thisdata['general_opt'][general_opt_list[ii]] == None:
                            pass
                        else:
                            thisdata['general_opt'][general_opt_list[ii]] = str(thisdata['general_opt'][general_opt_list[ii]])
                        if verbose:
                            buffer_tabs(level_num+1)
                            print('passed: general_opt."',general_opt_list[ii],'"')
                    # resource_end_time
                    elif general_opt_list[ii] == 'resource_end_time':
                        if thisdata['general_opt'][general_opt_list[ii]] == None:
                            pass
                        else:
                            thisdata['general_opt'][general_opt_list[ii]] = str(thisdata['general_opt'][general_opt_list[ii]])
                        if verbose:
                            buffer_tabs(level_num+1)
                            print('passed: general_opt."',general_opt_list[ii],'"')
                    # temporal_resolution
                    elif general_opt_list[ii] == 'temporal_resolution':
                        if thisdata['general_opt'][general_opt_list[ii]] == None:
                            pass
                        else:
                            thisdata['general_opt'][general_opt_list[ii]] = str(thisdata['general_opt'][general_opt_list[ii]])
                        if verbose:
                            buffer_tabs(level_num+1)
                            print('passed: general_opt."',general_opt_list[ii],'"')
                    # spatial_resolution
                    elif general_opt_list[ii] == 'spatial_resolution':
                        if thisdata['general_opt'][general_opt_list[ii]] == None:
                            pass
                        else:
                            thisdata['general_opt'][general_opt_list[ii]] = str(thisdata['general_opt'][general_opt_list[ii]])
                        if verbose:
                            buffer_tabs(level_num+1)
                            print('passed: general_opt."',general_opt_list[ii],'"')
                    elif isinstance(thisdata['general_opt'][general_opt_list[ii]], general_opt_list_type[ii]):
                        if general_opt_list_type[ii] == list:
                            thisdata['general_opt'][general_opt_list[ii]] = list_2_string(thisdata['general_opt'][general_opt_list[ii]])
                        elif general_opt_list_type[ii] == complex:
                            thisdata['general_opt'][general_opt_list[ii]] = {"type": "Point", "coordinates":thisdata['general_opt'][general_opt_list[ii]]}
                        
                    elif thisdata['general_opt'][general_opt_list[ii]] == None:
                        pass
                    else:
                        print('ERROR: In general_opt: Type of "' + general_opt_list[ii] + '" not correct. Needs to be ' + str(general_opt_list_type[ii]))
                        sys.exit()
                # Generate if not given
                else:
                    thisdata['general_opt'][general_opt_list[ii]] = None

        # resources
        if resource_label() not in thisdata:
            thisdata[resource_label()] = []
        elif thisdata[resource_label()] == None:
            thisdata[resource_label()] = []
        else:
            for thisResource in thisdata[resource_label()]:
                for ii in range(len(resources_list)):
                    if resources_list[ii] in thisResource:
                        if isinstance(thisResource[resources_list[ii]], resources_list_type[ii]):

                            if resources_list[ii] == 'source':
                                if os.path.isfile(thisResource['source']):
                                    if verbose: 
                                        buffer_tabs(level_num)
                                        print('Resource "', thisResource['source'] ,'" exists. Pass.')
                                else:
                                    print('Resource "', thisResource['source'], '" could not be found.')
                                    sys.exit()

                            elif resources_list[ii] in thisdata['general_opt']:
                                # converting geographic_location into spatial
                                if general_opt_list[ii] == 'type':
                                    if thisdata['general_opt'][general_opt_list[ii]] == None:
                                        pass
                                    else:
                                        temp = thisdata['general_opt'][general_opt_list[ii]]
                                        temp = temp.replace(' ', '').lower()
                                        thisdata['general_opt'][general_opt_list[ii]] = type_dict[temp]

                            if verbose:
                                buffer_tabs(level_num)
                                print('passed: ',resource_label(),'."',general_opt_list[ii],'"')

                        elif thisResource[resources_list[ii]] == None:
                            pass
                        else:
                            thisResource[resources_list[ii]] = None
                    else:
                        thisResource[resources_list[ii]] = None

    if verbose: 
        buffer_tabs(level_num)
        print('Setup File Test Passed.')
    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: check_meta_data()')
    return thisdata_in


def setup_dataset(ckan_url, 
                  api_token, 
                  org_id, 
                  thisdata, 
                  verbose = False, 
                  error = True,
                  level_num = 0):
    """
    Sets up a data set in the WindLab, using DataCite meta data.
    

    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String
        CKAN access token.
    org_id : String
        ID for the data set.  
    thisdata : Dict
        Dict containing DataCite meta data in respect of the data set.

    Returns
    -------
    String
        Data set ID.
    """
    level_num       = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: setup_dataset()')
    level_num = level_num + 1

    general_requ    = thisdata['general_requ']
    general_opt     = thisdata['general_opt']
    resources       = thisdata[resource_label()]

    # Check if data set name in use, otherwise find new one
    count = 0
    name_new = general_requ['title'].replace(' ', '_').replace('(', '_').replace(')', '_').lower()
    name_new = name_new[0:20]
    name = copy.deepcopy(name_new)
    while True:
        ret = is_name_in_use(ckan_url, 
                             api_token, 
                             name, 
                             verbose = verbose, 
                             error = error,
                             level_num = level_num)
        if ret == False:
            break
        else:
            name = name_new + '_' + str(count)
            count = count + 1

    # CKAN dataset creation API endpoint
    dataset_create_url = f'{ckan_url}/api/3/action/package_create'
    # Headers for CKAN API request
    headers = {'Authorization': api_token,
               'Content-Type': 'application/json'}

    data = {"name": name, 
            "owner_org": org_id, }
    # general_requ_list
    for key in general_requ_list:
        if key in general_requ:
            data[key] = general_requ[key]

    # general_opt_list
    for key in general_opt_list:
        if key in general_opt:
            data[key] = general_opt[key]

    # resources_list
    for key in resources_list:
        if key in resources:
            data[key] = resources[key]


    # Send a POST request to CKAN to create the dataset
    response = requests.post(dataset_create_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        if response.status_code == 409:
            print('ERROR: setup_dataset(): data set URL already in use.')
        else:
            print(f"Failed to create dataset. Status Code: {response.status_code}")
            print("Error:", response.json())
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: setup_dataset()')
        return []
    else:
        dataset_id = response.json()['result']['id']

    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: setup_dataset()')

    return dataset_id


def is_name_in_use(ckan_url, 
                   api_token, 
                   dataset_name,
                   verbose = False,
                   error = True,
                   level_num = 0):
    """
    Checks if name already given in WindLab

    Test: True

    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String (Not Used)
        CKAN access token.
    dataset_name : String
        Name to be checked

    Returns
    -------
    Boolean or None
        False if name not in WindLab so far. 
        True if name in WindLab.
        None, if error in WindLab querry
    """
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: is_name_in_use()')
    level_num = level_num + 1

    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)

    # Get the dataset details
    try:
        ret = ckan.action.package_show(id=dataset_name)
        if ret == None:
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: is_name_in_use()')
            return False
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: is_name_in_use()')
        return True
    except:
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: is_name_in_use()')
        return False
    


def setup_meta_dict(verbose = False,
                    level_num = 0):
    """
    Generates a default metadata dict.

    Parameters
    ----------

    Returns
    -------
    dict
    
    """
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: setup_meta_dict()')
    level_num = level_num + 1

    windlab_data = {}
    windlab_data['general_requ_list'] = {}
    windlab_data['general_opt_list'] = {}
    windlab_data['resource_opt_list'] = {}
    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: setup_meta_dict()')
    return windlab_data



def get_from_xls(file_name, 
                 verbose = False, 
                 error = False, 
                 level_num = 0):
    """
    Returns metadata from excel book.


    """
    level_num    = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: get_from_xls()')
    level_num = level_num + 1
        
    windlab_data = setup_meta_dict(verbose = verbose,
                                   level_num = level_num)
    if os.path.exists(file_name):
        # Reading the excel table.
        excel_data_df = pd.read_excel(file_name, sheet_name='Tabelle1', 
                  index_col=None,
                  dtype={'Name': str, 'Your input below': str})
        
        # Setting a few variables
        keys = excel_data_df['Name'].tolist()
        values = excel_data_df['Your input below'].tolist()
        num_resource_title = keys.count('resource_title')

        # Setting up the resources
        if num_resource_title > 0:
            windlab_data['resources'] = []

        # going through each key except resources
        for key, val in zip(keys, values):
            if key in general_requ_list:
                windlab_data['general_requ_list'][key] = val
            elif key in general_opt_list:
                windlab_data['general_opt_list'][key] = val

        # getting all the differen resources
        for ii in range(num_resource_title):
            windlab_data['resources'].append({})
            for key in resources_list:
                # finding where key in list of keys
                pos = keys.index(key)
                # Grabbing the value
                windlab_data['general_requ_list'][key] = values[pos]
                # removing key and value from list
                keys.pop(pos)
                values.pop(pos)

    # Any non-coded stuff
    else:
        print('Error: get_from_xls(): Not able to find file "', file_name, '"')
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: get_from_xls()')
        return {}

    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: get_from_xls()')
    return windlab_data



def read_yaml(access_dir_file_name  = 'default.yml',
                dir_file_name       = None, 
                section_name        = 'data', 
                process_name        = '',
                verbose             = False,
                error               = True,
                level_num           = 0):
    """
    Reads in the required meta data through a yaml file, 
    and returns the information as a dict.

    Parameters
    ----------
    access_dir_file_name : String, optional
        Dir and file name of the yaml file containing URL, access-token, and other settings.
    dir_file_name : String, optional
        File and path name of the yaml file. Abs path name needed. 
        The default is None.
    section_name : string, default 'data' 
        Reads in the information from section section_name
    process_name : String, optional
        Name of the proces, that was trying to read the yaml file. 
        The default is ''.
    verbose : Boolean, default False
        Boolean for display of messages to screen
    error : Boolean, default False
        Boolean for displaying caught error messages

    Returns
    -------
    ckan_url: String
        containing the URL of the WindLab
    api_token : String
        Containing the access key if given
    windlab_data
        Content of the yaml file.
    verbose : boolean
        Boolean to throw errors or not
    errorVal : boolean
        Boolean to write further information to screen
    """

    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: read_yaml()')
    level_num = level_num + 1

    # Getting some default information for WindLab
    ckan_url, api_token, verbose, errorVal = \
    read_access(dir_file_name   = access_dir_file_name, 
                verbose         = verbose,
                error           = error,
                level_num       = level_num)
    
    with open(dir_file_name, 'r') as file:
        windlab_data = yaml.safe_load(file)

    if 'file' in windlab_data['data']:
        windlab_temp = []
        rel_data_dir_name = ''
        if 'rel_data_dir_name' in windlab_data['data']:
            rel_data_dir_name = windlab_data['data']['rel_data_dir_name']

        for file_name in windlab_data['data']['file']: 
            # zip file
            if file_name[-4:].lower() == '.zip':
                windlab_temp.append([get_meta_from_zip(file_name)])
            
            # yaml files
            elif (file_name[-4:].lower() == '.yml') or (file_name[-5:].lower() == '.yaml'):
                with open(file_name, 'r') as file:
                    windlab_temp.append([yaml.safe_load(file)])
            
            # Excel tables
            elif (file_name[-4:].lower() == '.xls') or (file_name[-5:].lower() == '.xlsm'):
                windlab_temp.append(get_from_xls(file_name))

            else:
                print('ERROR: read_yaml(): This type of file not coded.')
                sys.exit

        windlab_data = windlab_temp[0]

    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: read_yaml()')

    return ckan_url, api_token, windlab_data, verbose, errorVal


#def read_json(access_dir_file_name = 'default.yml',
#                dir_file_name       = None, 
#                section_name        = 'data', 
#                process_name        = '',
#                verbose             = False,
#                error               = True):
#    
#    """
#    Reads in the required meta data through a json file, 
#    and returns the information as a dict.#
#
#    Parameters
#    ----------
#    access_dir_file_name : String, optional
#        Dir and file name of the yaml file containing URL, access-token, and other settings.
#    dir_file_name : String, optional
#        File and path name of the yaml file. Abs path name needed. 
#        The default is None.
#    section_name : string, default 'data' 
#        Reads in the information from section section_name
#    process_name : String, optional
#        Name of the proces, that was trying to read the yaml file. 
#        The default is ''.
#    verbose : Boolean, default False
#        Boolean for display of messages to screen
#    error : Boolean, default False
#        Boolean for displaying caught error messages
#
#    Returns
#    -------
#    ckan_url: String
#        containing the URL of the WindLab
#    api_token : String
#        Containing the access key if given
#    windlab_data
#        Content of the yaml file.
#    verbose : boolean
#        Boolean to throw errors or not
#    errorVal : boolean
#        Boolean to write further information to screen
#   """

    # Getting some default information for WindLab
#    ckan_url, api_token, verbose, errorVal = \
#    read_access(dir_file_name = access_dir_file_name, 
#                verbose = verbose,
#                error = error)
    
#    with open(dir_file_name, 'r') as file:
#        windlab_data = json.safe_load(file)
#    
#    return ckan_url, api_token, windlab_data, verbose, errorVal

    

def get_meta_from_zip(dir_file_name = None, 
                      verbose       = False, 
                      error         = True,
                      level_num     = 0):
    """
    Returns meta data from zip file. 
    REQUIREMENT: Meta data file name needs to have name of "WindLab_meta.yaml".

    """
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: get_meta_from_zip()')
        level_num = level_num + 1

    windlab_data = None
    if os.path.isfile(dir_file_name):
        archive = zipfile.ZipFile(dir_file_name, 'r')
        dir_name = os.path.basename(dir_file_name)
        dir_name = dir_name[:-4]
        windlab_data = archive.read(dir_name + '/WindLab_meta.yaml')
        windlab_data = yaml.safe_load(windlab_data)

        if verbose: 
            buffer_tabs(level_num-1)
            print(windlab_data)
    else:
        if error: print('ERROR: get_meta_from_zip(): zip file not found.')
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: get_meta_from_zip()')
        return None

    if verbose:
        buffer_tabs(level_num-1)
        print('Exiting: get_meta_from_zip()')
    return windlab_data




def get_line():

    return ''


def read_access(dir_file_name=None, 
               verbose = False,
               error = True, 
               level_num = 0):
    """
    Reads in the  file containing access to the WindLab

    Parameters
    ----------
    dir_file_name : String, optional
        File and path name of the yaml file. Abs path name needed. 
        The default is None.
    process_name : String, optional
        Name of the proces, that was trying to read the yaml file. 
        The default is ''.

    Returns
    -------
    Dict
        Content of the yaml file.

    """
    process_name = 'read_access'
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: read_access()')
    level_num = level_num + 1

    try:
        # Check, that yml file supplied
        if dir_file_name is None:
            print('Trying to ', process_name, ' data, but no setup file given.')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_access()')
            return False
        
        # Reading yml file, and getting URL and API token 
        with open(dir_file_name, 'r') as file:
            windlab_yaml = yaml.safe_load(file)

        if ('URL' not in windlab_yaml['API']) or ('token' not in windlab_yaml['API']) or ('verbose' not in windlab_yaml['API']) or ('error' not in windlab_yaml['API']):
            print('ERROR Reading set up file. File content API not up to date.')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_access()')
            return None
        
        ckan_url = windlab_yaml['API']['URL']
        api_token = windlab_yaml['API']['token']
        verbose = windlab_yaml['API']['verbose']
        errorVal = windlab_yaml['API']['error']
        
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: read_access()')
        return ckan_url, api_token, verbose, errorVal
    except Exception  as err:
        if error:
            raise ValueError('ERROR: read_access: with error message:', err)
        else:
            if verbose:
                print('ERROR: read_access: Not able to read setup file')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_access()')
            return None


def read_setup(dir_file_name= None, 
               section_name = 'data', 
               process_name = '',
               verbose      = False,
               error        = True,
               level_num    = 0):
    """
    Reads in the yaml setup file

    Parameters
    ----------
    dir_file_name : String, optional
        File and path name of the yaml file. Abs path name needed. 
        The default is None.
    process_name : String, optional
        Name of the proces, that was trying to read the yaml file. 
        The default is ''.

    Returns
    -------
    Dict
        Content of the yaml file.

    """
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: read_setup()')
    level_num = level_num + 1

    try:
        # Check, that yml file supplied
        if dir_file_name is None:
            print('Trying to ', process_name, ' data, but no setup file given.')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_setup()')
            return False
        
        # Reading yml file, and getting URL and API token 
        with open(dir_file_name, 'r') as file:
            windlab_yaml = yaml.safe_load(file)

        if ('URL' not in windlab_yaml['API']) or ('token' not in windlab_yaml['API']) or ('verbose' not in windlab_yaml['API']) or ('error' not in windlab_yaml['API']):
            print('ERROR Reading set up file. File content API not up to date.')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_setup()')
            return None
        
        ckan_url = windlab_yaml['API']['URL']
        api_token = windlab_yaml['API']['token']
        verbose = windlab_yaml['API']['verbose']
        errorVal = windlab_yaml['API']['error']
        
        # getting yaml dict of data to be stored.
        windlab_data = None
        if 'data' in windlab_yaml:
            if windlab_yaml['data'] != None:
                if 'file' in windlab_yaml['data']:
                    pass
                else:
                    windlab_data = windlab_yaml[section_name]
            
        
        if verbose:
            buffer_tabs(level_num-1)
            print('Exiting: read_setup()')
        return ckan_url, api_token, windlab_data, verbose, errorVal
    except Exception  as err:
        if error:
            raise ValueError('ERROR: read_setup: with error message:', err)
        else:
            if verbose:
                buffer_tabs(level_num)
                print('ERROR: read_setup: Not able to read setup file')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: read_setup()')
            return None


def print_all_org_names(ckan_url, 
                         api_token,
                         level_num = 0):
    
    level_num = level_num + 1
    if verbose:
        buffer_tabs(level_num)
        print('Starting: print_all_org_names()')
        level_num = level_num + 1

    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)
    
    # Get the dataset details
    try:
        entries_list  = ckan.action.organization_list(all_fields=True)
        
    except:
        
        if error:
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: print_all_org_names()')
            raise ValueError('No org_id')
        else:
            if verbose: 
                buffer_tabs(level_num)
                print('ERROR: print_all_org_names(): Not able to get list of all organizations from WindLab')
            if verbose:
                buffer_tabs(level_num-1)
                print('Exiting: print_all_org_names()')
            org_id = None
            return org_id
        
            
    for org in entries_list :
        print(org['display_name'].lower())


def get_org_id_from_name(ckan_url, 
                         api_token, 
                         org_disp_name, 
                         verbose = False,
                         error=True,
                         level_num = 0):
    """
    Returns the ID of an organisation, that is associated with a data set.

    Test: True
    
    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String
        CKAN access token.
    org_disp_name : String
        display name of the organization, as it is found on the WindLab web page.
    verbose : Boolean, optional
        If true, further display to screen. The default is False.

    Returns
    -------
    org_id : string
        string containing the ID of the organization.

    """
    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting: "get_org_id_from_name"')
        level_num = level_num + 1

    org_id = None
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)
    
    # Get the dataset details
    try:
        entries_list  = ckan.action.organization_list(all_fields=True)
        
    except:
        
        if error:
            raise ValueError('No org_id')
        else:
            if verbose: 
                print('ERROR: get_org_id_from_name(): Not able to get list of all organizations from WindLab')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_org_id_from_name"')
            org_id = None
            return org_id

    try:
        for org in entries_list :
            this_name = org['display_name'].lower()
            this_org_id = org['id']
            if this_name == org_disp_name.lower():
                org_id = this_org_id
                if verbose: 
                    buffer_tabs(level_num-1)
                    print('Exiting "get_org_id_from_name"')

                return org_id
    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_org_id_from_name"')
            raise ValueError('Not able to get ord_id')
        else:
            if verbose:
                print('ERROR: get_org_id_from_name(): Not able to get ord_id')

    Error_Mess = 'Not ord_id found for organization : '+ org_disp_name.lower()
    if verbose:
        buffer_tabs(level_num)
        print('+++++++')
        buffer_tabs(level_num)
        print('Options are:')
        for org in entries_list :
            buffer_tabs(level_num)
            print(org['display_name'].lower())

    if verbose: 
        buffer_tabs(level_num-1)
        print('Exiting "get_org_id_from_name"')
    raise ValueError(Error_Mess)
        

def get_cc_id_from_name(ckan_url, 
                        api_token, 
                        cc_disp_name, 
                        verbose = False,
                        error = True,
                        level_num = 0):
    """
    Returns the ID of the copy right, by supplying the name
    
    Test: True

    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String
        CKAN access token.
    cc_disp_name : String
        display name of the copy right, as it is found on the WindLab web page.
    verbose : Boolean, optional
        If true, further display to screen. The default is False.

    Returns
    -------
    org_id : string
        string containing the ID of the organization.

    """
    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting "get_cc_id_from_name"')
        level_num = level_num + 1
    
    cc_id = None
    
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)
    
    # Get the dataset details
    try:
        licenses = ckan.action.license_list()
    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_cc_id_from_name"')
            raise ValueError('No license_id')
        else:
            if verbose: 
                print('ERROR: get_cc_id_from_name(): No license_id')
            cc_id = None
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting: "get_cc_id_from_name"')
            return cc_id
            
    try:
        # Iterate through each dataset to get copyright information
        for license in licenses:
            title = license['title']
            id = license['id']
            # Check if the dataset has the copyright field
            if title == cc_disp_name:
                if verbose: 
                    buffer_tabs(level_num-1)
                    print('Exiting: "get_cc_id_from_name"')
                return id
            
        print('ERRORT: get_cc_id_from_name: License Disp Name did not exist. Options are:')
        for license in licenses:
            title = license['title']
            print(title)
        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "get_cc_id_from_name"')
        return None

    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_cc_id_from_name"')
            raise ValueError('No license ID')
        else:
            if verbose:
                print('ERROR: get_cc_id_from_name(): Not able to find CopyRight in WindLab')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_cc_id_from_name"')
            return cc_id


def get_dataset_all_id(ckan_url, 
                   api_token = None, 
                   verbose = False,
                   error = True,
                   level_num = 0):

    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting "get_dataset_all_id()"')
        level_num = level_num + 1

    # Initialize the CKAN API client
    ckan = RemoteCKAN(ckan_url, apikey=api_token)

    # Start by getting the first page of datasets
    start=0
    rows=100
    dataset_ids = []
    dataset_title = []
    while True:
        list_ids = ckan.action.package_search(q='title:*', start=start, rows=rows)

        # If there are no more datasets, break the loop
        if not list_ids['results']:
                break    # List to store dataset IDs
        

        # Append dataset IDs to the list
        for dataset in list_ids['results']:
            dataset_ids.append(dataset['id'])
            dataset_title.append(dataset['title'])

        # Update the start value for the next page
        start += rows

    if verbose: 
        buffer_tabs(level_num-1)
        print('Exiting "get_dataset_all_id()"')
    return dataset_ids, dataset_title
        

def get_dataset_id(ckan_url, 
                   api_token, 
                   dataset_names = '*', 
                   verbose = False,
                   error = True,
                   level_num = 0):
    """
    Gets the dataset_id from dataset_name
    
    Test: True

    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String
        CKAN access token.
    dataset_names : String, list
        Unique dataset name.
    verbose : Boolean, optional
        If true, further display to screen. The default is False.

    Returns
    -------
    dataset_id : String
        Unique dataset ID of for the unique dataset name.

    """
    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting "get_dataset_id()"')
        level_num = level_num + 1

    dataset_ids = []

    if type(dataset_names) == list:
        for dataset_name in dataset_names:
            dataset_id = get_dataset_id(ckan_url, 
                            api_token, 
                            dataset_name, 
                            verbose = verbose,
                            error = error,
                            level_num = level_num)
            dataset_ids.append(dataset_id)
        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "get_dataset_id"')
        return dataset_ids
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)

    # Get the dataset details
    try:
        dataset = ckan.action.package_show(id=dataset_names.lower())
        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "get_dataset_id"')
        return dataset['id']
    
    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_dataset_id"')
            raise ValueError('Not able to get dataset_id')
        else:
            if verbose:
                print(f'ERROR: get_dataset_id(): Not able to get dataset_id for dataset with name "{dataset_names}"')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "get_dataset_id"')
        return None
            
    if verbose: 
        buffer_tabs(level_num)
        print('Exiting "get_dataset_id"')
    return dataset_ids



    
def load_yaml(file_path):
    """
    Load YAML file and return data as Python dictionary.

    Parameters
    ----------
    file_path : String
        Path and file name of the yaml file

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    Dict
        Content of the yaml file.

    """

    with open(file_path, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            try:
                return yml_utils.load_yaml(file)
            except: 
                raise Exception(f"Error parsing YAML file: {exc}")
            
    
def validate_yaml_with_schema(yaml_file_path, 
                              resource_schema_type, 
                              resource_schema_name,
                              verbose = False,
                              error=True,
                              level_num = 0):
    """
    Validate a YAML file against a JSON schema.

    Parameters
    ----------
    yaml_file_path : String
        Dir and path name, where schema type can be found. Is a sub-folder
        in the "windlab/schemas/" folder.
    resource_schema_type : String
        Type of the schema that the data set should adher to.
    resource_schema_name : String
        Name of the resource schema to be tested against.
    verbose : Boolean, optional
        If true, further display to screen. 
        The default is False.

    Returns
    -------
    bool
        Returns True if schema test past.

    """

    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting "validate_yaml_with_schema"')
        level_num = level_num + 1

    try:
        #2 Loading the yaml data
        if os.path.exists(yaml_file_path):
            if verbose: 
                buffer_tabs(level_num)
                print('File "', yaml_file_path, '" found')
        else:
            if verbose: 
                buffer_tabs(level_num)
                print('File "', yaml_file_path, '" NOT found.')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "validate_yaml_with_schema"')
            return False
        if 1 == 1:
            try:
                validate_yaml(
                    data_file   = yaml_file_path, 
                    schema_file = 'schemas/' + resource_schema_type + '/' + resource_schema_name + '.yaml'
                )
            except:
                print('ERROR: File ', yaml_file_path, ' could not be validated.')
                sys.exit

            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "validate_yaml_with_schema"')
            return True

    
    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "validate_yaml_with_schema"')
            raise ValueError('Not able to validate data against scheema')
        else:
            if verbose: 
                print("ERROR: validate_yaml_with_schema(): Not able to validate data against scheema")
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "validate_yaml_with_schema"')
            return False
        

def check_against_schema(this_res, 
                 resource_schema_type = None,
                 verbose = False,
                 error = True,
                 level_num = 0):
    """
    Checks the resource against a schema. If failes, then key 'schema_name' of
    resourcde will be set to None.
    
    Test: True
    
    Parameters
    ----------
    this_res : Dict
        Resource in the format of a dict
    resource_schema_type : String
        Name of a schema type of the data set. Default is None
    verbose : Boolean, optional
        If true, further display to screen. 
        The default is False.

    Returns
    -------
    this_res : Dict
        Dict of the resource, where key this_res['schema_name'] has been set 
        to None, if did not carry out a schema test, or failed schema test.
    """
    
    level_num = level_num + 1
    # Check data schema against data if requested
    if verbose: 
        buffer_tabs(level_num)
        print('Entered "check_schema()"')
    level_num = level_num + 1
    try:
        if resource_schema_type is not None:
            if this_res[resource_schema_label()] == None:
                resource_schema_type = None
                this_res[resource_schema_label()] = None
                this_res[dataset_schema_label()] = None

            elif this_res[resource_schema_label()].lower() in data_format_list:
                resource_schema_name = this_res[resource_schema_label()]
                if resource_schema_name != None:
                    ret = validate_yaml_with_schema(yaml_file_path = this_res['source'], 
                                                    resource_schema_type = resource_schema_type,
                                                    resource_schema_name = resource_schema_name,
                                                    verbose = verbose,
                                                    level_num = level_num)
                else:
                    ret = False

                if ret is False:
                    resource_schema_type = None
                    this_res[resource_schema_label()] = None
                    this_res[dataset_schema_label()] = None
                else:
                    this_res[dataset_schema_label()] = resource_schema_type
            else:
                resource_schema_type = None
                this_res[resource_schema_label()] = None
                this_res[dataset_schema_label()] = None
        else:
            this_res[resource_schema_label()] = None
            this_res[dataset_schema_label()] = None

        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "check_schema()"')
        return this_res
    except:
        if error:
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "check_schema()"')
            raise ValueError('Not able to check data against scheema')
        else:
            if verbose:
                print('ERROR: check_schema(): Not able to check data against scheema')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "check_schema()"')
            return this_res

    
def write_resource(ckan_url,
                  api_token,
                  dataset_id,
                  this_res,
                  verbose = False,
                  error = True,
                  level_num = 0):
    """
    Function to dump data into the CKAN data base, and checks if compliant to 
    a schema if requested by user.

    Parameters
    ----------
    ckan_url : String
        URL of the ckan installation.
    api_token : String
        CKAN access token.
    dataset_id : String
        Unique dataset ID of for the unique dataset name.
    this_res : dict
        Is the resource supplied as a dict. Will need the following keys:
    schema_type : String, optional
        String of the schema name. Needs to be a schema located in a sub-
        folder in a folder "schemas", where "schema_type" is the sub-folder 
        name.
        If None, then dataset will not tested against schema
        The default is None.
    verbose : Boolean, optional
        If true, further display to screen. 
        The default is False.

    Returns
    -------
    boolean : 
        Boolean if process worked (True) or failed (False)

    """
    
    level_num = level_num + 1
    if verbose: 
        buffer_tabs(level_num)
        print('Starting "write_resource()"')
    level_num = level_num + 1

    # Headers for CKAN API request
    headers = {'Authorization': api_token, 'Content-Type': 'application/json'}

    # CKAN resource creation API endpoint
    resource_create_url = f'{ckan_url}/api/3/action/resource_create'  # resource_create, package_create
    resource_create_url = f'{ckan_url}/api/action/resource_create'    # resource_create, package_create

    headers = {'Authorization': api_token}

    if this_res['format'].lower() in data_format_list:
        # Resource details
        resource_data = {  # The ID of the dataset you want to add the resource to
            "package_id": dataset_id,  
            "name":         this_res['name'],
            "format":       this_res['format'],
            "description":  this_res['description'],
            "schema_type":  this_res[dataset_schema_label()],
            "schema_name":  this_res[resource_schema_label()],
        }
        # Get link to local data source
        resource_source = this_res['source']
        # Now dump the resource
        try:
            respo = requests.post(resource_create_url,
                              data=resource_data,
                              headers=headers,
                              files=[('upload', open(resource_source, 'rb'))])
            status_code = respo.status_code
            if status_code != 200:
                print('ERROR: write_resource(): ', respo.json()['error'])
                if verbose: 
                    buffer_tabs(level_num-1)
                    print('Exiting "write_resource()"')
                return False
        except Exception  as err:
            print(err)
            status_code = 'unknown'

    elif this_res['format'].lower() in ['url','link']:
        # Resource details
        resource_data = {  # The ID of the dataset you want to add the resource to
            "package_id":   dataset_id,  
            "name":         this_res['name'],
            "format":       this_res['format'],
            "description":  this_res['description'],
            "url":          this_res['url'],
            "schema_type":  this_res[dataset_schema_label()],
            "schema_name":  this_res[resource_schema_label()],
        }
        # Now dump the resource
        status_code = 'unknown'
        try:
            respo = requests.post(resource_create_url,
                              data=resource_data,
                              headers=headers)
            status_code = respo.status_code
        except:
            status_code = 'unknown'
    else:
        status_code = 'unknown'
        if verbose:
            print('ERROR: write_resource(): Code not written yet.')
            if verbose: 
                buffer_tabs(level_num-1)
                print('Exiting "write_resource()"')
        return False


    # Check the response status
    if status_code == 200:
        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "write_resource()"')
        return True
    else:
        print(f"ERROR: Failed to upload resource. Status Code: {status_code}")
        if status_code != 'unknown':
            print("Error:", respo.json())
        if verbose: 
            buffer_tabs(level_num-1)
            print('Exiting "write_resource()"')
        return False


def url_request():
    response = requests.get('http://192.102.154.16')
    if response.ok:
        return response.txt
    else:
        return  'Bad Response!'
    
    
    
    
    

if __name__ == '__main__':

    test_url = False
    if test_url:
        dir_name = './setup_files/FLOWHub/'
    else:
        dir_name = './setup_files/WindLab/'



    # get Organization ID from CKAN data base
    # get info from setup file
    if 1 == 1:
        ckan_url, api_token, organization, verbose, error = \
            read_setup(dir_file_name=dir_name + 'windlab_get_org_id.yml',
                       section_name = 'org')
        res = get_org_id_from_name(ckan_url, api_token, organization['org_disp_name'])
    
    # get copy right id
    if 1 == 0:
        ckan_url, api_token, cc, verbose, error = \
            read_setup(dir_file_name=dir_name + 'windlab_get_cc_id.yml',
                       section_name = 'license')
        res = get_cc_id_from_name(ckan_url, api_token, cc['license_disp_name'])


    print('')
    print('======================')
    print('Results:')
    print(res)
    print('======================')
    print('')
