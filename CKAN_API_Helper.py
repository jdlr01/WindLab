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
 
from jsonschema import ValidationError
from yml_utils import validate_yaml
#from windIO.yml_utils import validate_yaml
#from windIO.yml_utils import validate_yaml


# from yaml import load, dump
# from jsonschema import validate
# from yaml import CLoader as Loader, CDumper as Dumper
#from pathlib import Path
#from typing import Any
# from yamlinclude import YamlIncludeConstructor

data_format_list    = ['txt', 'csv', 'netcdf', 'pdf', 'yaml']
general_requ_list   = ['title', 'description', 'subject', 'copy_right_license_name', 
                     'org_name', 'group', 'private', 'identifier', 'version', 'author', 
                     'maintainer','maintainer_email']
general_opt_list    = ['funder', 'geographic_location', 'related_item', 'resource_type', 
                    'date', 'external_conditions', 'schema_compliance', 'model_type']
resource_opt_list   = ['resource_start_date', 'resource_end_date', 'resource_start_time', 
                       'resource_end_time', 'temporal_resolution', 'temporal_aggregation_method', 
                       'spatial_resolution', 'spatial_aggregation_method', 'data_instruments', 
                       'background', 'variables']
resources_list      = ['resource_title', 'resource_description', 'resource_schema_name', 'resource_format', 
                       'resource_source', 'resource_url']

general_requ_list_type  = [str, str, list, str, str, str, bool, str, float, str, str, str]
general_opt_list_type   = [str, list, str, str, str, list, str, str]
resource_opt_list_type  = [str, str, int, int, int, str, int, str, list, str, list]
resources_list_type     = [str, str, str, str, str, str]



def read_resource(ckan,
                  name,
                  resource_id,
                  write_to_file=False,
                  dir_name='', 
                  error = True,
                  verbose=False):
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
                print(f"Failed to get data from data base. Status code: {respo.status_code}")
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
                    print(f"CSV file saved as {resource_file_name}")
                data = resource_file_name
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
                data = resource_file_name

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
                    print(f"YAML file saved as {resource_file_name}")
                data = resource_file_name

            else:
                # get data as is
                data = respo.content.decode("utf-8")
                data = yaml.safe_load(data)

                # yaml.safe_load(respo.content)

                # with open(resource_file_name, 'r', encoding="utf8") as file:
                #    data = yaml.safe_load(file, Loader=loader)
                #    data = yaml.load(file, Loader=loader)

                # loader = yaml.SafeLoader  # Works with any loader
                # loader.add_constructor("!include", yaml_include_constructor)

                # data = yaml.load(respo.content, Loader=loader)

                # data_1 = yaml.load(respo.content)
                # data_1 = yaml.load(data)
                # data = yaml.safe_load(data)
                # data = yaml.safe_load(respo.content)

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
                    print(f"CSV file saved as {resource_file_name}")
                data = resource_file_name
            return data
        else:
            if verbose:
                print("The resource is not a CSV, netCDF nor YAML file.")

    except:
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

def check_meta_data(thisdata_in, 
                    verbose = False, 
                    error = True):
    
    for thisdata in thisdata_in:
        # general_requ
        for ii in range(len(general_requ_list)):
            if general_requ_list[ii] in thisdata['general_requ']:
                if isinstance(thisdata['general_requ'][general_requ_list[ii]], general_requ_list_type[ii]):
                    pass
                else:
                    print('Error: In general_requ: Type of ' + general_requ_list[ii] + ' not correct. Needs to be ' + str(general_requ_list_type[ii]))
                    sys.exit()
            else:
                print('Error: In general_requ: No key found for ' + general_requ_list[ii])
                sys.exit()
        
        # general_opt
        if 'general_opt' not in thisdata:
            thisdata['general_opt'] = {}
            for ii in range(len(general_opt_list)):
                thisdata['general_opt'][general_opt_list[ii]] = None
        elif thisdata['general_opt'] == None:
            thisdata['general_opt'] = {}
            for ii in range(len(general_opt_list)):
                thisdata['general_opt'][general_opt_list[ii]] = None
        else:
            for ii in range(len(general_opt_list)):
                if general_opt_list[ii] in thisdata['general_opt']:
                    if isinstance(thisdata['general_opt'][general_opt_list[ii]], general_opt_list_type[ii]):
                        pass
                    elif thisdata['general_opt'][general_opt_list[ii]] == None:
                        pass
                    else:
                        print('Error: In general_opt: Type of ' + general_opt_list[ii] + ' not correct. Needs to be ' + str(general_opt_list_type[ii]))
                        sys.exit()
                else:
                    thisdata['general_opt'][general_opt_list[ii]] = None

        # resource_opt
        if 'resource_opt' not in thisdata:
            thisdata['resource_opt'] = {}
            for ii in range(len(resource_opt_list)):
                thisdata['resource_opt'][resource_opt_list[ii]] = None
        elif thisdata['resource_opt'] == None:
            thisdata['resource_opt'] = {}
            for ii in range(len(resource_opt_list)):
                thisdata['resource_opt'][resource_opt_list[ii]] = None
        else:
            for ii in range(len(resource_opt_list)):
                if resource_opt_list[ii] in thisdata['resource_opt']:
                    if isinstance(thisdata['resource_opt'][resource_opt_list[ii]], resource_opt_list_type[ii]):
                        pass
                    elif thisdata['resource_opt'][resource_opt_list[ii]] == None:
                        pass
                    else:
                        print('Error: In resource_opt: Type of ' + resource_opt_list[ii] + ' not correct. Needs to be ' + str(resource_opt_list_type[ii]))
                        sys.exit()
                else:
                    thisdata['resource_opt'][resource_opt_list[ii]] = None

        # resources
        if 'resources' not in thisdata:
            thisdata['resources'] = []
        elif thisdata['resources'] == None:
            thisdata['resources'] = []
        else:
            for theEntry in thisdata['resources']:
                for ii in range(len(resources_list)):
                    if resources_list[ii] in theEntry:
                        if isinstance(theEntry[resources_list[ii]], resources_list_type[ii]):
                            pass
                        elif theEntry[resources_list[ii]] == None:
                            pass
                        else:
                            theEntry[resources_list[ii]] = None
                            #print('Error: In resource: Type of ' + resources_list[ii] + ' not correct. Needs to be ' + str(resources_list_type[ii]))
                            #sys.exit()
                    else:
                        theEntry[resources_list[ii]] = None

    if verbose: print('Setup File Test Passed.')
    return thisdata_in


def setup_dataset(ckan_url, 
                  api_token, 
                  org_id, 
                  thisdata, 
                  verbose = False, 
                  error = True):
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
    general_requ    = thisdata['general_requ']
    resources       = thisdata['resources']
    general_opt     = thisdata['general_opt']
    resource_opt    = thisdata['resource_opt']

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
                             error = error)
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

    # resource_opt
    for key in resource_opt_list:
        if key in resource_opt:
            data[key] = resource_opt[key]

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
        return []
    else:
        dataset_id = response.json()['result']['id']

    return dataset_id


def is_name_in_use(ckan_url, 
                   api_token, 
                   dataset_name,
                   verbose = False,
                   error = True):
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
    
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)

    # Get the dataset details
    try:
        ret = ckan.action.package_show(id=dataset_name)
        if ret == None:
            return False
        return True
    except:
        return False
    

def read_yaml(dir_file_name=None, 
               section_name='data', 
               process_name='',
               verbose = False,
               error = True):
    # Getting some default information for WindLab
    windlab_yaml, ckan_url, api_token, verbose, errorVal = \
    read_access(dir_file_name = 'default.yml', 
                verbose = verbose,
                error = error)
    
    with open(dir_file_name, 'r') as file:
        windlab_data = yaml.safe_load(file)

    if 'file' in windlab_data['data']:
        windlab_temp = []
        for file_name in windlab_data['data']['file']: 
            with open(file_name, 'r') as file:
                windlab_temp.append([yaml.safe_load(file)])
        windlab_data = windlab_temp[0]

    return windlab_yaml, ckan_url, api_token, windlab_data, verbose, errorVal


def read_json(dir_file_name=None, 
               section_name='data', 
               process_name='',
               verbose = False,
               error = True):
    
    # Getting some default information for WindLab
    windlab_yaml, ckan_url, api_token, verbose, errorVal = \
    read_access(dir_file_name = 'default.yml', 
                verbose = verbose,
                error = error)
    
    with open(dir_file_name, 'r') as file:
        windlab_data = json.safe_load(file)
    
    return windlab_yaml, ckan_url, api_token, windlab_data, verbose, errorVal

    
def read_txt(dir_file_name=None, 
               section_name='data', 
               process_name='',
               verbose = False,
               error = True):
    """
    Reads in the ReadMe.md file, and generates the required dics instead of a yaml file.

    Parameters
    ----------
    dir_file_name : String, optional
        File and path name of the yaml file. Abs path name needed. 
        The default is None.
    process_name : String, optional
        Name of the proces, that was trying to read the yaml file. 
        The default is ''.
    verbose : Boolean, default False
        Boolean for display of messages to screen
    error : Boolean, default False
        Boolean for displaying caught error messages

    Returns
    -------
    Dict
        Content of the yaml file.
    """
    try:
        # Getting some default information for WindLab
        windlab_yaml, ckan_url, api_token, verbose, errorVal = \
        read_access(dir_file_name = 'default.yml', 
                    verbose = verbose,
                    error = error)
        
        windlab_data = {}
        # Reading the ReadMe.md file
        f = open(dir_file_name, "r")
        text_cont = f.read()
        f.close()
        ## required meta data
        windlab_data['title']                   = get_line(text_cont, '- **title**:')
        windlab_data['description']             = get_line(text_cont, '- **description**:')
        windlab_data['subject']                 = get_line(text_cont, '- **subject**:')
        windlab_data['copy_right_license_name'] = get_line(text_cont, '- **copy right license name**:')
        windlab_data['org_name']                = get_line(text_cont, '- **org name:')
        windlab_data['group']                   = get_line(text_cont, '- **group**:')
        windlab_data['private']                 = get_line(text_cont, '- **private**:')
        windlab_data['identifier']              = get_line(text_cont, '- **identifier**:')
        windlab_data['version']                 = get_line(text_cont, '- **version**:')
        windlab_data['author']                  = get_line(text_cont, '- **author**:')
        windlab_data['maintainer']              = get_line(text_cont, '- **maintainer**:')
        windlab_data['maintainer_email']        = get_line(text_cont, '- **maintainer email**:')

        ## optional meta data
        windlab_data['funder']                  = get_line(text_cont, '- **funder**:')
        windlab_data['geographic_location']     = get_line(text_cont, '- **geographic location**:')
        windlab_data['related_item']            = get_line(text_cont, '- **related item**:')
        windlab_data['resource_type']           = get_line(text_cont, '- **resource type**:')
        windlab_data['date']                    = get_line(text_cont, '- **date**:')
        windlab_data['external_conditions']     = get_line(text_cont, '- **external conditions**:')
        windlab_data['tags']                    = get_line(text_cont, '- **tags**:')
        windlab_data['schema_compliance']       = get_line(text_cont, '- **schema compliance**:')
        windlab_data['model_type']              = get_line(text_cont, '- **model type**:')

        ## General resource set information
        windlab_data['resource_start_date']         = get_line(text_cont, '- **resource start date**:')
        windlab_data['resource_end_date']           = get_line(text_cont, '- **resource end date**:')
        windlab_data['resource_start_time']         = get_line(text_cont, '- **resource start time**:')
        windlab_data['resource_end_time']           = get_line(text_cont, '- **resource end time**:')
        windlab_data['temporal_resolution']         = get_line(text_cont, '- **temporal resolution**:')
        windlab_data['temporal_aggregation_method'] = get_line(text_cont, '- **temporal aggregation method**:')
        windlab_data['spatial_resolution']          = get_line(text_cont, '- **funspatial resolutionder**:')
        windlab_data['spatial_aggregation_method']  = get_line(text_cont, '- **spatial aggregation method**:')
        windlab_data['data_instruments']            = get_line(text_cont, '- **data instruments**:')
        windlab_data['background']                  = get_line(text_cont, '- **background**:')
        windlab_data['variables']                   = get_line(text_cont, '- **variables**:')

        windlab_data['resource_title']              = get_line(text_cont, '- **resource title**:')
        windlab_data['resource_description']        = get_line(text_cont, '- **resource description**:')
        windlab_data['resource_schema_name']        = get_line(text_cont, '- **resource schema name**:')

        return windlab_yaml, ckan_url, api_token, windlab_data, verbose, errorVal
    
    except Exception  as err:
        if error:
            raise ValueError('ERROR: read_setup: with error message:', err)
        else:
            if verbose:
                print('ERROR: read_setup: Not able to read setup file')
            return None


def get_line():

    return ''


def read_access(dir_file_name=None, 
               verbose = False,
               error = True):
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
    try:
        # Check, that yml file supplied
        if dir_file_name is None:
            print('Trying to ', process_name, ' data, but no setup file given.')
            return False
        
        # Reading yml file, and getting URL and API token 
        with open(dir_file_name, 'r') as file:
            windlab_yaml = yaml.safe_load(file)

        if ('URL' not in windlab_yaml['API']) or ('token' not in windlab_yaml['API']) or ('verbose' not in windlab_yaml['API']) or ('error' not in windlab_yaml['API']):
            print('ERROR Reading set up file. File content API not up to date.')
            return None
        
        ckan_url = windlab_yaml['API']['URL']
        api_token = windlab_yaml['API']['token']
        verbose = windlab_yaml['API']['verbose']
        errorVal = windlab_yaml['API']['error']
        
        return windlab_yaml, ckan_url, api_token, verbose, errorVal
    except Exception  as err:
        if error:
            raise ValueError('ERROR: read_access: with error message:', err)
        else:
            if verbose:
                print('ERROR: read_access: Not able to read setup file')
            return None


def read_setup(dir_file_name=None, 
               section_name='data', 
               process_name='',
               verbose = False,
               error = True):
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
    try:
        # Check, that yml file supplied
        if dir_file_name is None:
            print('Trying to ', process_name, ' data, but no setup file given.')
            return False
        
        # Reading yml file, and getting URL and API token 
        with open(dir_file_name, 'r') as file:
            windlab_yaml = yaml.safe_load(file)

        if ('URL' not in windlab_yaml['API']) or ('token' not in windlab_yaml['API']) or ('verbose' not in windlab_yaml['API']) or ('error' not in windlab_yaml['API']):
            print('ERROR Reading set up file. File content API not up to date.')
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
            
        
        return windlab_yaml, ckan_url, api_token, windlab_data, verbose, errorVal
    except Exception  as err:
        if error:
            raise ValueError('ERROR: read_setup: with error message:', err)
        else:
            if verbose:
                print('ERROR: read_setup: Not able to read setup file')
            return None


def print_all_org_names(ckan_url, 
                         api_token):
    
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
                print('ERROR: print_all_org_names(): Not able to get list of all organizations from WindLab')
            org_id = None
            return org_id
        
            
    for org in entries_list :
        print(org['display_name'].lower())

def get_org_id_from_name(ckan_url, 
                         api_token, 
                         org_disp_name, 
                         verbose = False,
                         error=True):
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
            org_id = None
            return org_id
        
            
    try:
        for org in entries_list :
            this_name = org['display_name'].lower()
            this_org_id = org['id']
            if this_name == org_disp_name.lower():
                org_id = this_org_id
                return org_id
    except:
        if error:
            raise ValueError('Not able to get ord_id')
        else:
            if verbose:
                print('ERROR: get_org_id_from_name(): Not able to get ord_id')

    Error_Mess = 'Not ord_id found for organization : '+ org_disp_name.lower()
    if verbose:
        print('+++++++')
        print('Options are:')
        for org in entries_list :
            print(org['display_name'].lower())
    raise ValueError(Error_Mess)
    return org_id
        

def get_cc_id_from_name(ckan_url, 
                        api_token, 
                        cc_disp_name, 
                        verbose = False,
                        error = True):
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
    
    cc_id = None
    
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)
    
    # Get the dataset details
    try:
        licenses = ckan.action.license_list()
    except:
        if error:
            raise ValueError('No license_id')
        else:
            if verbose: 
                print('ERROR: get_cc_id_from_name(): No license_id')
            cc_id = None
            return cc_id
            
    try:
        # Iterate through each dataset to get copyright information
        for license in licenses:
            title = license['title']
            id = license['id']
            # Check if the dataset has the copyright field
            if title == cc_disp_name:
                return id
            
        print('')
        print('ERRORT: get_cc_id_from_name: License Disp Name did not exist. Options are:')
        for license in licenses:
            title = license['title']
            print(title)
        return None

    except:
        if error:
            raise ValueError('No license ID')
        else:
            if verbose:
                print('ERROR: get_cc_id_from_name(): Not able to find CopyRight in WindLab')
            return cc_id


def get_dataset_all_id(ckan_url, 
                   api_token, 
                   verbose = False,
                   error = True):
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

    return dataset_ids, dataset_title
        
def get_dataset_id(ckan_url, 
                   api_token, 
                   dataset_names = '*', 
                   verbose = False,
                   error = True):
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
    dataset_ids = []

    if type(dataset_names) == list:
        for dataset_name in dataset_names:
            dataset_id = get_dataset_id(ckan_url, 
                            api_token, 
                            dataset_name, 
                            verbose = verbose,
                            error = error)
            dataset_ids.append(dataset_id)
        return dataset_ids
    # Connect to the CKAN instance
    ckan = RemoteCKAN(ckan_url)

    # Get the dataset details
    try:
        dataset = ckan.action.package_show(id=dataset_names.lower())
        return dataset['id']
    
    except:
        if error:
            raise ValueError('Not able to get dataset_id')
        else:
            if verbose:
                print(f'ERROR: get_dataset_id(): Not able to get dataset_id for dataset with name "{dataset_names}"')
        return None
            
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
            raise Exception(f"Error parsing YAML file: {exc}")
            
    
def validate_yaml_with_schema(yaml_file_path, 
                              resource_schema_type, 
                              resource_schema_name,
                              verbose = False,
                              error=True):
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
    try:
        #2 Loading the yaml data
        yaml_data = load_yaml(yaml_file_path)
        # Validating the yaml data
        validate_yaml(
            data_file   = yaml_data, 
            schema_file = 'schemas/' + resource_schema_type + '/' + resource_schema_name + '.yaml'
        )
        return True
    
    except:
        if error:
            raise ValueError('Not able to validate data against scheema')
        else:
            if verbose: 
                print("ERROR: validate_yaml_with_schema(): Not able to validate data against scheema")
            return False
        

def check_schema(this_res, 
                 resource_schema_type = None,
                 verbose = False,
                 error = True):
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
    
    # Check data schema against data if requested
    try:
        if resource_schema_type is not None:
            if this_res['resource_format'].lower() in data_format_list:
                resource_schema_name = this_res['resource_schema_name']
                if resource_schema_name != None:
                    ret = validate_yaml_with_schema(yaml_file_path = this_res['resource_source'], 
                                                    resource_schema_type = resource_schema_type,
                                                    resource_schema_name = resource_schema_name,
                                                    verbose = verbose)
                else:
                    ret = False

                if ret is False:
                    resource_schema_type = None
                    this_res['resource_schema_name'] = None
                    this_res['resource_schema_type'] = None
                else:
                    this_res['resource_schema_type'] = resource_schema_type
            else:
                resource_schema_type = None
                this_res['resource_schema_name'] = None
                this_res['resource_schema_type'] = None
        else:
            this_res['resource_schema_name'] = None
            this_res['resource_schema_type'] = None

        
        return this_res
    except:
        if error:
            raise ValueError('Not able to check data against scheema')
        else:
            if verbose:
                print('ERROR: check_schema(): Not able to check data against scheema')
            return this_res

    
def write_resource(ckan_url,
                  api_token,
                  dataset_id,
                  this_res,
                  verbose = False,
                  error = True):
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
    
    # Headers for CKAN API request
    headers = {'Authorization': api_token, 'Content-Type': 'application/json'}

    # CKAN resource creation API endpoint
    resource_create_url = f'{ckan_url}/api/3/action/resource_create'  # resource_create, package_create
    resource_create_url = f'{ckan_url}/api/action/resource_create'    # resource_create, package_create

    headers = {'Authorization': api_token}

    if this_res['resource_format'].lower() in data_format_list:
        # Resource details
        resource_data = {  # The ID of the dataset you want to add the resource to
            "package_id": dataset_id,  
            "name":         this_res['resource_title'],
            "format":       this_res['resource_format'],
            "description":  this_res['resource_description'],
            "schema_type":  this_res['resource_schema_type'],
            "schema_name":  this_res['resource_schema_name'],
        }
        # Get link to local data source
        resource_source = this_res['resource_source']
        # Now dump the resource
        try:
            respo = requests.post(resource_create_url,
                              data=resource_data,
                              headers=headers,
                              files=[('upload', open(resource_source, 'rb'))])
            status_code = respo.status_code
            if status_code != 200:
                print('ERROR: write_resource(): ', respo.json()['error'])
                return False
        except Exception  as err:
            print(err)
            status_code = 'unknown'

    elif this_res['resource_format'].lower() in ['url','link']:
        # Resource details
        resource_data = {  # The ID of the dataset you want to add the resource to
            "package_id":   dataset_id,  
            "name":         this_res['resource_title'],
            "format":       this_res['resource_format'],
            "description":  this_res['resource_description'],
            "url":          this_res['resource_url'],
            "schema_type":  this_res['schema_type'],
            "schema_name":  this_res['resource_schema_name'],
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
        return False


    # Check the response status
    if status_code == 200:
        return True
    else:
        print(f"Failed to upload resource. Status Code: {status_code}")
        if status_code != 'unknown':
            print("Error:", respo.json())
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
        windlab_yaml, ckan_url, api_token, organization, verbose, error = \
            read_setup(dir_file_name=dir_name + 'windlab_get_org_id.yml',
                       section_name = 'org')
        res = get_org_id_from_name(ckan_url, api_token, organization['org_disp_name'])
    
    # get copy right id
    if 1 == 0:
        windlab_yaml, ckan_url, api_token, cc, verbose, error = \
            read_setup(dir_file_name=dir_name + 'windlab_get_cc_id.yml',
                       section_name = 'license')
        res = get_cc_id_from_name(ckan_url, api_token, cc['license_disp_name'])


    print('')
    print('======================')
    print('Results:')
    print(res)
    print('======================')
    print('')
