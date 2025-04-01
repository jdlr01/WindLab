#!/usr/bin/env python
from ckanapi import RemoteCKAN, NotAuthorized, NotFound
import CKAN_API_Helper as CK_helper
import yaml  # needs pyyaml


data_format_list = ['txt', 'csv', 'netcdf']


def write_datasets_via_file(access_dir_file_name= 'default.yml', 
                            dir_file_name       = None):
    """
    Function to push data, specified through a yaml file, into a ckan database
    

    Parameters
    ----------
    dir_file_name : String, optional
        Dir and file name of the yaml file. The default is None.

    Returns
    -------
    boolean : 
        Boolean if process worked (True) or failed (False)

    """

    # get info from setup file
    if len(dir_file_name)>4:
        if dir_file_name[-4:] == '.yml':
            ckan_url, api_token, windlab_data, verbose, error = \
                CK_helper.read_yaml(access_dir_file_name= access_dir_file_name,
                                    dir_file_name       = dir_file_name, 
                                    process_name        = 'write_datasets')
            
        elif dir_file_name[-5:] == '.json':
            ckan_url, api_token, windlab_data, verbose, error = \
                CK_helper.read_json(access_dir_file_name= access_dir_file_name,
                                    dir_file_name       = dir_file_name, 
                                    process_name        = 'write_datasets')
            
        else:
            # Get info on WindLap
            ckan_url, api_token, windlab_data, verbose, error = \
                CK_helper.read_txt(access_dir_file_name = access_dir_file_name,
                                   dir_file_name        = dir_file_name, 
                                    process_name        = 'write_datasets')
        
        # Checking data data
        windlab_data = CK_helper.check_meta_data(windlab_data, 
                                                verbose = verbose, 
                                                error = error)
            
    else:
        print('ERROR: write_datasets_via_file(): Supplied string of file/dir name not long enough')
        return False


    ret = write_datasets(ckan_url=ckan_url,
                            api_token=api_token,
                            windlab_data=windlab_data,
                            verbose=verbose,
                            error=error)
    return ret


def write_datasets(ckan_url, 
                   api_token, 
                   windlab_data, 
                   verbose=False, 
                   error=False):
    """
    Function to push data, specified through a set of variables, into a ckan database
    
    Parameters
    ----------
    ckan_url : String
        URL of WindLab or other CKAn installation
    api_token : String
        String of the required API token
    windlab_data : Dict
        Dict of the to be written data
    verbose : Boolean, optional
        Boolean indicating to print messages to screen or not.
    error : Boolean, optional
        Boolean indicating to catch errors or not.

    Returns
    -------
    boolean : 
        Boolean if process worked (True) or failed (False)

    """
    if verbose: print('    Entered "write_datasets()"')
    # Dataset metadata to be uploaded to CKAN
    success_list = []
    for thisdata in windlab_data:

        if 'schema_compliance' in thisdata['general_opt']:
            schema_type = thisdata['general_opt']['schema_compliance']
        else:
            schema_type = None

        org_disp_name = thisdata['general_requ']['org_name']

        # get owner/organization ID
        org_id = CK_helper.get_org_id_from_name(ckan_url = ckan_url, 
                                                api_token = api_token, 
                                                org_disp_name = org_disp_name, 
                                                verbose = verbose,
                                                error   = error)
        # check resources on scheema 
        if verbose: print('         "check_schema()"')

        for this_res in thisdata['resources']:
            # Cehcking resource on schema if requested.
            if verbose: print('++++++++++++++++++')
            if verbose: print(this_res)
            this_res = CK_helper.check_schema(this_res              = this_res,
                                                resource_schema_type  = schema_type,
                                                verbose               = verbose,
                                                error                 = error)

        # Creating a package/entry to ckan 
        dataset_id = CK_helper.setup_dataset(ckan_url   = ckan_url,
                                             api_token  = api_token,
                                             org_id     = org_id,
                                             thisdata   = thisdata,
                                             verbose    = verbose,
                                             error      = error)
        if len(dataset_id) > 0:
            # dumpng the data
            print('++++++++++++++++++')
            print('++++++++++++++++++')
            for this_res in thisdata['resources']:
                # Cehcking resource on schema if requested.
                this_res = CK_helper.check_schema(this_res=this_res,
                                                  resource_schema_type=schema_type,
                                                  verbose=verbose,
                                                  error = error)
                # Dropping resource
                res_ret = CK_helper.write_resource(ckan_url,
                                                   api_token,
                                                   dataset_id,
                                                   this_res=this_res,
                                                   verbose=verbose,
                                                   error=error)
                success_list.append(res_ret)

        pass

    if False in success_list:
        return False
    else:
        return True

    

def delete_datasets_via_file(dir_file_name=None):
    """
    Deletes a data sets form the CKAN installation, using the unqiue dataset 
    names. To be deleted dataset need to be supplied through yaml file, which 
    is supplied through dir and file name.

    Parameters
    ----------
    dir_file_name : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    success_list : TYPE
        DESCRIPTION.

    """
    # get ULR for API
    ckan_url, api_token, verbose, error = \
        CK_helper.read_access(dir_file_name = 'default.yml')
    
    # get info from setup file
    with open(dir_file_name, 'r') as file:
        windlab_data = yaml.safe_load(file)


    #ckan_url, api_token, windlab_data, verbose, error = \
    #    CK_helper.read_setup(dir_file_name=dir_file_name, 
    #                         process_name='delete_dataset')
    print(windlab_data)
    ret = delete_datasets(ckan_url=ckan_url, 
                          api_token=api_token, 
                          windlab_data=windlab_data, 
                          verbose=verbose,
                          error=error)
    return ret


def delete_datasets(ckan_url, 
                    api_token, 
                    windlab_data, 
                    verbose=False, 
                    error=False):
    """
    Deletes a data sets form the CKAN installation, using the unqiue dataset 
    names. To be deleted dataset need to be supplied through a set of variables.

    Parameters
    ----------
    ckan_url : String
        URL of WindLab or other CKAn installation
    api_token : String
        String of the required API token
    windlab_data : Dict
        Dict of the to be deleted data set
    verbose : Boolean, optional
        Boolean indicating to print messages to screen or not.
    error : Boolean, optional
        Boolean indicating to catch errors or not.

    Returns
    -------
    success_list : TYPE
        DESCRIPTION.

    """

    # Itterating through the data sets to delete
    success_list = []
    thisdata     = windlab_data['data']

    # Find ID via name
    if 'data_set_names' in thisdata:
        dataset_name = thisdata['data_set_names']
        # get the dataset id
        dataset_ids = CK_helper.get_dataset_id(ckan_url, 
                                            api_token, 
                                            dataset_name, 
                                            verbose = verbose,
                                            error = error)
        if len(dataset_ids) >1:
            print('Too many data sets found with same name. Please use data set unique id.')
            dataset_ids = None
        
    # fine ID via ID
    if 'data_set_id' in thisdata:
        if type(thisdata['data_set_id']) == list:
            dataset_ids = thisdata['data_set_id']
        else:
            dataset_ids = [thisdata['data_set_id']]
        dataset_name = dataset_ids


    if dataset_ids == None:
        success_list.append({dataset_name: False})
    else:
        # Connect to the CKAN instance
        ckan = RemoteCKAN(ckan_url, apikey=api_token)
        
        try:
            # Delete the dataset
            for ii in range(len(dataset_ids)):
                dsi = dataset_ids[ii]
                ckan.action.package_delete(id=dsi)
                success_list.append({dataset_name[ii]: True})
            if verbose:
                print(f' Dataset "{dataset_name[ii]}" was successfuly deleted.')
        except NotAuthorized:
            success_list.append({dataset_name[ii]: False})
            print("Error: delete_datasets(): You are not authorized to delete this dataset.")
        except NotFound:
            success_list.append({dataset_name[ii]: False})
            print(f"Error: delete_datasets(): Dataset '{dataset_ids}' not found.")
        except Exception as e:
            success_list.append({dataset_name[ii]: False})
            print(f"ERROR: delete_datasets(): An error occurred: {e}")
        
    return success_list


def search_datasets_via_file(yml_dir_file_name=None):
    '''
    Main function to access the WindLAB ckan data base. Returns the DataCite 
    meta data, based on search criterias.

    Parameters
    ----------
    yml_dir_file_name : String, optional
        File and path name of the yaml file. Abs path name needed. 
        The default is None.

    Returns
    -------
    List of Lists
        Each outer list corresponds to a Dataset.
        Each inner elements of the list refers to a resource of the Dataset.
        Each entry is a dict, containing the following two key value pairs:
            'name': name of the Dataset
            'data': Either the data of the resource, or the file name with path
    '''
        
    # get info from setup file
    ckan_url, api_token, windlab_data, verbose, error = \
        CK_helper.read_setup(yml_dir_file_name=yml_dir_file_name, 
                             process_name='search_datasets')
    
    ret = search_datasets(ckan_url=ckan_url,
                          api_token=api_token,
                          windlab_data=windlab_data,
                          verbose=verbose,
                          error=error)
    return ret


def search_datasets(ckan_url,
                    api_token,
                    windlab_data,
                    verbose=False,
                    error=False):
    '''
    Function to access the WindLAB ckan data base. Returns the DataCite 
    meta data, based on search criterias.

    Parameters
    ----------
    ckan_url : String
        URL of WindLab or other CKAn installation
    api_token : String
        String of the required API token
    windlab_data : Dict
        Dict of the querrie
    verbose : Boolean, optional
        Boolean indicating to print messages to screen or not.
    error : Boolean, optional
        Boolean indicating to catch errors or not.

    Returns
    -------
    List of Lists
        Each outer list corresponds to a Dataset.
        Each inner elements of the list refers to a resource of the Dataset.
        Each entry is a dict, containing the following two key value pairs:
            'name': name of the Dataset
            'data': Either the data of the resource, or the file name with path
    '''
        
    # Cehck for ckan_url
    if ckan_url is None:
        print('No URL supplied. Program will terminate.')
        return []

    list_of_data_sets = []

    # Example: Connecting to CKAN instance (replace with the actual CKAN URL)
    # like a handle to the web service of ckan
    ckan = RemoteCKAN(ckan_url)
    start = 0
    rows = 10

    for data in windlab_data:
        tag_strings = data['data']['tag_strings']
        
        if type(tag_strings) is str:
            tag_strings = [tag_strings]
        elif type(tag_strings) is type(None):
            tag_strings = [tag_strings]
            
        for tag_string in tag_strings:
            # Search datasets by keyword
            if (tag_string == None) or (tag_string == ''):
                # Search datasets NOT by keywords
                while True:
                    datasets = ckan.action.package_search(include_private=True,
                                                          start=start, 
                                                          rows = rows)
                    list_of_data_sets.extend(datasets['results'])
                    if datasets["count"] <= start + rows:
                        break
                    start += rows
                    
            else:
                while True:
                    datasets = ckan.action.package_search(include_private=True, 
                                                          q=tag_string, 
                                                          start=start, 
                                                          rows = rows)
                    list_of_data_sets.extend(datasets['results'])
                    if datasets["count"] <= start + rows:
                        break
                    start += rows

    return list_of_data_sets


def read_datasets_via_file(access_dir_file_name = 'default.yml',
                           dir_file_name        = None):
    '''
    Main function to access the WindLAB ckan data based. Can return data or 
    download files from the data base.

    Parameters
    ----------
    dir_file_name : String
        File and path name of the yaml file. Abs path name needed. 
        The default is None.

    Returns
    -------
    List of Lists
        Each outer list corresponds to a Dataset.
        Each inner elements of the list refers to a resource of the Dataset.
        Each entry is a dict, containing the following two key value pairs:
            'name': name of the Dataset
            'data': Either the data of the resource, or the file name with path
    '''
        
    # get info from setup file
    ckan_url, api_token, windlab_data, verbose, error = \
        CK_helper.read_yaml(access_dir_file_name= access_dir_file_name,
                            dir_file_name       = dir_file_name, 
                            process_name        = 'read_datasets')

    
    ret = read_datasets(ckan_url=ckan_url,
                          api_token=api_token,
                          windlab_data=windlab_data,
                          verbose=verbose,
                          error=error)
    return ret


def read_datasets(ckan_url,
                  api_token,
                  windlab_data,
                  verbose=False,
                  error=False):
    '''
    Main function to access the WindLAB ckan data based. Can return data or 
    download files from the data base.

    Parameters
    ----------
    ckan_url : String
        URL of WindLab or other CKAn installation
    api_token : String
        String of the required API token
    windlab_data : Dict
        Dict of the data set to be read in
    verbose : Boolean, optional
        Boolean indicating to print messages to screen or not.
    error : Boolean, optional
        Boolean indicating to catch errors or not.

    Returns
    -------
    List of Lists
        Each outer list corresponds to a Dataset.
        Each inner elements of the list refers to a resource of the Dataset.
        Each entry is a dict, containing the following two key value pairs:
            'name': name of the Dataset
            'data': Either the data of the resource, or the file name with path
    '''
    # Cehck for ckan_url
    if ckan_url is None:
        print('No URL supplied. Program will terminate.')
        return []

    list_of_data_sets = []
    list_of_resources = []
    start = 0
    rows = 10

    # Example: Connecting to CKAN instance (replace with the actual CKAN URL)
    # like a handle to the web service of ckan
    ckan = RemoteCKAN(ckan_url)

    for data in windlab_data['data']:
        resource_type   = data['data']['resource_type']
        tag_strings     = data['data']['tag_strings']
        write_to_file   = data['data']['write_to_file']
        dir_name        = data['data']['dir_name']
        
        # Checking that tag_strings of correct type
        if type(tag_strings) is str:
            tag_strings = [tag_strings]
        elif type(tag_strings) is type(None):
            tag_strings = [tag_strings]
            
        # Going through each entry in tag_strings
        for tag_string in tag_strings:
            # Search datasets by keyword
            if (tag_string == None) or (tag_string == ''):
                # Search datasets NOT by keywords
                while True:
                    datasets = ckan.action.package_search(start=start, 
                                                          rows = rows,
                                                          include_private=True)
                    list_of_data_sets.extend(datasets['results'])
                    if datasets["count"] <= start + rows:
                        break
                    start += rows
            else:
                while True:
                    datasets = ckan.action.package_search(q=tag_string, 
                                                          start=start, 
                                                          rows = rows,
                                                          include_private=True)
                    list_of_data_sets.extend(datasets['results'])
                    if datasets["count"] <= start + rows:
                        break
                    start += rows

            # Check if querry returned entries
            if len(list_of_data_sets) == 0:
                if verbose:
                    print('No data set found with tag: ', tag_strings)
    
            # Get list of IDs  
            dataset_ids = []
            for dataset in list_of_data_sets:
                dataset_ids.append(dataset['id'])
                if verbose:
                    print(dataset['title'])
    
            # Go through each data set and see if to be selected due to other settings.
            for dataset_id in dataset_ids:
                resource_list = []
                # Fetch dataset details by its ID or name
                dataset = ckan.action.package_show(name_or_id=dataset_id)
                name = dataset['name']
                if verbose:
                    print(' ')
                    print('++++++++++++++++++++++++++++++++++++++++++++++++++')
                    print('data set name is "', name, '"')
                if resource_type == 'link':
                    for rr in dataset['resources']:
                        if (rr['url_type'] == '') or (rr['url_type'] == None):
                            resources_url = rr['url']
                            resource_list.append({'name': name, 'resource': {'url' : resources_url}})
                elif resource_type == 'file':
                    for rr in dataset['resources']:
                        if rr['url_type'] == 'upload':
                            resource_id = rr['id']
                            resource = CK_helper.read_resource(ckan,
                                                 name = name,
                                                 resource_id = resource_id,
                                                 write_to_file=write_to_file,
                                                 dir_name = dir_name)
                            resource_list.append({'name': name, 'resource': resource})
                elif resource_type == None:
                    for rr in dataset['resources']:
                        if rr['url_type'] == 'upload':
                            if verbose:
                                print('Full')
                                print(rr['name'])
                            resource_id = rr['id']
                            resource = CK_helper.read_resource(ckan,
                                                 name = name,
                                                 resource_id = resource_id,
                                                 dir_name = dir_name)
                            resource_list.append({'name': name, 'resource': resource})
                        else:
                            if verbose:
                                print('empty')
                                print(rr['name'])
                    
                else:
                    print('requeste resource type not coded: ', resource_type)
                    return []
        
                if len(resource_list) > 0:
                    list_of_resources.append(resource_list)

    return list_of_resources







if __name__ == '__main__':

    test_url = 'FlowHub'
    test_url = 'WindLab'
    test_url = 'WindDemo'

    if test_url == 'FlowHub':
        dir_name = './setup_files/FLOWHub/'
    elif test_url == 'WindLab':
        dir_name = './setup_files/WindLab/'
    elif test_url == 'WindLab':
        dir_name = './setup_files/WindDemo/'

        
    # Searching data in CKAN data base
    if 1 == 0:
        print('')
        print('')
        res = search_datasets_via_file(dir_name + 'windlab_search_datasets_1.yml')
        print('  ')
        print('  ')
        print('+++++++++++ ')
        print('found ', len(res), ' data entries')
        print('  ')
        print('First data set:')
        print('+++++++++++++++')
        print(res[6])

    # search data sets
    if 1 == 0:
        print('')
        print('')
        res = search_datasets_via_file(dir_name + 'windlab_search_datasets_2.yml')
        print('  ')
        print('  ')
        print(' +++++++++++ ')
        print('found   : ', len(res), ' data entries')
        print('  ')
        print(' +++++++++++ ')
        print(' For firste data set:')
        print('name    : ', res[0]['name'])
        print('title   : ', res[0]['title'])
        print('id      : ', res[0]['id'])
        print('notes   : ', res[0]['notes'])

    # reading data from CKAN data base
    if 1 == 0:
        print('')
        print('')
        res   = read_datasets_via_file(dir_name + 'windlab_read_datasets.yml')
        print('found ', len(res), ' data entries')
        print(res[0])

    # +++++++++++++++++++++++++++++++++++
    # Writing data into WindLab data base
    # +++++++++++++++++++++++++++++++++++
    if 1 == 0:
        print('')
        print('')
        # Publications
        if 1 == 1:
            res = write_datasets_via_file(dir_name + 'windlab_write_dataSet.yml')
            print(res)
        # Publications
        if 1 == 0:
            res = write_datasets_via_file(dir_name + 'windlab_write_publications.yml')
            print(res)
        # Model Data
        if 1 == 0:
            res = write_datasets_via_file(dir_name + 'windlab_write_model_data.yml')
            print(res)
        # Experimental Data
        if 1 == 0:
            res = write_datasets_via_file(dir_name + 'windlab_write_model_data.yml')
            print(res)
        # Jupyter Notebooks
        if 1 == 0:
            #res = write_datasets_via_file(dir_name + 'windlab_write_jupyter_notebooks.yml')
            res = write_datasets_via_file('ReadMe_Sample.md')
            print(res)
    
    # deleting data from CKAN data base
    if 1 == 0:
        print('')
        print('')
        res = delete_datasets_via_file(dir_name + 'windlab_delete_datasets.yml')
        print(res)

    # Here are furthr smaller processes.
    if 1 == 0:
        print('')
        print('')
        # get Organization ID from CKAN data base
        # get info from setup file
        ckan_url, api_token, organization, verbose = \
            CK_helper.read_setup(yml_dir_file_name=dir_name + 'windlab_get_org_id.yml')
        res = CK_helper.get_org_id_from_name(ckan_url, api_token, organization['organization_name'])
    
    # Examples
    if 1 == 0:
        dir_name = './setup_files/Examples/'
        # get Org ID
        if 1 == 0:
            print('')
            print('')
            ckan_url, api_token, organization, verbose, error = \
                CK_helper.read_setup(yml_dir_file_name=dir_name + 'FlowHub_get_org_id.yml',
                        section_name = 'org')
            res = CK_helper.get_org_id_from_name(ckan_url, api_token, organization['org_disp_name'])
            print('Org ID is ', res)
        
        # get CC id
        if 1 == 0:
            print('')
            print('')
            ckan_url, api_token, cc, verbose, error = \
                CK_helper.read_setup(yml_dir_file_name=dir_name + 'FlowHub_get_cc_id.yml',
                        section_name = 'license')
            res = CK_helper.get_cc_id_from_name(ckan_url, api_token, cc['license_disp_name'])
            print('Copy right ID is ', res)

        # Search data set
        if 1 == 0:
            print('')
            print('')
            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_search_datasets.yml')
            print('Number of data sets found are: ', len(res))
            for re in res:
                print('title           : ',re['title'])
                print('# of resources  : ',re['num_resources'])

        # Read data set with links into memory
        if 1 == 0:
            dataSet = read_datasets(yml_dir_file_name=dir_name + 'FlowHub_read_datasets_link.yml')
            print('')
            print('')
            print('Number of data sets found are: ', len(dataSet))
            print('Resources of the first one are:')
            for resource in dataSet[0]:
                print('==========')
                print('name  : ',resource['name'])
                print('url   : ',resource['resource']['url'])

        # Read data set with files into memory
        if 1 == 0:
            dataSet = read_datasets(yml_dir_file_name=dir_name + 'FlowHub_read_datasets_file.yml')
            print('')
            print('')
            print('Number of data sets found are: ', len(dataSet))
            print('Resources of the first one are:')
            for resource in dataSet[0]:
                print('==========')
                print('name  : ',resource['name'])
                print('content : ')
                print(resource['resource'])
    
        # Delete data set via writing data set
        if 1 == 0:
            print('')
            print('')
            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_delete_search.yml')
            print('Initial Search: Number of data sets found are: ', len(res))

            print('')
            res = write_datasets_via_file(dir_name + 'FlowHub_delete_write_prior.yml')
            print('Success writing test data to FlowHUB: ',res[0])

            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_delete_search.yml')
            print('Test that test data in FlowHub: ', len(res))

            res = delete_datasets_via_file(dir_name + 'FlowHub_delete_datasets.yml')

    
        # Write data set with linked data
        if 1 == 0:
            print('')
            print('')
            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_write_link_search.yml')
            print('Number of data sets in FlowHub: ', len(res))

            print('')
            res = write_datasets_via_file(dir_name + 'FlowHub_write_link.yml')
            print('Success writing test data to FlowHUB: ',res[0])

            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_write_link_search.yml')
            print('Test that test data in FlowHub: ', len(res))

        # Write data set with data in files
        if 1 == 0:
            print('')
            print('')
            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_write_files_search.yml')
            print('Number of data sets in FlowHub: ', len(res))

            print('')
            res = write_datasets_via_file(dir_name + 'FlowHub_write_files.yml')
            print('Success writing test data to FlowHUB: ',res)

            res = search_datasets_via_file(yml_dir_file_name=dir_name + 'FlowHub_write_files_search.yml')
            print('Test that test data in FlowHub: ', len(res))

    # Other help functions
    if 1 == 0:
        ckan_url, api_token, windlab_data, verbose, errorVal = \
        CK_helper.read_setup(dir_file_name = 'default.yml')

        # Getting list of all organisations
        if 1 == 1:
            CK_helper.print_all_org_names(ckan_url, api_token)

        # Get list of all unique data sets ids
        if 1 == 0:
            dataset_ids, dataset_title = CK_helper.get_dataset_all_id(ckan_url, 
                                                    api_token, 
                                                    verbose = verbose, 
                                                    error = errorVal)
            for ii in range(len(dataset_title)):
                print(str(ii) + ': ' , dataset_title[ii] + ': ' +  dataset_ids[ii])

    #print('')
    #print('======================')
    #print('Results:')
    #print(res)
    print('')
    print('')


