import CKAN_API_Calls as CK_Calls
import CKAN_API_Helper as CK_helper
from yml_utils import validate_yaml
import yaml

# Setting dir to work from
dir_name = './setup_files/WindDemo/'

# Get URL for API and other access information from file
ckan_url, api_token, windlab_data, verbose, errorVal = \
    CK_helper.read_setup(dir_file_name = 'default_token.yml')

get_org_names       = 0
get_all_ids         = 1
read_data           = 0
write_data          = 0
delete_data         = 0
delete_data_with_id = 0

## Get list of all organisations
if 1 == get_org_names:
    print('========================')
    print('Org names')
    print('========================')
    CK_helper.print_all_org_names(ckan_url, api_token)
    print(' ')



## Get List of all data sets names with IDs
if 1 == get_all_ids:
    print('========================')
    print('Data set names with IDs')
    print('========================')
    dataset_ids, dataset_title = CK_helper.get_dataset_all_id(ckan_url, api_token)
    for ii in range(len(dataset_title)):
        print(str(ii) + ': ' , dataset_title[ii] + ': ' +  dataset_ids[ii]) 
    print(' ')



## Loading Data set into memory for data residing in the WindLab
if read_data:
    if 1 == 0:
        print('========================')
        print('Loading Meta Data with Data in Link')
        print('========================')
        res   = CK_Calls.read_datasets_via_file(access_dir_file_name = 'default.yml',
                                            dir_file_name        =dir_name + 'Read_DataSets_Link.yml')
        print('found ', len(res), ' data entries')
        print(res[0])

    if 1 == 0:
        print('========================')
        print('Loading Meta Data with Data into Memory')
        print('========================')
        res   = CK_Calls.read_datasets_via_file(access_dir_file_name = 'default.yml',
                                            dir_file_name        =dir_name + 'Read_DataSets_File_2Memory.yml')
        print('found ', len(res), ' data entries')
        print(res[0])    

    if 1 == 1:
        print('========================')
        print('Loading Meta Data with Data into local File')
        print('========================')
        res   = CK_Calls.read_datasets_via_file(access_dir_file_name = 'default.yml',
                                                dir_file_name        =dir_name + 'Read_DataSets_File_2File.yml')
        print('found ', len(res), ' data entries')
        print(res[0])

    if 1 == 0:
        print('========================')
        print('Loading Meta Data with windIO conform Data into local File')
        print('========================')
        res   = CK_Calls.read_datasets_via_file(dir_name + 'Read_DataSets_File_2File_windIO.yml')
        print('found ', len(res), ' data entries:')
        print(' ')
        print('+++++++++++++++++++++++++++++')
        for ress in res:
            print(ress[0]['name'])
            print(ress)
            print(' ')
            print('+++++++++++++++++++++++++++++')

    if 1 == 0:
        #windlab_data= {'data': [None, None]}
        tag_strings = ['j. dietrich']
        windlab_data = {'data': [{'data': {'tag_strings': tag_strings, 'resource_type': 'link', 'write_to_file': True, 'dir_name': '..\\temp'}}]}
        
        ret = CK_Calls.read_datasets(ckan_url   = ckan_url,
                                    api_token   = None,
                                    windlab_data= windlab_data)
        
        print('# of entries found ', str(len(ret)))



## Writing data
if write_data:
    if 1 == 0:
        print('========================')
        print('Writing test data with Link')
        print('========================')
        # Writing a data set, that contains a link to the data
        res = CK_Calls.write_datasets_via_file(access_dir_file_name = 'default.yml',
                                            dir_file_name        = dir_name + 'Write_DataSet_Link.yml')
        print(' ')
    if 1 == 0:
        print('========================')
        print('Writing test data wtih File')
        print('========================')
        # Writing a data set, that contains a file that gets dumped in the WindLab
        res = CK_Calls.write_datasets_via_file(access_dir_file_name= 'default.yml',
                                            dir_file_name = dir_name + 'Write_DataSet_File.yml')
        print(' ')
    if 1 == 0:
        print('========================')
        print('Writing test WindIO conform data with File')
        print('========================')
        # Writing a data set, that contains a file that gets dumped in the WindLab
        res = CK_Calls.write_datasets_via_file(access_dir_file_name= 'default.yml',
                                            dir_file_name = dir_name + 'Write_DataSet_File_WindIO.yml')
        print(' ')


## Delete Data Set
if delete_data:
    if 1 == 0:
        print('========================')
        print('Delete Data set')
        print('========================')
        # There is an options to delete a data set from the WindLab
        # Using Data Set ID:
        #   Find unique ID using above ''Data set names with IDs'' and place ID in 
        #   file ''Delete_DataSets_using_ID.yml''
        #
        # Then execute the following;
        res = CK_Calls.delete_datasets_via_file(dir_name + 'Delete_DataSets_using_ID.yml')

if delete_data_with_id:
    if 1 == 1:
        print('========================')
        print('Delete Data set through ID ')
        print('========================')
        # There is an options to delete a data set from the WindLab
        # Using Data Set ID:
        #   Find unique ID using above ''Data set names with IDs'' and place ID in 
        #   file ''Delete_DataSets_using_ID.yml''
        #
        # Then execute the following;
        data_set_id = "c6a1adb1-8171-4387-bbd0-f0d660690109"
        ret = CK_Calls.delete_datasets(ckan_url     = 'https://windlab.hlrs.de', 
                                        api_token   = api_token, 
                                        windlab_data= {'data': {'data_set_id':data_set_id}}, 
                            )







