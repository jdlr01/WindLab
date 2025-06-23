import CKAN_API_Calls as CK_Calls
import CKAN_API_Helper as CK_helper
from yml_utils import validate_yaml
import yaml

# Setting dir to work from
dir_name = './setup_files/WindDemo/'
dir_name_Julian = './setup_files/Flow_Imports/20_Experimental_Data/'

# Get URL for API and other access information from file
ckan_url, api_token, windlab_data, verbose, error = \
    CK_helper.read_setup(dir_file_name = 'default_token.yml')

get_org_names       = 0
get_all_ids         = 0
read_data           = 1

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

    if 1 == 1:
        print('========================')
        print('Loading Meta Data with windIO conform Data into local File')
        print('========================')

        # setting up Querry settings
        tag_strings         = ['Quick'] # place your string here, you are looking for in title
        resource_type       = 'link'    # resouces to data sets are either 'file' or 'link' to resouces
        write_to_file       = True      # Writing found data sets to file working only
        dir_name            = '..\\temp'# destination where to write the data to
        schema_compliance   = [] # You only want windIO_2 compliant data sets, otherwise set as emtpy list: 'windIO_2'

        windlab_data        = CK_helper.make_meta_request(tag_strings, resource_type, write_to_file, dir_name, schema_compliance, )

        res = CK_Calls.read_datasets(ckan_url = ckan_url,
                                api_token     = api_token,
                                windlab_data  = windlab_data,
                                verbose       = verbose,
                                error         = error)

        print('found ', len(res), ' data entries:')
        print(' ')
        print('+++++++++++++++++++++++++++++')
        for ress in res:
            print(ress[0]['name'])
            print(ress)
            print(' ')
            print('+++++++++++++++++++++++++++++')




