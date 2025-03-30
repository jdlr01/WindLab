[![License](??)](??)

![WindLab](https://github.com/jdlr01/WindLab/blob/main/WindLab_logo.svg)

## Welcome to WindLab
- an API for exploring, reading and writing to the WindLab data and knowledge hub


This is a work-in-progress attempt originating from the three following European funded projects:
- AIRE: Aire project works to improve efficiency of the wind energy sector by studying wind flows at different altitudes and weather conditions, providing better design, durability and performance of the wind turbines and farms. Funded under HORIZON-CL5-2021-D3-03 programme (https://aire-project.eu/)
- Meridional: MERIDIONAL provides a comprehensively validated toolchain based on an open source Platform, which draws on an integrated knowledge and data hub to allow the efficient and accurate assessment of the performance and loads experienced by onshore, offshore, and airborne wind energy system. Funded under HORIZON-101084216 (https://meridional.eu/)
- FLOW: FLOW is developing new and innovative prediction methods for production statistics and load performance of modern offshore and onshore wind energy systems. Funded under the HORIZON- (https://flow-horizon.eu/)





## Source code repository (and issue tracker):

[https://github.com/jdlr01/WindLab](https://github.com/jdlr01/WindLab)


## Quick Start:

`pip install -r requirements.txt`


## Example:

`ckan_url = "https://windlab.hlrs.de" `

`dataset_ids, dataset_title = CK_helper.get_dataset_all_id(ckan_url)`

`for ii in range(len(dataset_title)):`

`&nbsp; print(str(ii) + ": " , dataset_title[ii] + ": " +  dataset_ids[ii]) `
	
	
## License:
[???]()

## Documentation, installation, etc:

[https://github.com/jdlr01/WindLab](https://github.com/jdlr01/WindLab).


