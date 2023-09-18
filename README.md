# ESource-Case-Study

## Project Overview

The application recieves 3 data sets from each of 5 different utilities operating in NY. Those data sets are:
- Network Data: Electrical circuit (feeder) infrastructure, feeder segments, substations, and available hosting capacity at each circuit segment, feeder, and substation.
- Installed DER Data: Distributed Energy Renewables (DERs) such as Rooftop Solar, Community Solar, Energy Storage, Combined Heat and Power, Anaerobic Digester, and Fuel Cells. This data set provides the generation capacity (nameplate rating), type, and the feeder it is installed on.
- Planned DER Data: Project data for DERs that are in the development and interconnection queue. These projects may or may not be completed and eventually result in Installed DER records.

There are 3 API requests that can be made:
`/refreshData`: reloads all datasets (bronze, silver, and gold) using files in `source` directory
`/getFeeders`: returns list of JSON objects of Feeder data where the max hosting capacity for the feeder is greater than `arg(maxHostingCapacity)`
`/getDERsForFeeder`: returns list of JSON objects of DER data (both planned and installed) where the feeder ID = `arg(feederID)`

Assumptions:


## Developer Notes

### How To Run Locally:
1. Install required packages:
```
pip install pyspark
pip install findspark
pip install fastapi
pip install uvicorn
```
2. Update paths throughout to point to local directories (this step should not be required to run - improvement is noted in "Code Cleanliness section")

3. Spin up API:
`uvicorn main:app --reload`
- `http://127.0.0.1:8000/docs` will show available API requests


## Improvements (TODO):
### Basic Functionality
- For each bronze layer processor, allow the dataframe to be created from all files in the respective directory, not just one csv
- Read and write from s3 bucket instead of local directory (store access keys in AWS Secrets)
- Create orchestration layer (either event-driven, refresh data upon new file load to s3 bucket, or scheduled, attempt refresh monthly)

### Edge Cases, Error Handling, Robustness
- Add logic to check for new or updated files prior to calling the loadSource functions
- Add return value to `refreshData` function to indicate success or failure
    - ideally should be able to indicate what layers succeeded and at what point failure occurred
    - Wrap all load and transform actions in try/except to handle errors

### Data Quality
- Standardize circuitID and derID values (instead of using the values given from utilities as is, wrap them in a hash or md5 with utility name)
    - Prevents duplicates across utilities
    - Standardizes format

### Code Cleanliness
- Standardize use of “feeder” vs “circuit” across all code/file names
- Update package import paths to be relative (not specific to my machine)
- Reconsider directory/file organization and naming conventions for readability and extendability
- Abstract the loadSource function (pass in schema and file locations? Maybe file configs like type, delimiter, etc). Have a single processor file with config files for each source table that needs to be loaded
- Add gitignore file to ignore pycache files


