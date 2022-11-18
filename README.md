# BEIS Data Service repo for AWS

This is the data service repository for the BEIS infrastructure being deployed to AWS. DVC is initialised in this repository and points to s3://beis-dvc-bucket/dvcstorage

## DVC setup

DVC is a tool for large dataset versioning - think Git for data. The raw data is stored in S3 and Git tracks changes to this data using hashes. None of the data is stored in Git. Docs can be found [here](https://dvc.org/doc/start/data-management/data-versioning). We are using it to store all raw documents for the ORP. 
1. Once you have cloned the repository, install all required Python packages (it's good practice to do so in a virtual environment)
    - `pip install -r requirements.txt`
2. To pull the data down from S3 to use locally run
    - `dvc pull`
3. If you make any changes to the data, run the following commands to push the changes
    - `dvc add <path-to-changes>`
    - `git commit <path-to-changes>.dvc -m "Dataset updates"`
    - `dvc push`

### Requirements

- Python (>= v3.8)
- AWS access to beis-dvc-bucket