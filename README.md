# CORIO (KOR-I-O)
CORIO (Pronounced as KOR-IO) is an open source tool to exercise long run IO to check IO stability.
This is a protocol agnostic tool hence any other IO protocol can be plugged in and exercised as and when needed.
This Tool can be divided logically in following sections:

    Workload
    Core Library
    Configurations
    Scripts
    Utils
Refer [Architecture and Design documents](docs/Architecture_and_Design.md)

## Start Here

**Note:** CORIO is an under development project, use with caution and report issues.

Brush up Git knowledge if you are coming from other versioning systems,
Follow the [github process readme](https://github.com/Seagate/cortx/blob/main/doc/github-process-readme.md)
to configure git on your local machine. Create a GitHub account and get access to Seagate Repositories.
You may need a separate client with any Linux Flavour to install client side pre-requisites and start using CORIO.

### Get the Sources

Fork local repository from Seagate's [CORIO](https://github.com/Seagate/corio.git). Clone CORIO repository from your local/forked repository.

    git clone https://github.com/Seagate/corio.git
    cd corio/
    git status
    git branch
    git checkout dev
    git remote -v
    git remote add upstream https://github.com/Seagate/corio.git
    git remote -v
    Issuing the above command again will return you output as shown below.
    > origin    https://github.com/YOUR_USERNAME/corio.git (fetch)
    > origin    https://github.com/YOUR_USERNAME/corio.git (push)
    > upstream        https://github.com/Seagate/corio.git (fetch)
    > upstream        https://github.com/Seagate/corio.git (push)
    # Create new branch and push changes to it.
    git checkout -b <branch>
    git commit -s -m "message" <file>
    git fetch upstream
    git pull upstream --rebase dev # Resolve if any conflict and continue rebase.
    git push origin <branch>

### Set up dev environment

    Following steps helps to set up env, where corio runs. These steps assume that you have installed git client and cloned repo.

    1. Python 3.7 Version should be installed and configured in client system.
    2. Run following commands to update yum repo and pip.
    3. `yum update -y`
    4. `pip install --upgrade pip`
    5. Change dir to corio project directory, make sure a requirement file is present in project dir.
    6. Create Virtual environment `python3.7 -m venv virenv`
    7. Activate Virtual environment `source virenv/bin/activate`
    8. Use following command to install python packages.
            `pip install --ignore-installed -r requirements.txt`

### Prerequisite

-   A s3 account and access key and secret key should be present to carry out execution.

-   Test input may be either a single yaml file path or a directory path containing multiple tests data input yaml files, which will be executed parallelly.

    -   file path i.e. workload/s3/s3api/bucket_operations.yaml
    -   directory path i.e. workload/s3/s3api or any other customized directory(i.e. workload/s3/io)
    -   refer [yaml structure](docs/YAML_documents/yaml_structure.md) and [sample file](docs/YAML_documents/sample_file.yaml) to create workload.

### CORIO runner command, help options

  Use following command to execute workloads from different scenarios in parallel.

    python corio.py -ak <access_key> -sk <secret_key> -ep <s3.seagate.com> -ti <workload_directory_path>

corio.py --help

#### Usage

    corio.py [-h HELP][-ti TEST_INPUT] [-v, --verbose] [-us USE_SSL] [-sd SEED]
    [-sk SECRET_KEY] [-ak ACCESS_KEY] [-ep ENDPOINT] [-nn NUMBER_OF_NODES]

#### Arguments

      -h, --help    
                show this help message and exit

      -ti, --test_input
                Directory path containing test data input yaml files or input yaml file path.
    
      -v, --VERBOSE 
                Log level used verbose(debug), default is info.
    
      -us, --use_ssl
                Use HTTPS/SSL connection for S3 endpoint.
    
      -sd, --seed
                Seed used to regenerate same workload  execution.
    
      -sk, --secret_key
                s3 secret Key.
    
      -ak, --access_key
                s3 access Key.
    
      -ep, --endpoint
                fqdn/ip:port of s3 endpoint for io operations without http/https.
                protocol(http/https) in endpoint is based on use_ssl flag.
    
      -nn, --number_of_nodes (optional)
               Number of nodes in k8s system.

      -sb, --support_bundle (optional)
                Capture support bundle, specific to cortx.

      -hc, --health_check (optional)
                Check cluster health(services up and running), specific to cortx.

      -tp, --test_plan (optonal)
                jira xray test plan id.
