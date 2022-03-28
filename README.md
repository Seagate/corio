#CORIO (KOR-I-O)
CORIO (Pronounced as KOR-IO) is a open source tool to exercise long run IO to check IO stability. This is a protocol agnostic tool hence any other protocol IO can be plugged in and exercises as and when needed.
This Tool can be divided logically in following sections:

    Core Library
    Configurations
    Scripts
    Utils
Start Here:
Note: CORIO is a under development project, Use with Caution and report issues.
brush up Git knowledge if you are coming from other versioning systems
Follow the link <https://github.com/Seagate/cortx/blob/main/doc/github-process-readme.md> to configure git on your local machine
Create a GitHub account and get access to Seagate Repositories
You may need a separate client with any Linux Flavour to install client side pre-requisites and start using CORIO.

Get the Sources:

Fork local repository from Seagate's CORIO. Clone CORIO repository from your local/forked repository.

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
    git fetch upstream
    git pull upstream --rebase dev

Set up dev environment:

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

Steps to Configure test scripts for execution in parallel mode.:

    Use following command to execute workloads from different scenarios in parallel.

    python corio.py -ak <access_key> -sk <secret_key> -ep <s3.seagate.com> -ti config/io

Prerequisite: A S3 account and access key and secret key should be present to carry out execution.

here test input may be either a single yaml file path i.e. config/s3/s3api/bucket_operations.yaml

or

A Directory path containing multiple tests data input yaml files those need to be executed in parallel

i.e. config/s3/s3api or any other customized directory(i.e. config/s3/io) location may be created containing test data input yaml files and can be passed as argument

CORIO Runner Help Options:

corio.py --help

usage:

corio.py [-h HELP][-ti TEST_INPUT] [-ll LOGGING_LEVEL] [-us USE_SSL] [-sd SEED]
[-sk SECRET_KEY] [-ak ACCESS_KEY] [-ep ENDPOINT] [-nn NUMBER_OF_NODES]

arguments:

  -h HELP, --help       show this help message and exit

  -ti TEST_INPUT, --test_input TEST_INPUT
                        Directory path containing test data input yaml files
                        or input yaml file path.

  -ll LOGGING_LEVEL, --logging-level LOGGING_LEVEL
                        log level value as defined below:

                        CRITICAL=50 
                        ERROR=40
                        WARNING=30 
                        INFO=20 
                        DEBUG=10

  -us USE_SSL, --use_ssl USE_SSL
                        Use HTTPS/SSL connection for S3 endpoint.

  -sd SEED, --seed SEED
                        seed used to regenerate same workload  execution.

  -sk SECRET_KEY, --secret_key SECRET_KEY
                        s3 secret Key.

  -ak ACCESS_KEY, --access_key ACCESS_KEY
                        s3 access Key.

  -ep ENDPOINT, --endpoint ENDPOINT
                        fqdn/ip:port of s3 endpoint for io operations without
                        http/https.protocol in endpoint is based on use_ssl
                        flag.

  -nn NUMBER_OF_NODES, --number_of_nodes NUMBER_OF_NODES
                        number of nodes in system
