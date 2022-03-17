# This is under development project, Use with Caution

CORIO (Pronounced as KOR-IO) is a open source tool to exercise long run IO to check IO stability.
This Tool can be divided logically in following sections
    Core Library
    Configurations
    Scripts
    Utils
Start Here:
brush up Git knowledge if you are coming from other versioning systems
Follow the link <https://github.com/Seagate/cortx/blob/main/doc/github-process-readme.md> to configure git on your local machine
Create a GitHub account and get access to Seagate Repositories
You may need a separate client with any Linux Flavour to install client side pre-requisites and start using CORIO.

Get the Sources
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
    git pull upstream dev

Set up dev environment
    Following steps helps to set up env, where corio runs. These steps assume that you have installed git client and cloned repo.
    1. Python Latest Version should be installed and configured in Client System
    2. `yum update -y`
    3. `pip install --upgrade pip`
    4. Change dir to corio project directory, make sure a requirement file is present in project dir
    5. Create Virtual environment `python3.7 -m venv virenv`
    6. Activate Virtual environment `source virenv/bin/activate`
    7. Use following command to install python packages.
            `pip install --ignore-installed -r requirements.txt`

Steps to Configure test scripts for execution in parallel mode.
    Use following command to execute workloads from different scenarios in parallel.

    python corio.py -ak <access_key> -sk <secret_key> -ti config/io

    Prerequisite: A S3 account and access key and secret key should be present to carry out execution.

    here test input may be either a single yaml file path i.e. config/s3/s3api/bucket_operations.yaml
    or
    A Directory path containing multiple tests data input yaml files those need to be executed in parallel
    i.e. config/s3/s3api or any other customized directory location may be created containing test data input yaml files and can be passed as argument

CORIO Runner Help Options
    Usages corio.py

        "-sk", "--secret_key", type=str, help="s3 secret Key."
        "-ak", "--access_key", type=str, help="s3 access Key."

        "-ti", "--test_input", type=str,
                            help="Directory path containing test data input yaml files or "
                                "input yaml file path."

    Optional Arguments:

        "-us", "--use_ssl",
                            type=lambda x: bool(strtobool(str(x))), default=True,
                            help="Use HTTPS/SSL connection for S3 endpoint."
        "-sd", "--seed", type=int, help="seed.",
                            default=random.SystemRandom().randint(1, 9999999)

        "-ep", "--endpoint", type=str,
                            help="fqdn/ip:port of s3 endpoint for io operations without http/https."
                                "protocol in endpoint is based on use_ssl flag.",
                            default="s3.seagate.com"
        "-nn", "--number_of_nodes", type=int,
                            help="number of nodes in k8s system", default=1

        "-ll", "--logging-level", type=int, default=20,
                            help="log level value as defined below: " +
                                "CRITICAL=50 " +
                                "ERROR=40 " +
                                "WARNING=30 " +
                                "INFO=20 " +
                                "DEBUG=10"
