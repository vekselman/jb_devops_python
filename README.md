# jb_devops_python
Final project of python module in john bryce devops course.

## Dependencies

Dependencies are defined in:

- `requirements.txt`

### Virtual Environments

It is best practice during development to create an
isolated [Python virtual environment](https://docs.python.org/3/library/venv.html) using the `venv`
standard library module. This will keep dependant Python packages from interfering with other
Python projects on your system.

On *Nix:

```bash
# On Python 3.9+, add --upgrade-deps
$ python3 -m venv venv
$ source venv/bin/activate
```

### AWS Credentials

In order to be able to work with aws, need to specify credentials in proper file
```bash
$ mkdir ~/.aws/
$ touch ~/.aws/credentials
```
Edit file credentials file with following properties
```editorconfig
[default]
aws_access_key_id = <ACCES_KEY>
aws_secret_access_key = <SECRET_ACCESS_KEY>
region = eu-west-1
```

## Run the main execution function
```bash
$ python aws_app/main.py
```