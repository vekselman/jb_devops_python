# jb_devops_python
Final project of python module in john bryce devops course

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

## Run the main execution function
```bash
$ python aws_app/main.py
```