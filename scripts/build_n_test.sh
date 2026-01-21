#!/bin/sh
set -e

export PYTHONPATH="${PYTHONPATH}:/code/src"

flake8 --config=flake8.cfg
python -m unittest discover
