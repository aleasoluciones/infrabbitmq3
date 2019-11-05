#!/bin/bash

pip install pip --upgrade
python setup.py develop
#for package in $(ls -d */); do pushd $package; if [ -e setup.py ]; then python setup.py develop; fi; popd; done
pip install -r requirements.txt --upgrade
pip install -r requirements-dev.txt --upgrade