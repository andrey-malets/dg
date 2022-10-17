#!/usr/bin/env bash

apt-get -y install libsoap-lite-perl mono-devel python3-pip python3-termcolor
pip install -r "$(dirname "$0")/requirements.txt"
