#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd ) # script dir
SCRIPT="codeface-extraction/codeface-extraction.py" # the Python script to run: the extraction process

#CFDIR="/home/hunsen/codeface/codeface-repo" # root of Codeface repository

# construct proper PYTHONPATH for imports:
# Codeface + everything else
export PYTHONPATH=:${CFDIR}:${PYTHONPATH}

# run the extraction process
python -B ${SCRIPT} "$@"

