#!/usr/bin/bash

SCRIPT=${BASH_SOURCE[0]}
SCRIPT_DIR=$(realpath $(dirname $SCRIPT))
VENV_DIR="$SCRIPT_DIR/venv"
VENV_ACTIVATE_EXECUTE="$VENV_DIR/bin/activate"

virtualenv "$VENV_DIR"
if [ $? -ne 0 ]; then
    pip install virtualenv
    if [ $? -ne 0 ]; then
        echo "Failed to setup the project."
        exit 1
    fi
fi

printf "\n# set required environment variables for the project" >> "$VENV_ACTIVATE_EXECUTE"
printf "\nPATH=$SCRIPT_DIR:\$PATH" >> "$VENV_ACTIVATE_EXECUTE"
printf "\nexport PATH\n" >> "$VENV_ACTIVATE_EXECUTE"

source "$VENV_ACTIVATE_EXECUTE"
pip install -r requirements.txt
python -m pre_commit install

python -m leantask init
python -m leantask discover

deactivate
