VENV_DIR="venv"
VENV_ACTIVATE_SCRIPT="venv/bin/activate"

virtualenv venv
if [ $? -ne 0 ]; then
    pip install virtualenv
    if [ $? -ne 0 ]; then
        echo "Failed to setup the project."
        exit 1
    fi
fi

printf "\n# set required environment variables for the project" >> "$VENV_ACTIVATE_SCRIPT"
printf "\nexport PATH=$(pwd):$PATH\n" >> "$VENV_ACTIVATE_SCRIPT"

source "$VENV_ACTIVATE_SCRIPT"
pip install -r requirements.txt

python -m leantask init
python -m leantask discover

deactivate
