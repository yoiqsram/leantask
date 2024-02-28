virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python -m leantask init
python -m leantask discover
